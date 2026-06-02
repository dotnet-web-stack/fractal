using System.Buffers;
using System.Runtime.InteropServices;
using Fractal.Utils;
using Glyph11.Protocol;
// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal.Http;

public sealed partial class StaticFileHandler
{
    // Per-connection reassembly slab. It holds the bytes of requests not yet fully parsed, so a
    // request split across recv buffers (or a pipelined batch) carries over to the next read. A
    // request bigger than this is dropped, which lines up with Glyph11's header limits.
    private const int InflightSlabSize = 16 * 1024;

    /// <summary>
    /// Raw per-connection handler that drives the connection's ReadAsync/TryGetItem API directly with
    /// no PipeReader/PipeWriter wrapper. Each recv buffer is appended to one reusable per-connection
    /// slab and parsed there, so it reassembles a request split across buffers and drains pipelined
    /// requests with no per-request allocation. Plug into <see cref="ServerConfig.Handler"/>.
    /// </summary>
    public async Task HandleAsync(Reactor reactor, Connection conn)
    {
        using var request = new BinaryRequest();
        UnmanagedMemoryManager slab = AllocSlab(InflightSlabSize, out nint slabPtr);
        Memory<byte> mem = slab.Memory;
        int tail = 0;

        try
        {
            while (true)
            {
                RecvSnapshot snap = await conn.ReadAsync();

                while (conn.TryGetItem(snap, out SpscRecvRing.Item item))
                {
                    if (!item.HasBuffer)
                    {
                        continue;
                    }

                    // Append the recv buffer after whatever is carried over, then return it.
                    ReadOnlySpan<byte> src = item.AsSpan();
                    bool overflow = tail + src.Length > InflightSlabSize;
                    if (!overflow)
                    {
                        src.CopyTo(mem.Span[tail..]);
                        tail += src.Length;
                    }
                    conn.ReturnBuffer(in item);
                    if (overflow)
                    {
                        return;     // a pending request bigger than the slab, drop the connection
                    }

                    // Serve every complete request in the slab, then keep the unparsed tail.
                    int consumed = 0;
                    while (true)
                    {
                        ReadOnlySequence<byte> rest =
                            SkipLeadingCrlf(new ReadOnlySequence<byte>(mem.Slice(consumed, tail - consumed)));

                        ParseStatus status = TryRoute(rest, request, out int len, out bool resolved,
                            out AssetCache.Asset asset, out byte[] headers);
                        if (status == ParseStatus.NeedMore)
                        {
                            break;
                        }
                        if (status == ParseStatus.Malformed)
                        {
                            return;
                        }

                        consumed = tail - (int)rest.Length + len;

                        if (resolved)
                        {
                            await ServeAsync(conn, asset, headers);
                        }
                        else
                        {
                            conn.Write(headers);
                            await conn.FlushAsync();
                        }
                    }

                    // Shift the unparsed tail to the front of the slab for the next read.
                    if (consumed > 0)
                    {
                        mem.Span.Slice(consumed, tail - consumed).CopyTo(mem.Span);
                        tail -= consumed;
                    }
                }

                conn.ResetRead();

                if (snap.IsClosed)
                {
                    break;
                }
            }
        }
        finally
        {
            FreeSlab(slabPtr);
        }
    }

    private static unsafe UnmanagedMemoryManager AllocSlab(int size, out nint ptr)
    {
        ptr = (nint)NativeMemory.AlignedAlloc((nuint)size, 64);
        
        return new UnmanagedMemoryManager((byte*)ptr, size);
    }

    private static unsafe void FreeSlab(nint ptr) => NativeMemory.AlignedFree((void*)ptr);
}
