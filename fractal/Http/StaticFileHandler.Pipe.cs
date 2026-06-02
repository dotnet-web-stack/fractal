using System.Buffers;
using System.IO.Pipelines;
using Glyph11;
using Glyph11.Parser.UltraHardened;
using Glyph11.Protocol;
using Glyph11.Validation;
// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal.Http;

public sealed partial class StaticFileHandler
{
    private enum ParseStatus
    {
        Complete,   // full request parsed, drop the consumed bytes then serve it
        NeedMore,   // header incomplete, keep the bytes and read again
        Malformed,  // protocol or semantic violation, reject and close
    }

    /// <summary>
    /// PipeReader/PipeWriter handler. Reassembles a request split across recv buffers (incomplete ->
    /// keep the bytes and read more, via the reader's examined/backpressure path) and drains
    /// pipelined requests from a single read. Plug into <see cref="ServerConfig.Handler"/>.
    /// </summary>
    public async Task HandlePipeAsync(Reactor reactor, Connection conn)
    {
        var reader = new ConnectionPipeReader(conn);
        var writer = new ConnectionPipeWriter(conn);
        using var request = new BinaryRequest();

        try
        {
            while (true)
            {
                ReadResult read = await reader.ReadAsync();
                var buffer = read.Buffer;
                SequencePosition examined = buffer.End;     // we look at everything we were handed
                SequencePosition consumed = buffer.Start;
                bool close = false;

                // Drain every complete request in the buffer (pipelining), stop at the first
                // incomplete one, leaving its bytes to be reassembled on the next read.
                while (true)
                {
                    buffer = SkipLeadingCrlf(buffer);
                    consumed = buffer.Start;

                    ParseStatus status = TryRoute(buffer, request, out int len, out bool resolved,
                        out AssetCache.Asset asset, out byte[] headers);
                    if (status == ParseStatus.NeedMore)
                    {
                        break;
                    }
                    if (status == ParseStatus.Malformed)
                    {
                        close = true;
                        break;
                    }

                    buffer = buffer.Slice(len);             // consume past this request
                    consumed = buffer.Start;

                    if (resolved)
                    {
                        await ServeAsync(conn, asset, headers);   // headers = the baked prefix
                    }
                    else
                    {
                        conn.Write(headers);                      // the 404 block
                        await conn.FlushAsync();
                    }
                }

                // consumed = end of the last complete request, examined = end of buffer, so the
                // reader keeps the incomplete tail and only wakes us once more bytes arrive.
                reader.AdvanceTo(consumed, examined);

                if (close || read.IsCompleted)
                {
                    break;
                }
            }
        }
        finally
        {
            reader.Complete();
            writer.Complete();
        }
    }

    // Skip a stray leading CR/LF before a request line. RFC 9112 section 2.2 robustness, and it also
    // realigns pipelined requests against Glyph 0.3.3's bytesReadCount, which under-reports the
    // consumed length by one (leaving a '\n' that the next parse would otherwise choke on).
    private static ReadOnlySequence<byte> SkipLeadingCrlf(ReadOnlySequence<byte> buffer)
    {
        SequenceReader<byte> reader = new(buffer);
        while (reader.TryPeek(out byte b) && b is (byte)'\r' or (byte)'\n')
        {
            reader.Advance(1);
        }
        return buffer.Slice(reader.Position);
    }

    
    // New API to reload atomic moved files without restarting the server.
    // Rebuilds the in-memory responses and swaps them in
    //
    // Parse one request from the front of `input` and resolve it on the path bytes (no string):
    // Complete (consumed length + resolved asset/headers), NeedMore (header incomplete, read
    // more), or Malformed (reject). `resolved` is false for a rejected path-traversal or a missing
    // file, which is still a complete, consumable request -> served as 404 (headers = NotFound).
    private ParseStatus TryRoute(ReadOnlySequence<byte> input, BinaryRequest request,
        out int consumed, out bool resolved, out AssetCache.Asset asset, out byte[] headers)
    {
        AssetResponses responses = Volatile.Read(ref _responses);
        consumed = 0;
        resolved = false;
        asset    = default;
        headers  = responses.NotFound;
        try
        {
            if (!UltraHardenedParser.TryExtractFullHeaderValidated(ref input, request, in Limits, out consumed))
            {
                return ParseStatus.NeedMore;
            }
            if (!RequestSemantics.HasDotSegments(request))
            {
                AcceptEncoding accept = AcceptEncodingParser.Parse(request);
                resolved = responses.TryResolve(request.Path.Span, accept, out asset, out headers);
            }
            return ParseStatus.Complete;
        }
        catch (HttpParseException)
        {
            return ParseStatus.Malformed;
        }
        finally
        {
            request.Clear();
        }
    }
}
