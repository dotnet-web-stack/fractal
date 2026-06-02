using System.Runtime.InteropServices;
using Fractal.Utils;
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal;

/// <summary>
/// Incremental-mode (IOU_PBUF_RING_INC) per-connection buffer-ring state. The reactor
/// (Reactor.Incremental) drives setup/teardown and the refcounted recycle, while this partial just
/// holds the state and routes a handler return to the reactor. All of it persists across pool reuse
/// and is freed in Dispose().
/// </summary>
public sealed unsafe partial class Connection
{
    internal byte*   BufRing;          // kernel-shared ring control area
    internal byte*   BufSlab;          // this connection's recv slab
    internal ushort  Bgid;
    internal uint    BufRingMask;
    internal int     BufRingEntries;
    internal bool    IncrementalMode;

    internal int[]?  CumOffset;        // per-bid: byte offset where the next slice begins
    internal int[]?  RefCount;         // per-bid: outstanding handler refs
    internal bool[]? KernelDone;       // per-bid: kernel finished appending (no F_BUF_MORE)

    internal int Generation => Volatile.Read(ref _generation); 

    /// <summary>
    /// Hand a consumed recv buffer back. Incremental carries (fd, gen, bid) for the refcounted
    /// recycle, while the shared path returns the bare bid to the reactor's buf_ring.
    /// </summary>
    public void ReturnBuffer(in SpscRecvRing.Item item)
    {
        if (IncrementalMode)
        {
            _reactor.ApplyReturnIncremental(ClientFd, item.Gen, item.Bid);
        }
        else
        {
            _reactor.ReturnBufferDirect(item.Bid);
        }
    }

    private void DisposeIncremental()
    {
        if (BufRing != null)
        {
            NativeMemory.AlignedFree(BufRing);
            BufRing = null;
        }

        if (BufSlab != null)
        {
            NativeMemory.AlignedFree(BufSlab);
            BufSlab = null;
        }
    }
}
