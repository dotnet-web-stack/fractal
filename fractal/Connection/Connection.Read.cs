using System.Threading.Tasks.Sources;
using Fractal.Utils;

// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal;

/// <summary>
/// Per-connection state. Fractal is pure inline thread-per-core, the handler runs on the reactor
/// thread and every await (recv, flush, file read) is completed by this reactor's own IVTS, so the
/// handler suspends and resumes on that same thread with no cross-thread queue or wake. Recv
/// completion and handler resumption are interleaved on the one reactor thread and never run at the
/// same time. The arm flags (`_armed` plus the sticky `_pending`) order an await against a
/// completion so a recv that lands before the handler arms isn't lost.
///
/// Lifetime is pool-managed: the reactor pops a Connection on accept (or new
/// one if pool is empty), and pushes it back on teardown after `Clear()`. The
/// `_generation` field is bumped on each `Clear` so stale `ValueTask` tokens
/// from a previous connection life are detectable and return `Closed()`
/// instead of leaking the new tenant's state.
/// </summary>
public sealed unsafe partial class Connection : IValueTaskSource<RecvSnapshot>
{
    internal Connection SetFd(int fd)
    {
        ClientFd = fd;
        return this;
    }

    private ManualResetValueTaskSourceCore<RecvSnapshot> _readSignal;
    private int _armed;
    private int _pending;
    private int _closed;

    private readonly SpscRecvRing _recv = new(capacityPow2: 16);

    public ValueTask<RecvSnapshot> ReadAsync()
    {
        if (!_recv.IsEmpty() || Volatile.Read(ref _pending) == 1)
        {
            Volatile.Write(ref _pending, 0);
            return new ValueTask<RecvSnapshot>(
                new RecvSnapshot(_recv.SnapshotTail(), Volatile.Read(ref _closed) != 0));
        }

        if (Volatile.Read(ref _closed) != 0)
        {
            return new ValueTask<RecvSnapshot>(RecvSnapshot.Closed());
        }

        if (Interlocked.Exchange(ref _armed, 1) == 1)
        {
            throw new InvalidOperationException("ReadAsync already armed.");
        }

        // Snapshot the generation as the IVTS token so a later Clear() invalidates this awaiter.
        int gen = Volatile.Read(ref _generation);

        // Race recovery: re-check between arming and returning the IVTS task.
        if (!_recv.IsEmpty() || Volatile.Read(ref _pending) == 1 || Volatile.Read(ref _closed) != 0)
        {
            Volatile.Write(ref _pending, 0);
            Interlocked.Exchange(ref _armed, 0);
            
            return new ValueTask<RecvSnapshot>(
                new RecvSnapshot(_recv.SnapshotTail(), Volatile.Read(ref _closed) != 0));
        }

        return new ValueTask<RecvSnapshot>(this, (short)gen);
    }

    public bool TryGetItem(in RecvSnapshot snap, out SpscRecvRing.Item item)
        => _recv.TryDequeueUntil(snap.Tail, out item);

    public void ResetRead() => _readSignal.Reset();

    public void Complete(int res, ushort bid, bool hasBuffer, byte* ptr)
    {
        if (!_recv.TryEnqueue(new SpscRecvRing.Item
                 {
                     Ptr = ptr,
                     Bid = bid,
                     Len = res,
                     HasBuffer = hasBuffer,
                     Gen = (ushort)Volatile.Read(ref _generation)
                 }))
        {
            Console.Error.WriteLine("[conn] recv queue overflow.");
            if (hasBuffer)
            {
                _reactor.ReturnBufferDirect(bid);
            }
            Volatile.Write(ref _closed, 1);
        }

        if (Interlocked.Exchange(ref _armed, 0) == 1)
        {
            _readSignal.SetResult(new RecvSnapshot(_recv.SnapshotTail(), Volatile.Read(ref _closed) != 0));
        }
        else
        {
            Volatile.Write(ref _pending, 1);
        }
    }
    
    internal void DrainRecv()
    {
        // Return buffers still in the SPSC ring (handler exited early, or recv arrived after close).
        while (_recv.TryDequeue(out SpscRecvRing.Item item))
        {
            if (item.HasBuffer)
            {
                _reactor.ReturnBufferDirect(item.Bid);
            }
        }
    }

    // IValueTaskSource plumbing. The ValueTask token is the `_generation` snapshot at await time,
    // compared against the live `_generation` to fail stale awaiters (post Clear()/pool reuse) with a
    // sentinel. The core itself is driven by `_readSignal.Version`, which is a separate per-op version.

    RecvSnapshot IValueTaskSource<RecvSnapshot>.GetResult(short token)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            return RecvSnapshot.Closed();
        }
        
        return _readSignal.GetResult(_readSignal.Version);
    }

    ValueTaskSourceStatus IValueTaskSource<RecvSnapshot>.GetStatus(short token)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            return ValueTaskSourceStatus.Succeeded;
        }
        
        return _readSignal.GetStatus(_readSignal.Version);
    }

    void IValueTaskSource<RecvSnapshot>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            // Stale, so run the continuation now and the awaiter unblocks to Closed().
            continuation(state);
            
            return;
        }
        
        _readSignal.OnCompleted(continuation, state, _readSignal.Version, flags);
    }
}
