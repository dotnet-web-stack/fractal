using System.Buffers;
using System.Threading.Tasks.Sources;
using Fractal.Utils;

// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal;

public sealed unsafe partial class Connection : IValueTaskSource, IBufferWriter<byte>
{
    private readonly int _writeSlabSize;
    internal byte* WriteBuffer;
    internal int   WriteHead;
    internal int   WriteTail;
    internal int   WriteInFlight;
    
    private readonly UnmanagedMemoryManager _manager;

    // The write slab as a span, plus its size, for handlers that lay a response out directly
    // (read-first Content-Length, chunked framing). Reactor-thread-only like everything else here.
    internal Span<byte> WriteSlab     => new(WriteBuffer, _writeSlabSize);
    internal int        WriteSlabSize => _writeSlabSize;

    private ManualResetValueTaskSourceCore<bool> _flushSignal = new()
    {
        RunContinuationsAsynchronously = false,
    };
    private int _flushArmed;
    private int _flushInProgress;
    
#region IBufferWrite<byte>
    
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (Volatile.Read(ref _flushInProgress) != 0)
        {
            throw new InvalidOperationException("Cannot write while flush is in progress.");
        }

        int remaining = _writeSlabSize - WriteTail;
        if (sizeHint > remaining)
        {
            throw new InvalidOperationException("Buffer too small.");
        }

        return _manager.Memory.Slice(WriteTail, remaining);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        if (Volatile.Read(ref _flushInProgress) != 0)
        {
            throw new InvalidOperationException("Cannot write while flush is in progress.");
        }

        if (WriteTail + sizeHint > _writeSlabSize)
        {
            throw new InvalidOperationException("Write buffer too small.");
        }

        return new Span<byte>(WriteBuffer + WriteTail, _writeSlabSize - WriteTail);
    }

    public void Advance(int count)
    {
        if (Volatile.Read(ref _flushInProgress) != 0)
        {
            throw new InvalidOperationException("Cannot write while flush is in progress.");
        }

        WriteTail += count;
    }
    
#endregion
    
    public void Write(ReadOnlySpan<byte> source)
    {
        if (Volatile.Read(ref _flushInProgress) != 0)
        {
            throw new InvalidOperationException("Cannot write while flush is in progress.");
        }

        int len = source.Length;
        if (WriteTail + len > _writeSlabSize)
        {
            throw new InvalidOperationException("Write buffer too small.");
        }

        source.CopyTo(new Span<byte>(WriteBuffer + WriteTail, len));
        WriteTail += len;
    }

    public ValueTask FlushAsync()
    {
        // Already torn down (reactor saw EOF or an error). Return completed so the handler unwinds to
        // its next ReadAsync, sees IsClosed and exits, instead of hanging on a flush that never happens.
        if (Volatile.Read(ref _closed) == 1)
        {
            return default;
        }

        if (Interlocked.Exchange(ref _flushInProgress, 1) == 1)
        {
            throw new InvalidOperationException("FlushAsync already in progress.");
        }

        int target = WriteTail;
        if (target == 0)
        {
            Volatile.Write(ref _flushInProgress, 0);
            
            return default;
        }

        if (Interlocked.Exchange(ref _flushArmed, 1) == 1)
        {
            throw new InvalidOperationException("FlushAsync already armed.");
        }

        _flushSignal.Reset();
        WriteInFlight = target;

        int gen = Volatile.Read(ref _generation);

        _reactor.Flush(ClientFd);

        // Race recovery (mirrors ReadAsync): if close raced in, self-complete so we don't hang.
        if (Volatile.Read(ref _closed) == 1 && Interlocked.Exchange(ref _flushArmed, 0) == 1)
        {
            Volatile.Write(ref _flushInProgress, 0);
            _flushSignal.SetResult(true);
        }

        return new ValueTask(this, (short)gen);
    }

    // Completed by the reactor's send branch.
    internal void CompleteFlush()
    {
        WriteHead = 0;
        WriteTail = 0;
        WriteInFlight = 0;
        Volatile.Write(ref _flushInProgress, 0);
        Interlocked.Exchange(ref _flushArmed, 0);
        
        _flushSignal.SetResult(true);
    }
    
#region IValueTaskSource
    
    void IValueTaskSource.GetResult(short token)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            return;
        }
        
        _flushSignal.GetResult(_flushSignal.Version);
    }

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            return ValueTaskSourceStatus.Succeeded;
        }
        
        return _flushSignal.GetStatus(_flushSignal.Version);
    }

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            continuation(state);
            
            return;
        }
        _flushSignal.OnCompleted(continuation, state, _flushSignal.Version, flags);
    }
    
#endregion
}