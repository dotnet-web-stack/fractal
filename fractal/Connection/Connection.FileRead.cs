using System.Threading.Tasks.Sources;

// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal;

/// <summary>
/// File-read IVTS facet, mirroring the recv and flush facets: the handler awaits
/// <see cref="ReadFileAsync"/>, the reactor completes it inline from its KindFileRead CQE
/// (<see cref="CompleteFileRead"/>), and bytes land in the write slab at WriteTail.
/// </summary>
public sealed unsafe partial class Connection : IValueTaskSource<int>
{
    private ManualResetValueTaskSourceCore<int> _fileSignal = new()
    {
        RunContinuationsAsynchronously = false,
    };
    private int _fileArmed;

    // Stashed request, set by ReadFileAsync and read by Reactor.SubmitFileRead. Both run on the
    // reactor thread, so it's a direct field handoff with no queue.
    internal int   FileReadFd;
    internal byte* FileReadDst;
    internal int   FileReadLen;
    internal long  FileReadOffset;

    /// <summary>
    /// Read <paramref name="len"/> bytes from <paramref name="fileFd"/> at <paramref name="fileOffset"/>
    /// into the write slab (at WriteTail) via IORING_OP_READ on this reactor's ring. Returns bytes read.
    /// </summary>
    public ValueTask<int> ReadFileAsync(int fileFd, int len, long fileOffset)
    {
        if (Volatile.Read(ref _closed) == 1)
        {
            return new ValueTask<int>(0);
        }

        if (WriteTail + len > _writeSlabSize)
        {
            throw new InvalidOperationException("Write slab too small for file read.");
        }

        if (Interlocked.Exchange(ref _fileArmed, 1) == 1)
        {
            throw new InvalidOperationException("ReadFileAsync already armed.");
        }

        _fileSignal.Reset();

        FileReadFd     = fileFd;
        FileReadDst    = WriteBuffer + WriteTail;
        FileReadLen    = len;
        FileReadOffset = fileOffset;

        int gen = Volatile.Read(ref _generation);

        _reactor.SubmitFileRead(ClientFd);

        // Race recovery (mirrors FlushAsync): if a close raced in, self-complete so we don't hang.
        if (Volatile.Read(ref _closed) == 1 && Interlocked.Exchange(ref _fileArmed, 0) == 1)
        {
            _fileSignal.SetResult(0);
        }

        return new ValueTask<int>(this, (short)gen);
    }

    // Completed by the reactor's KindFileRead branch when the read CQE lands.
    internal void CompleteFileRead(int res)
    {
        Interlocked.Exchange(ref _fileArmed, 0);
        _fileSignal.SetResult(res);
    }

    int IValueTaskSource<int>.GetResult(short token)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            return 0;
        }
        return _fileSignal.GetResult(_fileSignal.Version);
    }

    ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            return ValueTaskSourceStatus.Succeeded;
        }
        return _fileSignal.GetStatus(_fileSignal.Version);
    }

    void IValueTaskSource<int>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        if (token != (short)Volatile.Read(ref _generation))
        {
            continuation(state);
            return;
        }
        _fileSignal.OnCompleted(continuation, state, _fileSignal.Version, flags);
    }
}
