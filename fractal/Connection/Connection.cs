using System.Runtime.InteropServices;
using Fractal.Utils;

namespace Fractal;

public sealed unsafe partial class Connection 
{
    private readonly Reactor _reactor;

    public int ClientFd { get; private set; }

    // Bumped on Clear(). Its low 16 bits are the IVTS token, so we can spot stale awaiters
    // left over from a previous pool life.
    private int _generation;

    // Refcount over the connection's two owners, the reactor (recv) and the handler. It starts at 2
    // on accept and teardown only runs at refs==0, so a connection is never recycled mid-request
    // (say the handler is parked on a file read when a recv error lands). Both run on the reactor
    // thread, so this just orders the two completions.
    private int _refs;

    public Connection(Reactor reactor, int fd, int writeSlabSize = 1024 * 16)
    {
        _reactor = reactor;
        ClientFd = fd;
        _writeSlabSize = writeSlabSize;
        WriteBuffer = (byte*)NativeMemory.AlignedAlloc((nuint)writeSlabSize, 64);
        
        _manager = new UnmanagedMemoryManager(WriteBuffer, writeSlabSize);
    }
    
    // Pool lifecycle (reactor-thread only): teardown = MarkClosed -> DrainRecv -> close -> Clear -> pool.

    public void MarkClosed()
    {
        Volatile.Write(ref _closed, 1);

        if (Interlocked.Exchange(ref _armed, 0) == 1)
        {
            _readSignal.SetResult(new RecvSnapshot(_recv.SnapshotTail(), isClosed: true));
        }
        else
        {
            Volatile.Write(ref _pending, 1);
        }

        if (Interlocked.Exchange(ref _flushArmed, 0) == 1)
        {
            Volatile.Write(ref _flushInProgress, 0);
            _flushSignal.SetResult(true);
        }

        if (Interlocked.Exchange(ref _fileArmed, 0) == 1)
        {
            _fileSignal.SetResult(0);
        }
    }

    // Init to 2 (reactor + handler) at accept.
    internal void InitRefs() => Volatile.Write(ref _refs, 2);

    // Release one owner's ref. Whoever drives it to 0 triggers teardown, never before both are done.
    internal void DecRef()
    {
        if (Interlocked.Decrement(ref _refs) == 0)
        {
            _reactor.Recycle(this, ClientFd);
        }
    }

    internal void Clear()
    {
        // Bump generation first, so stale IVTS tokens then resolve to Closed() or a no-op.
        Interlocked.Increment(ref _generation);

        Volatile.Write(ref _armed, 0);
        Volatile.Write(ref _pending, 0);
        Volatile.Write(ref _closed, 0);
        Volatile.Write(ref _flushArmed, 0);
        Volatile.Write(ref _flushInProgress, 0);
        Volatile.Write(ref _fileArmed, 0);

        WriteHead = 0;
        WriteTail = 0;
        WriteInFlight = 0;

        _readSignal.Reset();
        _flushSignal.Reset();
        _fileSignal.Reset();

        _recv.Reset();             // discard any leftover SPSC items
        IncrementalMode = false;   // per-conn ring (if any) was torn down before Clear
    }

    public void Dispose()
    {
        if (WriteBuffer != null)
        {
            NativeMemory.AlignedFree(WriteBuffer);
            WriteBuffer = null;
        }
        DisposeIncremental();
    }
}