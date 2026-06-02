using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using static Fractal.Native;
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal;

// MDA2AV Note:
//
// This is outside part 7 scope
//
// This is the core of the server: it owns one io_uring instance and all the connections and handlers running on it.
// Reactor will be covered in a future part series probably in a Minima context,
// but the general model is that each reactor thread runs one infinite loop polling its ring and dispatching

/// <summary>
/// One reactor is one thread, one io_uring, one SO_REUSEPORT listener and one connection map,
/// all owned by that one reactor thread. Handlers run inline on it, and every await (recv, file
/// read, flush) is completed by the reactor's own IVTS with synchronous continuations, so the
/// handler just resumes inline with no thread hopping. Doing off-reactor async work in a handler
/// is not supported and will race the SQ ring.
/// </summary>
public sealed unsafe partial class Reactor
{
    public readonly int Id;
    public Ring Ring = null!;   // created on the reactor's own thread (DEFER_TASKRUN requires same-thread setup+enter)
    public readonly Dictionary<int,Connection> Connections = new();

    private int _listenFd;
    private readonly ServerConfig _config;
    private readonly ushort _port;
    private readonly uint _ringEntries;
    private readonly bool _incremental;
    private readonly uint _recvBufferSize;

    // CQE user_data layout: kind tag in the high 32 bits, fd in the low 32.
    private const ulong KindAccept   = 1UL << 32;
    private const ulong KindRecv     = 2UL << 32;
    private const ulong KindSend     = 3UL << 32;
    private const ulong KindFileRead = 4UL << 32; // file read into the write slab (this ring)

    // Provided-buffer ring (one per reactor, shared by all its connections).
    private const ushort BgId = 1;
    private readonly uint _bufferRingEntries;   // power of two
    private byte*  _bufRing;                    // io_uring_buf_ring (kernel-shared)
    private byte*  _bufSlab;                    // contiguous slab of recv buffers
    private uint   _bufRingMask;
    private ushort _bufRingTail;

    // TODO: Any better alternative to stack here?
    //
    // Reactor-thread-only pool, so a plain Stack suffices. PoolMax caps the slab footprint:
    // PoolMax x WriteSlabSize x ReactorCount = total reserved native memory.
    private readonly int PoolMax;
    private readonly Stack<Connection> _pool;

    // Incremental-mode (IOU_PBUF_RING_INC) sizing. Per-connection rings, bounded by
    // PoolMax x ConnBufRingEntries x IncRecvBufferSize x ReactorCount. Keep entries small,
    // one buffer holds many reads so few are needed per connection.
    private readonly int  _maxConnections;       // GID cap (one bgid per active connection)
    private readonly int  _connBufRingEntries;   // buffers per connection ring
    private readonly uint _incRecvBufferSize;    // bytes per buffer (filled incrementally)

    // Transient io_uring_enter errnos (Linux): interrupted, would-block, busy.
    private const int EINTR  = 4;
    private const int EAGAIN = 11;
    private const int EBUSY  = 16;

    public Reactor(int id, ServerConfig config)
    {
        Id = id;
        _config = config;
        _port = config.Port;
        _ringEntries = config.RingEntries;
        _incremental = config.Incremental;
        _recvBufferSize = (uint)config.RecvBufferSize;
        _bufferRingEntries = (uint)config.BufferRingEntries;
        PoolMax = config.PoolMax;
        _maxConnections = config.MaxConnections;
        _connBufRingEntries = config.ConnBufRingEntries;
        _incRecvBufferSize = (uint)config.IncRecvBufferSize;
        
        _pool = new Stack<Connection>(config.PoolMax);
    }

    //TODO: Add a solid documentation for reactor buffer ring using provided ring buffer
    //
    private void InitBufferRing()
    {
        nuint ringBytes = (nuint)_bufferRingEntries * 16;
        _bufRing = (byte*)NativeMemory.AlignedAlloc(ringBytes, 4096);
        NativeMemory.Clear(_bufRing, ringBytes);

        nuint slabBytes = _bufferRingEntries * (nuint)_recvBufferSize;
        _bufSlab = (byte*)NativeMemory.AlignedAlloc(slabBytes, 64);

        _bufRingMask = _bufferRingEntries - 1;

        var reg = new io_uring_buf_reg 
        {
            ring_addr = (ulong)_bufRing,
            ring_entries = _bufferRingEntries,
            bgid = BgId,
        };

        int ret = io_uring_register(Ring.Fd, IORING_REGISTER_PBUF_RING, &reg, 1);
        if (ret < 0)
        {
            int err = Marshal.GetLastPInvokeError();

            throw new InvalidOperationException($"register pbuf_ring failed: ret={ret} errno={err}");
        }

        // Slot 0 overlaps the ring tail at offset 14, but we write only addr/len/bid
        // (offsets 0..13), so tail stays zero until we set it explicitly below.
        for (ushort bid = 0; bid < _bufferRingEntries; bid++) 
        {
            byte* slot = _bufRing + (uint)bid * 16;
            *(ulong*)(slot + 0)  = (ulong)(_bufSlab + bid * (nuint)_recvBufferSize);
            *(uint*)(slot + 8)   = _recvBufferSize;
            *(ushort*)(slot + 12) = bid;
        }
        _bufRingTail = (ushort)_bufferRingEntries;

        Volatile.Write(ref *(ushort*)(_bufRing + 14), _bufRingTail);
    }

    // Reactor-thread-only: re-add a consumed recv buffer to the shared buf_ring.
    internal void ReturnBufferDirect(ushort bid)
    {
        byte* slot = _bufRing + (_bufRingTail & _bufRingMask) * 16;
        *(ulong*)(slot + 0)  = (ulong)(_bufSlab + bid * (nuint)_recvBufferSize);
        *(uint*)(slot + 8)   = _recvBufferSize;
        *(ushort*)(slot + 12) = bid;
        _bufRingTail++;

        Volatile.Write(ref *(ushort*)(_bufRing + 14), _bufRingTail);
    }
    
    internal void Flush(int fd)
    {
        if (Connections.TryGetValue(fd, out var conn))
        {
            SubmitSend(fd, conn.WriteBuffer, (uint)conn.WriteInFlight);
        }
    }

    public void Run()
    {
        Ring = Ring.Create(_ringEntries);
        _listenFd = OpenReusePortListener(_port);

        if (_incremental)
        {
            InitIncremental();
        }
        else
        {
            InitBufferRing();
        }

        Console.WriteLine($"[r{Id}] listening on 0.0.0.0:{_port} (incremental={_incremental})");
        SubmitAcceptMultishot();

        if (_incremental)
        {
            LoopIncremental();
        }
        else
        {
            LoopShared();
        }

        close(_listenFd);
        Ring.Dispose();
    }

    private void LoopShared()
    {
        while (true)
        {
            int rc = Ring.SubmitAndWait(1);
            if (rc < 0 && rc != -EINTR && rc != -EAGAIN && rc != -EBUSY)
            {
                Console.Error.WriteLine($"[r{Id}] io_uring_enter failed: {rc}");
                break;
            }

            uint ready = Ring.CqReady();
            for (uint i = 0; i < ready; i++)
            {
                Dispatch(in Ring.CqeAt(i));
            }
            Ring.CqAdvance(ready);
        }
    }

    // Owns the handler's lifecycle: crashes are logged (not fatal), and the handler's connection
    // ref is released on return so teardown runs once the reactor's ref is released too.
    private void RunHandler(Connection conn)
    {
        // ExecuteSynchronously keeps the ref release on the reactor thread, which is the inline model.
        _config.Handler(this, conn).ContinueWith(
            static (task, state) =>
            {
                var (reactor, connection) = ((Reactor, Connection))state!;
                if (task.IsFaulted)
                {
                    Console.Error.WriteLine($"[r{reactor.Id}] handler crash on fd={connection.ClientFd}: {task.Exception}");
                }
                connection.DecRef();
            },
            (this, conn),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private void Dispatch(in IoUringCqe cqe)
    {
        ulong kind = cqe.user_data & 0xffffffff_00000000UL;
        int   fd   = (int)(cqe.user_data & 0xffffffffUL);
        bool  more = (cqe.flags & IORING_CQE_F_MORE) != 0;

        if (kind == KindAccept)
        {
            if (cqe.res >= 0)
            {
                int clientFd = cqe.res;
                SetNoDelay(clientFd);
                Connection conn = _pool.TryPop(out var pooled)
                    ? pooled.SetFd(clientFd)
                    : new Connection(this, clientFd, _config.WriteSlabSize);
                Connections[clientFd] = conn;
                conn.InitRefs();
                SubmitRecvMultishot(clientFd);

                RunHandler(conn);
            }
            else
            {
                Console.Error.WriteLine($"[r{Id}] accept error: {cqe.res}");
            }
            // Multishot accept stays armed, we only re-arm if the kernel terminated it.
            if (!more)
            {
                SubmitAcceptMultishot();
            }
        }
        else if (kind == KindRecv)
        {
            bool   hasBuf = (cqe.flags & IORING_CQE_F_BUFFER) != 0;
            ushort bid    = hasBuf ? (ushort)(cqe.flags >> IORING_CQE_BUFFER_SHIFT) : (ushort)0;

            if (cqe.res <= 0)
            {
                // Peer EOF or recv error, so the reactor owns teardown.
                if (hasBuf)
                {
                    ReturnBufferDirect(bid);
                }
                if (Connections.Remove(fd, out var dyingConn))
                {
                    dyingConn.MarkClosed();   // signal the handler to exit
                    dyingConn.DecRef();       // release the reactor's ref, teardown happens at refs==0
                }
                return;
            }

            if (!Connections.TryGetValue(fd, out var conn))
            {
                // Straggler buffer for an already-closed connection.
                if (hasBuf)
                {
                    ReturnBufferDirect(bid);
                }
                return;
            }

            byte* ptr = hasBuf ? _bufSlab + (nuint)bid * (nuint)_recvBufferSize : null;
            conn.Complete(cqe.res, bid, hasBuf, ptr);

            if (!more)
            {
                SubmitRecvMultishot(fd);
            }
        }
        else if (kind == KindSend)
        {
            if (!Connections.TryGetValue(fd, out var conn))
            {
                return;
            }
            if (cqe.res <= 0)
            {
                // Send error, so release the reactor's ref. Teardown happens when the handler exits too.
                Connections.Remove(fd);
                conn.MarkClosed();
                conn.DecRef();
                return;
            }
            conn.WriteHead += cqe.res;
            if (conn.WriteHead < conn.WriteInFlight)
            {
                // Partial send: resubmit the remainder.
                SubmitSend(fd, conn.WriteBuffer + conn.WriteHead, (uint)(conn.WriteInFlight - conn.WriteHead));
                return;
            }
            conn.CompleteFlush();   // fully sent, reset buffer state and signal the awaiter
        }
        else if (kind == KindFileRead)
        {
            // File read done, hand the byte count to the awaiting ReadFileAsync which resumes inline.
            if (Connections.TryGetValue(fd, out var conn))
            {
                conn.CompleteFileRead(cqe.res);
            }
        }
    }

    private IoUringSqe* GetSqeOrFlush()
    {
        IoUringSqe* sqe = Ring.GetSqe();
        if (sqe != null)
        {
            return sqe;
        }

        Ring.SubmitAndWait(0);
        sqe = Ring.GetSqe();

        if (sqe == null)
        {
            throw new InvalidOperationException("SQ full after flush");
        }

        return sqe;
    }

    private void SubmitAcceptMultishot()
    {
        IoUringSqe* sqe = GetSqeOrFlush();
        Unsafe.InitBlockUnaligned(sqe, 0, 64);
        sqe->opcode    = IORING_OP_ACCEPT;
        sqe->ioprio    = IORING_ACCEPT_MULTISHOT;
        sqe->fd        = _listenFd;
        sqe->user_data = KindAccept | (uint)_listenFd;
    }

    private void SubmitRecvMultishot(int fd) => SubmitRecvMultishot(fd, BgId);

    private void SubmitRecvMultishot(int fd, ushort bgid)
    {
        IoUringSqe* sqe = GetSqeOrFlush();
        Unsafe.InitBlockUnaligned(sqe, 0, 64);
        sqe->opcode    = IORING_OP_RECV;
        sqe->flags     = IOSQE_BUFFER_SELECT;
        sqe->ioprio    = IORING_RECV_MULTISHOT;
        sqe->fd        = fd;
        sqe->buf_index = bgid;          // buffer-group id (shared BgId, or per-conn in incremental)
        sqe->user_data = KindRecv | (uint)fd;
    }

    private void SubmitSend(int fd, byte* buf, uint len)
    {
        IoUringSqe* sqe = GetSqeOrFlush();
        Unsafe.InitBlockUnaligned(sqe, 0, 64);
        sqe->opcode    = IORING_OP_SEND;
        sqe->fd        = fd;
        sqe->addr      = (ulong)buf;
        sqe->len       = len;
        sqe->user_data = KindSend | (uint)fd;
    }

    // File read into the connection's write slab on this same ring (no separate file ring),
    // demuxed by KindFileRead -> completes _fileSignal -> handler resumes inline. Reactor-thread-only.
    internal void SubmitFileRead(int fd)
    {
        if (!Connections.TryGetValue(fd, out var conn))
        {
            return;
        }
        IoUringSqe* sqe = GetSqeOrFlush();
        Unsafe.InitBlockUnaligned(sqe, 0, 64);
        sqe->opcode    = IORING_OP_READ;
        sqe->fd        = conn.FileReadFd;
        sqe->addr      = (ulong)conn.FileReadDst;
        sqe->len       = (uint)conn.FileReadLen;
        sqe->off       = (ulong)conn.FileReadOffset;
        sqe->user_data = KindFileRead | (uint)fd;   // fd = client fd -> whose IVTS to complete
    }

    internal void Recycle(Connection conn, int fd)
    {
        // Wake awaiters, reclaim buffers, close the fd, reset, and pool the Connection (or free it).
        conn.MarkClosed();

        if (_incremental)
        {
            // Per-connection ring is freed wholesale, no per-buffer return.
            TeardownConnectionBufRing(conn);
        }
        else
        {
            conn.DrainRecv();   // return leftover buffers to the shared ring
        }

        // TODO: Validate HRESULT return path and error handling for this
        close(fd);

        conn.Clear();

        if (_pool.Count < PoolMax)
        {
            _pool.Push(conn);
        }
        else
        {
            conn.Dispose();
        }
    }

    // TODO: Validate HRESULT return path and error handling for this
    // Disable Nagle per accepted socket. TCP_NODELAY doesn't inherit across accept.
    private static void SetNoDelay(int fd)
    {
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(int));
    }

    // TODO: Validate HRESULT return path and error handling for this
    private static int OpenReusePortListener(ushort port)
    {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
        {
            throw new InvalidOperationException($"socket failed: {fd}");
        }

        int one = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(int));
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(int));

        sockaddr_in addr = default;
        addr.sin_family      = AF_INET;
        addr.sin_port        = Htons(port);
        addr.sin_addr.s_addr = 0; // 0.0.0.0

        if (bind(fd, &addr, (uint)sizeof(sockaddr_in)) < 0)
        {
            throw new InvalidOperationException("bind failed");
        }

        if (listen(fd, 128) < 0)
        {
            throw new InvalidOperationException("listen failed");
        }

        return fd;
    }
}
