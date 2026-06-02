using System.Runtime.InteropServices;
using static Fractal.Native;

// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal;

// MDA2AV Note:
//
// This is outside part 7 scope
//
//TODO: THIS IS STILL EXPERIMENTAL - USE NORMAL REACTOR FOR NOW

/// <summary>
/// Incremental-buffer (IOU_PBUF_RING_INC) path: each connection gets its own ring, one buffer
/// accumulating its byte stream across many recvs. A buffer recycles only once the kernel is done
/// appending AND the handler has returned every slice. Selected per reactor by `_incremental`.
/// </summary>
public sealed unsafe partial class Reactor
{
    private Stack<ushort>?  _freeGids;

    private void InitIncremental()
    {
        // Per-connection rings, no shared ring. GID 1 is reserved, per-conn GIDs are 2..MaxConnections+1.
        _freeGids = new Stack<ushort>(_maxConnections);
        for (int g = _maxConnections + 1; g >= 2; g--)
        {
            _freeGids.Push((ushort)g);
        }
    }

    private ushort AllocGid() => _freeGids!.Pop();
    private void FreeGid(ushort gid) => _freeGids!.Push(gid);

    private void SetupConnectionBufRing(Connection conn)
    {
        ushort gid = AllocGid();
        int entries = _connBufRingEntries;

        // Ring/slab/tracking arrays are allocated once and reused across pool lives,
        // only the kernel registration is per-life.
        if (conn.BufRing == null)
        {
            conn.BufRing = (byte*)NativeMemory.AlignedAlloc((nuint)entries * 16, 4096);
        }
        NativeMemory.Clear(conn.BufRing, (nuint)entries * 16);

        if (conn.BufSlab == null)
        {
            conn.BufSlab = (byte*)NativeMemory.AlignedAlloc((nuint)entries * (nuint)_incRecvBufferSize, 64);
        }

        conn.CumOffset  ??= new int[entries];
        conn.RefCount   ??= new int[entries];
        conn.KernelDone ??= new bool[entries];
        Array.Clear(conn.CumOffset, 0, entries);
        Array.Clear(conn.RefCount, 0, entries);
        Array.Clear(conn.KernelDone, 0, entries);

        var reg = new io_uring_buf_reg
        {
            ring_addr    = (ulong)conn.BufRing,
            ring_entries = (uint)entries,
            bgid         = gid,
            flags        = IOU_PBUF_RING_INC,
        };
        int ret = io_uring_register(Ring.Fd, IORING_REGISTER_PBUF_RING, &reg, 1);
        if (ret < 0)
        {
            throw new InvalidOperationException($"register pbuf_ring (inc) failed: ret={ret} gid={gid}");
        }

        conn.Bgid            = gid;
        conn.BufRingEntries  = entries;
        conn.BufRingMask     = (uint)(entries - 1);
        conn.IncrementalMode = true;

        for (ushort bid = 0; bid < entries; bid++)
        {
            byte* slot = conn.BufRing + (uint)bid * 16;
            *(ulong*)(slot + 0)   = (ulong)(conn.BufSlab + bid * (nuint)_incRecvBufferSize);
            *(uint*)(slot + 8)    = _incRecvBufferSize;
            *(ushort*)(slot + 12) = bid;
        }
        Volatile.Write(ref *(ushort*)(conn.BufRing + 14), (ushort)entries);
    }

    private void TeardownConnectionBufRing(Connection conn)
    {
        if (conn.IncrementalMode)
        {
            var reg = new io_uring_buf_reg { bgid = conn.Bgid };
            io_uring_register(Ring.Fd, IORING_UNREGISTER_PBUF_RING, &reg, 1);
            FreeGid(conn.Bgid);
        }
        // BufRing / BufSlab / arrays stay allocated for pool reuse.
    }

    // Re-add a fully-consumed buffer to its connection's ring (reactor-thread only).
    private void ReturnConnectionBuffer(Connection conn, ushort bid)
    {
        conn.CumOffset![bid]  = 0;
        conn.RefCount![bid]   = 0;
        conn.KernelDone![bid] = false;

        ushort tail = Volatile.Read(ref *(ushort*)(conn.BufRing + 14));
        byte* slot  = conn.BufRing + (tail & conn.BufRingMask) * 16;
        *(ulong*)(slot + 0)   = (ulong)(conn.BufSlab + bid * (nuint)_incRecvBufferSize);
        *(uint*)(slot + 8)    = _incRecvBufferSize;
        *(ushort*)(slot + 12) = bid;
        Volatile.Write(ref *(ushort*)(conn.BufRing + 14), (ushort)(tail + 1));
    }
    
    // Refcounted return (handler -> reactor), carrying (fd, gen, bid). Reactor-thread-only.
    internal void ApplyReturnIncremental(int fd, ushort gen, ushort bid)
    {
        if (!Connections.TryGetValue(fd, out var conn) || !conn.IncrementalMode)
        {
            return; // fd gone / ring already torn down
        }
        if ((ushort)conn.Generation != gen)
        {
            return; // stale return from a previous life (fd reused)
        }

        conn.RefCount![bid]--;
        if (conn.RefCount[bid] <= 0 && conn.KernelDone![bid])
        {
            ReturnConnectionBuffer(conn, bid);
        }
    }
    
    private void LoopIncremental()
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
                DispatchIncremental(in Ring.CqeAt(i));
            }
            Ring.CqAdvance(ready);
        }
    }

    private void DispatchIncremental(in IoUringCqe cqe)
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
                SetupConnectionBufRing(conn);
                SubmitRecvMultishot(clientFd, conn.Bgid);

                RunHandler(conn);
            }
            else
            {
                Console.Error.WriteLine($"[r{Id}] accept error: {cqe.res}");
            }
            if (!more)
            {
                SubmitAcceptMultishot();
            }
        }
        else if (kind == KindRecv)
        {
            bool   hasBuf  = (cqe.flags & IORING_CQE_F_BUFFER)   != 0;
            bool   bufMore = (cqe.flags & IORING_CQE_F_BUF_MORE) != 0;
            ushort bid     = hasBuf ? (ushort)(cqe.flags >> IORING_CQE_BUFFER_SHIFT) : (ushort)0;

            if (cqe.res <= 0)
            {
                // Peer EOF or recv error, the whole per-conn ring is freed in Recycle.
                if (Connections.Remove(fd, out var dyingConn))
                {
                    dyingConn.MarkClosed();
                    dyingConn.DecRef();
                }

                return;
            }

            if (!Connections.TryGetValue(fd, out var conn))
            {
                return; // straggler for a connection whose ring is already gone
            }

            // Data lands at the buffer's running offset, the kernel keeps appending to this bid
            // until it's full (F_BUF_MORE clear).
            byte* ptr = conn.BufSlab + (nuint)bid * (nuint)_incRecvBufferSize + (nuint)conn.CumOffset![bid];
            conn.CumOffset[bid] += cqe.res;
            conn.RefCount![bid]++;
            if (!bufMore || !more)
            {
                conn.KernelDone![bid] = true;
            }

            conn.Complete(cqe.res, bid, hasBuffer: true, ptr);

            if (!more)
            {
                SubmitRecvMultishot(fd, conn.Bgid);
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
                Connections.Remove(fd);
                conn.MarkClosed();
                conn.DecRef();

                return;
            }
            conn.WriteHead += cqe.res;
            if (conn.WriteHead < conn.WriteInFlight)
            {
                SubmitSend(fd, conn.WriteBuffer + conn.WriteHead, (uint)(conn.WriteInFlight - conn.WriteHead));
                
                return;
            }
            
            conn.CompleteFlush();
        }
        else if (kind == KindFileRead)
        {
            // File read done, resume the awaiting ReadFileAsync inline.
            if (Connections.TryGetValue(fd, out var conn))
            {
                conn.CompleteFileRead(cqe.res);
            }
        }
    }
}
