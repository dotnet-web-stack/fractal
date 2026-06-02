namespace Fractal;

/// <summary>
/// Engine tunables for a fleet of <see cref="Reactor"/>s, plus the per-connection
/// <see cref="Handler"/> each reactor starts on accept. Override via object initializer.
/// </summary>
public sealed record ServerConfig
{
    public ushort Port { get; init; } = 8080;
    public int ReactorCount { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// Per-connection handler, started on accept and run inline on the reactor thread. It must not
    /// move off that thread :), so await only the connection's own IVTS ops (ReadAsync/ReadFileAsync/FlushAsync).
    /// </summary>
    public required Func<Reactor, Connection, Task> Handler { get; init; }

    public uint RingEntries  { get; init; } = 8192;

    // Shared buffer ring (Incremental == false).
    public int RecvBufferSize    { get; init; } = 32 * 1024;
    public int BufferRingEntries { get; init; } = 4096;

    // Per-connection write slab + connection pool cap.
    public int WriteSlabSize { get; init; } = 16 * 1024;
    public int PoolMax { get; init; } = 1024;

    
    // TODO: Validate incremental buffer functionality - maybe future series part?

    // Incremental mode (IOU_PBUF_RING_INC), per-connection rings. (not tested yet)

    // reserved native memory ~ PoolMax x ConnBufRingEntries x IncRecvBufferSize x ReactorCount.
    public bool Incremental { get; init; } = false;
    public int MaxConnections { get; init; } = 4096;   // GID cap (one bgid per active connection)
    public int ConnBufRingEntries { get; init; } = 16;     // buffers per connection ring
    public int IncRecvBufferSize { get; init; } = 4096;   // bytes per buffer (filled incrementally)
}
