# fractal

ultra high performance io_uring file reactor — static file serving on one reactor ring per core

fractal is a Linux server engine built on io_uring and a pure thread-per-core model. It runs one reactor per CPU; each reactor owns its own io_uring, its own `SO_REUSEPORT` listener, and its own set of connections. The kernel spreads incoming connections across the reactors, and because no connection is ever shared between threads, the hot path runs with no locks and no cross-thread synchronization.

What makes it a *file* reactor is that disk reads ride the same ring as the network. Accept, receive, send, and file read are all submitted to one io_uring per core and told apart by a tag on each completion — so a connection can read a file straight into its send buffer with `ReadFileAsync`, without ever leaving the reactor. On a warm page cache the read completes almost instantly and the handler picks up inline; on a cold miss it overlaps other connections' work instead of stalling the whole reactor.

It all runs inline on the reactor thread. Each connection's receive, file read, and flush are completed by the reactor through an `IValueTaskSource`, so an `async` handler suspends and resumes on that same thread with no thread-pool hops — the speed of a hand-written event loop with the ergonomics of `async`/`await`.

At its core fractal is wire-agnostic: you plug in a per-connection handler (`Func<Reactor, Connection, Task>`), speak whatever protocol you like, and the reactor manages the connection lifecycle for you. On top of that it ships `Fractal.Http.StaticFileHandler` — point it at a directory and it parses HTTP/1.1 requests (via Glyph11, with path-traversal rejection), serves the matching file from a pre-opened `AssetCache`, and negotiates pre-compressed `.br`/`.gz` variants from `Accept-Encoding`. See `fractal.Playground` for the handful of lines that wire it up.

Requires Linux (io_uring) and .NET 10.
