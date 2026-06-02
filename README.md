# fractal

ultra high performance io_uring file reactor static file serving on one reactor ring per core

fractal is a Linux server engine built on io_uring and a pure thread-per-core model. It runs one reactor per CPU; each reactor owns its own io_uring, its own `SO_REUSEPORT` listener, and its own set of connections. The kernel spreads incoming connections across the reactors, and because no connection is ever shared between threads, the hot path runs with no locks and no cross-thread synchronization.

Requires Linux (io_uring) and .NET 10.
