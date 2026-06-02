# nginx-bench

A **tuned** nginx serving the exact same files as the fractal playground
(`../fractal.Playground/wwwroot`), so you can benchmark fractal against the
industry-standard static file server under matched conditions.

This is deliberately *not* a stock config — a default nginx would be an unfair
(too slow) comparison. Every setting below is the standard high-throughput
static-serving tuning, chosen to mirror what fractal does.

## Run

Needs nginx (with the bundled `gzip_static` module — Ubuntu's `nginx-core`/`nginx-full` have it):

```sh
sudo apt-get install -y nginx-core      # binary only; we don't use the system service

cd nginx-bench
./run.sh            # foreground on http://127.0.0.1:8081  (Ctrl-C to stop)
# or: ./run.sh test  to validate the config, ./run.sh stop to quit a running one
```

It runs unprivileged, entirely under this directory (pid/temp files here), and
touches no system nginx service.

## Benchmark

Run one server at a time (so they don't contend for cores), same wrk command, different port:

```sh
# fractal  (set ReactorCount and run the playground on :8080)
wrk -c512 -t12 -d10s http://127.0.0.1:8080/10kb.html

# nginx
wrk -c512 -t12 -d10s http://127.0.0.1:8081/10kb.html
```

`10kb.html` is 9170 bytes; wrk sends no `Accept-Encoding`, so both serve the
identity body. Loopback numbers reflect memory bandwidth, not a real NIC — treat
them as an upper bound.

## What's tuned, and why it's a fair fight

| setting | why |
|---|---|
| `worker_processes 12` + `worker_cpu_affinity auto` | one worker pinned per core — mirrors fractal's 12 thread-per-core reactors. **Set this equal to fractal's `ReactorCount`.** |
| `listen … reuseport` | SO_REUSEPORT: a per-worker accept queue, kernel-load-balanced, no accept-mutex contention — the same listener model fractal uses |
| `sendfile on` + `tcp_nopush on` | zero-copy page-cache → socket, full packets — nginx's fastest path, analogous to fractal reading from the page cache via io_uring |
| `open_file_cache` | holds fds + metadata open, so there's no `open()`/`stat()` per request — mirrors fractal's pre-opened `AssetCache` |
| `access_log off` | an access-log line is a `write()` on every request; the single biggest default-config tax |
| `keepalive_requests 10000000` | default is **1000** — nginx would otherwise close and re-handshake every connection mid-run, crushing throughput |
| `tcp_nodelay on`, `multi_accept on`, big `worker_connections`/backlog | latency + accept throughput under the connection storm |
| `gzip_static on` | serve pre-compressed `.gz` siblings like fractal serves `.br`/`.gz` (no effect on the no-Accept-Encoding wrk run) |

## Fairness notes

- Same files, same machine, same loopback, same wrk parameters, same core count.
- nginx serves from a warm page cache here; so does fractal. Neither pays cold-miss disk cost.
- For an apples-to-apples core count, keep `worker_processes` == fractal `ReactorCount`.
