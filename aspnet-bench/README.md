# aspnet-bench

Idiomatic ASP.NET Core static serving for the fractal/nginx comparison — the whole app is
`app.MapStaticAssets()` over `wwwroot/` (the .NET 9+ build-optimized static pipeline: content-hash
ETags, brotli/gzip negotiation, immutable caching).

## Run

`MapStaticAssets` resolves `wwwroot` relative to the content root, so publish and run from the
publish directory:

```sh
dotnet publish aspnet-bench/aspnet-bench.csproj -c Release -o ./pub
( cd ./pub && ASPNETCORE_URLS=http://127.0.0.1:8082 DOTNET_PROCESSOR_COUNT=12 dotnet aspnet-bench.dll )
```

`DOTNET_PROCESSOR_COUNT=12` matches fractal's 12 reactors / nginx's 12 workers. (For quick local
runs, `dotnet run` from the project dir also works — content root is the project, no publish needed.)

## Benchmark

```sh
wrk -c512 -t12 -d10s http://127.0.0.1:8082/10kb.html
```
