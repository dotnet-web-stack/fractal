using System.Text;
// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal.Http;

/// <summary>
/// Servable responses: a directory's files (via <see cref="AssetCache"/>) with a pre-baked header
/// block each, plus content negotiation. Each URL is grouped with its .br/.gz variants into a
/// <see cref="Resolved"/> at startup, keyed by path <em>bytes</em>, so a request resolves with one
/// span lookup + a flag check, no per-request string and no ".br"/".gz" concatenation.
/// </summary>
internal sealed class AssetResponses : IDisposable
{
    // A resolved URL: the identity file plus any pre-compressed variants, each with its
    // pre-baked header block. Built once at startup and selected per request by Accept-Encoding.
    private readonly record struct Resolved(
        AssetCache.Asset Identity, byte[] IdentityHeaders,
        AssetCache.Asset Br, byte[] BrHeaders, bool HasBr,
        AssetCache.Asset Gz, byte[] GzHeaders, bool HasGz);

    private readonly AssetCache _cache;
    private readonly Dictionary<byte[], Resolved> _byPath;
    private readonly Dictionary<byte[], Resolved>.AlternateLookup<ReadOnlySpan<byte>> _lookup;

    /// <summary>Pre-baked 404 response (status line + headers, no body).</summary>
    public byte[] NotFound { get; } =
        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n"u8.ToArray();

    public int    Count   => _cache.Count;
    public string RootDir => _cache.RootDir;

    public AssetResponses(string wwwRoot)
    {
        _cache = new AssetCache(wwwRoot);

        // Pre-bake one header block per asset (string-keyed, startup only).
        var headers = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        foreach ((string url, AssetCache.Asset asset) in _cache.Assets)
        {
            headers[url] = BuildHeaders(url, asset);
        }

        // Group every asset URL with its br/gz negotiation variants, keyed by the path bytes
        // for zero-allocation span lookup on the hot path. ".br"/".gz" siblings are resolved
        // here once, never concatenated per request.
        _byPath = new Dictionary<byte[], Resolved>(PathBytesComparer.Instance);
        foreach ((string url, AssetCache.Asset asset) in _cache.Assets)
        {
            bool hasBr = _cache.TryGet(url + ".br", out AssetCache.Asset br);
            bool hasGz = _cache.TryGet(url + ".gz", out AssetCache.Asset gz);
            _byPath[Encoding.ASCII.GetBytes(url)] = new Resolved(
                asset, headers[url],
                br, hasBr ? headers[url + ".br"] : null!, hasBr,
                gz, hasGz ? headers[url + ".gz"] : null!, hasGz);
        }
        _lookup = _byPath.GetAlternateLookup<ReadOnlySpan<byte>>();
    }

    /// <summary>
    /// Resolve a request path (raw bytes) and its accepted encodings to an asset and its pre-baked
    /// headers. "/" -> "/index.html"; serve the best accepted pre-compressed variant (br &gt; gzip
    /// &gt; identity). Returns false (with the 404 header block) when nothing matches. Zero allocation.
    /// </summary>
    public bool TryResolve(ReadOnlySpan<byte> path, AcceptEncoding accept,
        out AssetCache.Asset asset, out byte[] headers)
    {
        if (path.IsEmpty || path.SequenceEqual("/"u8))
        {
            path = "/index.html"u8;
        }

        if (_lookup.TryGetValue(path, out Resolved r))
        {
            if ((accept & AcceptEncoding.Br) != 0 && r.HasBr)
            {
                asset = r.Br;
                headers = r.BrHeaders;
                return true;
            }
            if ((accept & AcceptEncoding.Gzip) != 0 && r.HasGz)
            {
                asset = r.Gz;
                headers = r.GzHeaders;
                return true;
            }
            asset = r.Identity;
            headers = r.IdentityHeaders;
            return true;
        }

        asset   = default;
        headers = NotFound;
        return false;
    }

    private static byte[] BuildHeaders(string url, AssetCache.Asset asset)
    {
        string? encoding = null;
        string  typePath = asset.Path;
        if (url.EndsWith(".br", StringComparison.Ordinal))
        {
            encoding = "br";
            typePath = asset.Path[..^3];
        }
        else if (url.EndsWith(".gz", StringComparison.Ordinal))
        {
            encoding = "gzip";
            typePath = asset.Path[..^3];
        }

        // Header prefix only: no Content-Length and no terminating blank line. The handler appends
        // "Content-Length: <n>" once the body is read, or "Transfer-Encoding: chunked" if it overflows.
        string head =
            $"HTTP/1.1 200 OK\r\nContent-Type: {MimeTypes.For(typePath)}\r\n" +
            $"Vary: Accept-Encoding\r\nConnection: keep-alive\r\n";
        if (encoding is not null)
        {
            head += $"Content-Encoding: {encoding}\r\n";
        }
        return Encoding.ASCII.GetBytes(head);
    }

    public void Dispose() => _cache.Dispose();

    // byte[]-keyed equality that also matches a ReadOnlySpan<byte>, so a request path resolves
    // without allocating a string. The span and array hashes must agree, both route through Hash.
    private sealed class PathBytesComparer
        : IEqualityComparer<byte[]>, IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>
    {
        public static readonly PathBytesComparer Instance = new();
        private PathBytesComparer() { }

        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);
        public int  GetHashCode(byte[] obj)      => Hash(obj);

        public bool   Equals(ReadOnlySpan<byte> alternate, byte[] other) => alternate.SequenceEqual(other);
        public int    GetHashCode(ReadOnlySpan<byte> alternate)          => Hash(alternate);
        public byte[] Create(ReadOnlySpan<byte> alternate)               => alternate.ToArray();

        private static int Hash(ReadOnlySpan<byte> s)
        {
            var h = new HashCode();
            h.AddBytes(s);
            return h.ToHashCode();
        }
    }
}
