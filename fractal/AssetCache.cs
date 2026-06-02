using Microsoft.Win32.SafeHandles;
// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal;

/// <summary>
/// A directory of static assets opened once at construction, every file opened read-only, its
/// fd kept for the cache's lifetime, keyed by URL path (e.g. "/css/app.css"). Reads are
/// positional and fds never mutate, so an asset serves many connections/reactors at once. One fd
/// per file stays open, so mind <c>RLIMIT_NOFILE</c> for large trees.
/// </summary>
public sealed class AssetCache : IDisposable
{
    /// <summary>A pre-opened file: its raw fd and absolute path. No length is recorded, the handler
    /// derives Content-Length from the bytes it actually reads, so a changed file can't go stale.</summary>
    public readonly record struct Asset(int Fd, string Path);

    private readonly Dictionary<string, Asset> _assets;
    private readonly SafeFileHandle[] _handles;
    private int _disposed;

    /// <summary>
    /// Absolute root directory the cache was built over.
    /// </summary>
    public string RootDir { get; }

    /// <summary>
    /// Number of files opened.
    /// </summary>
    public int Count => _assets.Count;

    /// <summary>
    /// All assets keyed by URL path, handy for pre-baking per-asset headers, etc.
    /// </summary>
    public IReadOnlyDictionary<string, Asset> Assets => _assets;

    public AssetCache(string rootDir)
    {
        RootDir = Path.GetFullPath(rootDir);

        if (!Directory.Exists(RootDir))
        {
            throw new DirectoryNotFoundException(RootDir);
        }

        _assets = new Dictionary<string, Asset>(StringComparer.Ordinal);
        var handles = new List<SafeFileHandle>();

        foreach (string path in Directory.EnumerateFiles(RootDir, "*", SearchOption.AllDirectories))
        {
            SafeFileHandle handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            handles.Add(handle);

            int    fd  = (int)handle.DangerousGetHandle();   // valid while the handle is held (below)
            string key = "/" + Path.GetRelativePath(RootDir, path).Replace('\\', '/');

            _assets[key] = new Asset(fd, path);
        }

        _handles = handles.ToArray();
    }

    /// <summary>
    /// Look up a pre-opened asset by URL path. Returns false if no such file.
    /// </summary>
    public bool TryGet(string urlPath, out Asset asset) => _assets.TryGetValue(urlPath, out asset);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }
        foreach (SafeFileHandle h in _handles)
        {
            h.Dispose();
        }

        _assets.Clear();
    }
}
