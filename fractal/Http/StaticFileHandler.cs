using System.Buffers;
using Glyph11;
using Glyph11.Parser;
using Glyph11.Parser.UltraHardened;
using Glyph11.Protocol;
using Glyph11.Validation;
// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal.Http;

public sealed partial class StaticFileHandler : IDisposable
{
    private static readonly ParserLimits Limits = ParserLimits.Default;

    // Grace before an old, swapped-out snapshot's fds are closed. io_uring keeps a file reference for
    // any read already in flight, so this window only has to outlast the gap between a handler
    // resolving the snapshot and submitting its read. Deploys are rare, so it costs nothing.
    private static readonly TimeSpan ReloadGrace = TimeSpan.FromSeconds(10);

    private readonly string _wwwRoot;
    private readonly object _reloadLock = new();
    private AssetResponses _responses;   // swapped atomically by Reload(), read with Volatile
    
    public StaticFileHandler(string wwwRoot)
    {
        _wwwRoot = wwwRoot;
        _responses = new AssetResponses(wwwRoot);
    }
    
    public int    FileCount => Volatile.Read(ref _responses).Count;

    public string RootDir   => Volatile.Read(ref _responses).RootDir;

    /// <summary>
    /// Rebuild the asset snapshot from disk and swap it in atomically, then close the old fds after a
    /// grace period. Reactors see the whole new snapshot (fds, sizes, headers) or the whole old one,
    /// never a mix, so Content-Length always matches the body shipped with it. Call after a deploy,
    /// for example from a SIGHUP handler. Thread-safe.
    /// </summary>
    public void Reload()
    {
        lock (_reloadLock)
        {
            AssetResponses fresh;
            try
            {
                fresh = new AssetResponses(_wwwRoot);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"[fractal] reload failed, keeping current assets: {e.Message}");
                return;
            }

            AssetResponses old = Interlocked.Exchange(ref _responses, fresh);
            _ = Task.Delay(ReloadGrace).ContinueWith(_ => old.Dispose(), TaskScheduler.Default);
        }
    }

    // Obsolete
    [Obsolete]
    private bool Route(ReadOnlyMemory<byte> input, BinaryRequest request,
        out AssetCache.Asset asset, out byte[] headers)
    {
        AssetResponses responses = Volatile.Read(ref _responses);
        asset   = default;
        headers = responses.NotFound;
        var bytes = new ReadOnlySequence<byte>(input);
        try
        {
            if (UltraHardenedParser.TryExtractFullHeaderValidated(ref bytes, request, in Limits, out _)
                && !RequestSemantics.HasDotSegments(request))
            {
                AcceptEncoding accept = AcceptEncodingParser.Parse(request);
                return responses.TryResolve(request.Path.Span, accept, out asset, out headers);
            }
        }
        catch (HttpParseException)
        {
            // Malformed / abusive request - 404
        }
        finally
        {
            request.Clear();
        }
        return false;
    }

    public void Dispose() => Volatile.Read(ref _responses).Dispose();
}
