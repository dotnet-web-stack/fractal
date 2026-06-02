namespace Fractal.Http;

/// <summary>
/// Maps a file path's extension to a response Content-Type.
/// </summary>
internal static class MimeTypes
{
    public static string For(string path) => Path.GetExtension(path) switch
    {
        ".html" or ".htm" => "text/html; charset=utf-8",
        ".css"            => "text/css",
        ".js"             => "text/javascript",
        ".json"           => "application/json",
        ".svg"            => "image/svg+xml",
        ".png"            => "image/png",
        ".txt"            => "text/plain; charset=utf-8",
        _                 => "application/octet-stream",
    };
}
