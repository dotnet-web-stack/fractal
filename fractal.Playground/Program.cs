using System.Runtime.InteropServices;
using Fractal.Http;
// ReSharper disable SuggestVarOrType_SimpleTypes
// ReSharper disable SuggestVarOrType_Elsewhere
// ReSharper disable SuggestVarOrType_BuiltInTypes

namespace Fractal.Playground;

/// <summary>
/// Example usage of the Fractal io_uring file reactor: a multi-reactor HTTP/1.1 static-file
/// server. The whole HTTP layer — request parsing, routing, Accept-Encoding negotiation — lives
/// in the library's <see cref="StaticFileHandler"/>; this just points it at a wwwroot directory
/// and spins up one reactor per core (each with its own io_uring + SO_REUSEPORT listener).
/// </summary>
internal static class Program
{
    private static readonly string WwwRoot = Path.Combine(AppContext.BaseDirectory, "wwwroot");

    private static int Main()
    {
        bool usePipe = false;

        using var files = new StaticFileHandler(WwwRoot);

        // TODO: Test this - check for performance regressions
        //
        // Early experimental - to be included in the fractal library
        //
        // Reload the asset snapshot on SIGHUP (e.g. `kill -HUP <pid>` after a deploy). The new files
        // are opened and swapped in atomically, the old fds close after a short grace.
        using var reload = PosixSignalRegistration.Create(PosixSignal.SIGHUP, ctx =>
        {
            ctx.Cancel = true;   // handle the signal, don't let the default action terminate us
            files.Reload();

            Console.WriteLine($"[fractal] reloaded, now serving {files.FileCount} files");
        });

        var config = new ServerConfig
        {
            ReactorCount = 12, // Set as many as your harness can saturate the CPU cores with (e.g. 1 per core)
            Incremental  = false,
            Handler      = usePipe ? files.HandlePipeAsync : files.HandleAsync,
        };

        Console.WriteLine($"[fractal] serving {files.FileCount} files from '{files.RootDir}'");
        Console.WriteLine($"[fractal] {config.ReactorCount} reactors on :{config.Port} (incremental={config.Incremental}, pipe={usePipe})");

        var threads = new Thread[config.ReactorCount];
        for (var i = 0; i < config.ReactorCount; i++)
        {
            var reactor = new Reactor(i, config);
            threads[i] = new Thread(reactor.Run)
            {
                Name = $"reactor-{i}", IsBackground = false
            };

            threads[i].Start();
        }
        foreach (var t in threads)
        {
            t.Join();
        }

        return 0;
    }
}
