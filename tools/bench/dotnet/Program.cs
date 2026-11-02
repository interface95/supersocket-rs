using System.Globalization;
using System.Runtime;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SuperSocket.Command;
using SuperSocket.Connection;
using SuperSocket.ProtoBase;
using SuperSocket.Server;
using SuperSocket.Server.Abstractions;
using SuperSocket.Server.Abstractions.Middleware;
using SuperSocket.Server.Abstractions.Session;
using SuperSocket.Server.Host;

namespace PipelineBench;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        var settings = new BenchSettings(
            ReadInteger("BENCH_PORT", 4040, 65535),
            ReadInteger("BENCH_MAX_SESSIONS", 20000, 1000000));
        using var host = SuperSocketHostBuilder.Create<StringPackageInfo, CommandLinePipelineFilter>(args)
            .UseSession<BenchSession>()
            .UseMiddleware<AdmissionMiddleware>()
            .UsePackageHandlingScheduler<SerialPackageHandlingScheduler<StringPackageInfo>>()
            .UseCommand(commands =>
            {
                commands.AddCommand<NameCommand>();
                commands.AddCommand<WhoAmICommand>();
                commands.AddCommand<EchoCommand>();
                commands.AddCommand<StatsCommand>();
                commands.AddCommand<QuitCommand>();
                commands.RegisterUnknownPackageHandler<StringPackageInfo>((session, package, token) =>
                    ((BenchSession)session).ReplyAsync("ERR unknown command\r\n", token));
            })
            .ConfigureServices((context, services) => services.AddSingleton(settings))
            .ConfigureSuperSocket(options =>
            {
                options.Name = "pipeline-bench";
                options.MaxPackageLength = 4096;
                options.ReceiveBufferSize = 4096;
                options.SendBufferSize = 4096;
                options.SendTimeout = 5000;
                options.PackageHandlingTimeOut = 30;
                options.DefaultTextEncoding = new UTF8Encoding(false, true);
                options.Listeners =
                [
                    new ListenOptions
                    {
                        Ip = "127.0.0.1",
                        Port = settings.Port,
                        BackLog = 1024,
                        NoDelay = true
                    }
                ];
            })
            .ConfigureLogging((context, logging) => logging.ClearProviders())
            .Build();

        await host.StartAsync();
        Console.WriteLine(
            $"LISTENING 127.0.0.1:{settings.Port} framework={RuntimeInformation.FrameworkDescription} " +
            $"server_gc={GCSettings.IsServerGC} processors={Environment.ProcessorCount} " +
            $"echo_impl={typeof(EchoCommand).GetMethod(nameof(EchoCommand.ExecuteAsync))!.MethodImplementationFlags}");
        await host.WaitForShutdownAsync();
    }

    private static int ReadInteger(string name, int fallback, int maximum)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (value is null)
            return fallback;
        if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed)
            || parsed < 1 || parsed > maximum)
            throw new ArgumentException($"{name} must be an integer in 1..{maximum}.");
        return parsed;
    }
}

public sealed record BenchSettings(int Port, int MaxSessions);

public sealed class BenchSession() : AppSession
{
    private readonly object _nameLock = new();
    private string? _name;
    private long _echoCount;

    public bool HasName
    {
        get { lock (_nameLock) return _name is not null; }
    }

    public string Name
    {
        get { lock (_nameLock) return _name ?? "anonymous"; }
    }

    public long EchoCount => Interlocked.Read(ref _echoCount);

    public void SetName(string name)
    {
        lock (_nameLock)
            _name = name;
    }

    public void CountEcho() => Interlocked.Increment(ref _echoCount);

    public ValueTask ReplyAsync(string reply, CancellationToken cancellationToken = default) =>
        ((IAppSession)this).SendAsync(Encoding.UTF8.GetBytes(reply), cancellationToken);

    protected override ValueTask OnSessionConnectedAsync() => ReplyAsync("READY\r\n");
}

public sealed class AdmissionMiddleware(BenchSettings settings) : MiddlewareBase
{
    private int _active;

    public override async ValueTask<bool> RegisterSession(IAppSession session)
    {
        while (true)
        {
            var active = Volatile.Read(ref _active);
            if (active >= settings.MaxSessions)
            {
                // SuperSocket 2.1 rejects before starting this connection's send loop. This
                // uses the normal API, but a rejected peer may observe EOF without this line.
                await ((BenchSession)session).ReplyAsync("ERR server busy\r\n");
                return false;
            }
            if (Interlocked.CompareExchange(ref _active, active + 1, active) == active)
                return true;
        }
    }

    public override ValueTask<bool> UnRegisterSession(IAppSession session)
    {
        // Rejected sessions do not get this callback in SuperSocket 2.1.
        Interlocked.Decrement(ref _active);
        return ValueTask.FromResult(true);
    }
}

[Command(Key = "NAME")]
public sealed class NameCommand() : IAsyncCommand<BenchSession, StringPackageInfo>
{
    public async ValueTask ExecuteAsync(BenchSession session, StringPackageInfo package, CancellationToken cancellationToken)
    {
        var name = (package.Body ?? string.Empty).Trim();
        if (name.Length is < 1 or > 32 || name.Any(character =>
                !(character is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '_')))
        {
            await session.ReplyAsync("ERR invalid name\r\n", cancellationToken);
            return;
        }
        session.SetName(name);
        await session.ReplyAsync($"OK name={name}\r\n", cancellationToken);
    }
}

[Command(Key = "WHOAMI")]
public sealed class WhoAmICommand() : IAsyncCommand<BenchSession, StringPackageInfo>
{
    public ValueTask ExecuteAsync(BenchSession session, StringPackageInfo package, CancellationToken cancellationToken) =>
        session.ReplyAsync(string.IsNullOrWhiteSpace(package.Body)
            ? $"OK name={session.Name}\r\n" : "ERR usage: WHOAMI\r\n", cancellationToken);
}

public sealed class RequireNameAttribute() : AsyncCommandFilterAttribute
{
    public override async ValueTask<bool> OnCommandExecutingAsync(CommandExecutingContext context)
    {
        var session = (BenchSession)context.Session;
        if (!session.HasName)
        {
            await session.ReplyAsync("ERR set NAME first\r\n", context.CancellationToken);
            return false;
        }
        var body = ((StringPackageInfo)context.Package).Body ?? string.Empty;
        if (string.IsNullOrWhiteSpace(body) || body.Any(char.IsControl))
        {
            await session.ReplyAsync("ERR invalid echo\r\n", context.CancellationToken);
            return false;
        }
        return true;
    }

    public override ValueTask OnCommandExecutedAsync(CommandExecutingContext context)
    {
        if (context.Exception is null)
            ((BenchSession)context.Session).CountEcho();
        return ValueTask.CompletedTask;
    }
}

[Command(Key = "ECHO"), RequireName]
public sealed class EchoCommand() : IAsyncCommand<BenchSession, StringPackageInfo>
{
    public async ValueTask ExecuteAsync(BenchSession session, StringPackageInfo package, CancellationToken cancellationToken) =>
        await session.ReplyAsync($"OK echo={package.Body}\r\n", cancellationToken);
}

[Command(Key = "STATS")]
public sealed class StatsCommand() : IAsyncCommand<BenchSession, StringPackageInfo>
{
    public ValueTask ExecuteAsync(BenchSession session, StringPackageInfo package, CancellationToken cancellationToken) =>
        session.ReplyAsync(string.IsNullOrWhiteSpace(package.Body)
            ? $"OK count={session.EchoCount}\r\n" : "ERR usage: STATS\r\n", cancellationToken);
}

[Command(Key = "QUIT")]
public sealed class QuitCommand() : IAsyncCommand<BenchSession, StringPackageInfo>
{
    public async ValueTask ExecuteAsync(BenchSession session, StringPackageInfo package, CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(package.Body))
        {
            await session.ReplyAsync("ERR usage: QUIT\r\n", cancellationToken);
            return;
        }
        await session.ReplyAsync("BYE\r\n", cancellationToken);
        await session.CloseAsync(CloseReason.LocalClosing);
    }
}
