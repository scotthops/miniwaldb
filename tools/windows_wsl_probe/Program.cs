using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;

// A Windows console check only: no Godot dependency and no database implementation.
if (!OperatingSystem.IsWindows() || args.Length != 2 || !args[1].StartsWith('/'))
{
    Console.Error.WriteLine("Run on Windows: WindowsWslProbe DISTRIBUTION /absolute/linux/path/miniwaldb_shell");
    return 2;
}
Console.OutputEncoding = new UTF8Encoding(false);

string distribution = args[0];
string executable = args[1];
// Deliberately include spaces to exercise ArgumentList across the Windows/WSL boundary.
string directory = $"/tmp/miniwaldb Windows probe {Guid.NewGuid():N}";
Console.WriteLine($"Distribution: {distribution}\nLinux executable: {executable}\nDisposable Linux data: {directory}");

try
{
    await using (var db = await WslDatabase.OpenAsync(distribution, executable, directory))
    {
        foreach (string command in new[] { "begin", "insert 1 10", "insert 2 0", "commit" })
            await db.OkAsync(command);
        await db.ExpectPairAsync("10", "0");
        await db.OkAsync("begin");
        await db.OkAsync("update 1 7");
        await db.ExpectPairAsync("7", "0"); // Pending reads, not committed state.
        await db.CrashAsync(); // No COMMIT sent for this staged purchase.
    }
    Console.WriteLine("PASS: acknowledged pre-commit interruption stopped the Linux database.");

    await using (var db = await WslDatabase.OpenAsync(distribution, executable, directory))
    {
        await db.ExpectPairAsync("10", "0");
        foreach (string command in new[] { "begin", "update 1 7", "update 2 1", "commit" })
            await db.OkAsync(command);
        await db.CrashAsync(); // COMMIT success was received before sending SIGKILL.
    }
    Console.WriteLine("PASS: commit success received before terminating the Linux database.");

    await using (var db = await WslDatabase.OpenAsync(distribution, executable, directory))
    {
        await db.ExpectPairAsync("7", "1");
        JsonElement error = await db.SendAsync("commit");
        WslDatabase.RequireStatus(error, "error"); // No transaction; session stays usable.
        await db.ExpectPairAsync("7", "1");
        await db.OkAsync("begin");
        const string text = "café \"quote\" \\ tail  ";
        await db.OkAsync("insert 3 " + text);
        JsonElement row = await db.SendAsync("get 3");
        WslDatabase.RequireStatus(row, "ok");
        if (row.GetProperty("row").GetProperty("value").GetString() != text)
            throw new InvalidOperationException("UTF-8, JSON escaping, or trailing spaces changed.");
        await db.OkAsync("abort");
        await db.QuitAsync();
    }
    Console.WriteLine("PASS: restart recovered 7/1; ordinary errors, UTF-8, abort, and normal exit work.");
    Console.WriteLine("All Windows C# -> WSL checks passed. This tests process interruption, not power loss.");
    return 0;
}
catch (Exception error)
{
    Console.Error.WriteLine($"FAIL: {error.Message}");
    return 1;
}

sealed class WslDatabase : IAsyncDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);
    private static readonly Encoding Utf8 = new UTF8Encoding(false, true);
    private readonly string distribution;
    private readonly Process launcher;
    private readonly Task<string> diagnostics;
    private int? linuxPid;
    private bool stopped;

    private WslDatabase(string distribution, string executable, string directory)
    {
        this.distribution = distribution;
        launcher = StartWsl(distribution, executable, "--machine", "--dir", directory);
        launcher.StandardInput.NewLine = "\n";
        diagnostics = launcher.StandardError.ReadToEndAsync(); // Drain concurrently.
    }

    private static Process StartWsl(string distribution, params string[] arguments)
    {
        var info = new ProcessStartInfo("wsl.exe")
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            StandardInputEncoding = Utf8,
            StandardOutputEncoding = Utf8,
            StandardErrorEncoding = Utf8,
        };
        info.ArgumentList.Add("--distribution");
        info.ArgumentList.Add(distribution);
        info.ArgumentList.Add("--exec");
        foreach (string argument in arguments) info.ArgumentList.Add(argument);
        return Process.Start(info) ?? throw new InvalidOperationException("Could not start wsl.exe.");
    }

    public static async Task<WslDatabase> OpenAsync(string distribution, string executable, string directory)
    {
        var db = new WslDatabase(distribution, executable, directory);
        try
        {
            JsonElement ready = await db.ReadAsync();
            RequireStatus(ready, "ready");
            if (ready.GetProperty("protocol").GetInt32() != 1)
                throw new InvalidOperationException("Unsupported protocol version.");
            int pid = ready.GetProperty("pid").GetInt32();
            if (pid <= 1) throw new InvalidOperationException("Invalid Linux database PID.");
            db.linuxPid = pid;
            Console.WriteLine($"Ready: Windows launcher PID {db.launcher.Id}; Linux database PID {pid}");
            return db;
        }
        catch
        {
            await db.DisposeAsync();
            throw;
        }
    }

    private async Task<JsonElement> ReadAsync()
    {
        string? line = await launcher.StandardOutput.ReadLineAsync().WaitAsync(Timeout);
        if (line is null) throw new IOException("Database stdout closed before a response.");
        Console.WriteLine("< " + line);
        using JsonDocument document = JsonDocument.Parse(line);
        return document.RootElement.Clone(); // Keep the value after disposing the parser.
    }

    public async Task<JsonElement> SendAsync(string command)
    {
        if (stopped || launcher.HasExited) throw new IOException("Database session is closed.");
        Console.WriteLine("> " + command);
        await launcher.StandardInput.WriteLineAsync(command).WaitAsync(Timeout);
        await launcher.StandardInput.FlushAsync().WaitAsync(Timeout);
        return await ReadAsync(); // The caller sends only one command at a time.
    }

    public static void RequireStatus(JsonElement response, string expected)
    {
        if (response.GetProperty("status").GetString() != expected)
            throw new InvalidOperationException($"Expected {expected}: {response.GetRawText()}");
    }

    public async Task OkAsync(string command) => RequireStatus(await SendAsync(command), "ok");

    public async Task ExpectPairAsync(string coins, string supplies)
    {
        foreach (var (key, expected) in new[] { (1, coins), (2, supplies) })
        {
            JsonElement response = await SendAsync($"get {key}");
            RequireStatus(response, "ok");
            JsonElement row = response.GetProperty("row");
            if (row.GetProperty("id").GetInt32() != key || row.GetProperty("value").GetString() != expected)
                throw new InvalidOperationException($"Unexpected recovered/pending value for key {key}.");
        }
    }

    private async Task RunControlAsync(params string[] arguments)
    {
        using Process control = StartWsl(distribution, arguments);
        control.StandardInput.Close();
        Task<string> output = control.StandardOutput.ReadToEndAsync();
        Task<string> errors = control.StandardError.ReadToEndAsync();
        await control.WaitForExitAsync().WaitAsync(Timeout);
        string text = await output.WaitAsync(Timeout);
        string error = await errors.WaitAsync(Timeout);
        if (control.ExitCode != 0)
            throw new IOException($"WSL control failed ({control.ExitCode}): {text}{error}");
    }

    private async Task ConfirmStoppedAsync()
    {
        await launcher.WaitForExitAsync().WaitAsync(Timeout);
        if (await launcher.StandardOutput.ReadLineAsync().WaitAsync(Timeout) is not null)
            throw new IOException("Unexpected extra protocol line before EOF.");
        string errors = await diagnostics.WaitAsync(Timeout);
        if (errors.Length != 0) Console.Error.Write(errors);
        if (linuxPid is int pid)
        {
            // Verify the Linux process is gone as well as the Windows launcher exiting.
            await RunControlAsync("/usr/bin/test", "!", "-e", $"/proc/{pid}");
            Console.WriteLine($"Confirmed: Linux PID {pid} absent; launcher exited {launcher.ExitCode}; stdout EOF.");
        }
        stopped = true;
    }

    public async Task CrashAsync()
    {
        if (stopped || launcher.HasExited || linuxPid is null)
            throw new InvalidOperationException("No live owned database to interrupt.");
        await RunControlAsync("/bin/kill", "-KILL", linuxPid.Value.ToString(CultureInfo.InvariantCulture));
        await ConfirmStoppedAsync(); // Reopening is allowed only after this succeeds.
    }

    public async Task QuitAsync()
    {
        RequireStatus(await SendAsync("quit"), "bye");
        await ConfirmStoppedAsync();
        if (launcher.ExitCode != 0) throw new IOException("Normal database exit failed.");
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (!stopped)
            {
                // On failure, close input rather than commit, then await actual exit.
                // Never kill only the Windows launcher or shut down the distribution.
                if (!launcher.HasExited && linuxPid is int pid)
                    await RunControlAsync("/bin/kill", "-KILL", pid.ToString(CultureInfo.InvariantCulture));
                launcher.StandardInput.Close();
                await launcher.WaitForExitAsync().WaitAsync(Timeout);
                await diagnostics.WaitAsync(Timeout);
                if (linuxPid is int exitedPid)
                    await RunControlAsync("/usr/bin/test", "!", "-e", $"/proc/{exitedPid}");
            }
        }
        finally { launcher.Dispose(); }
    }
}
