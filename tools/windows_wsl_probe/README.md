# Windows C# → WSL database probe

This is a small console verification tool, not a Godot game or a database binding.
It uses the existing machine protocol without changing miniwaldb. It requires
Windows, a .NET 8 SDK/runtime, and a machine-enabled Linux shell in WSL2.
There are no NuGet package dependencies.

From **Windows PowerShell**, select the distribution explicitly:

```powershell
wsl.exe --list --verbose
$project = "\\wsl.localhost\Ubuntu\home\scott\projects\miniwaldb\tools\windows_wsl_probe\WindowsWslProbe.csproj"
dotnet run --project $project -- Ubuntu /tmp/miniwaldb-machine-Usf9Un/miniwaldb_shell
```

Replace `Ubuntu` with the exact distribution name, including in the project UNC
path if needed. Replace the executable with your absolute Linux path to the
machine-enabled build. The `/tmp` executable above was the isolated build used
for this check; use a stable reviewed build path for the eventual game.

The probe automatically chooses a new `/tmp/miniwaldb Windows probe GUID`
directory inside WSL. It never opens personal database data. The data remains
available at the printed path for inspection. `ArgumentList` handles the spaces
in that directory without a shell command string.

It verifies, with responses coordinating all transaction boundaries:

1. Readiness includes protocol version and the actual Linux database PID.
2. One persistent session initializes `10` coins and `0` supplies, then stages
   coins `7` without committing. Pending reads return `7`/`0`.
3. A second `wsl.exe` runs `/bin/kill -KILL` on that reported PID. The probe awaits
   the original launcher exit, consumes stdout EOF, and checks `/proc/PID` is absent.
4. Only after those checks, reopen the same directory and read committed `10`/`0`.
5. Commit a two-key purchase, receive success, then terminate and confirm exit again.
6. Reopen and read `7`/`1`; verify an ordinary command error, UTF-8 text and JSON
   escaping, abort, and a normal `quit`/`bye` exchange with confirmed process exit.

The Windows launcher PID is printed separately and is never the crash target.
The distribution remains running. There are no sleeps, networking, game assets,
or automatic purchase retries. Ten-second deadlines detect failed operations;
they do not select crash timing. Failure prints `FAIL` and exits nonzero rather
than continuing to the next session.

Read `Program.cs` from the scenario sequence through `StartWsl`, `ReadAsync`,
`SendAsync`, `CrashAsync`, and `ConfirmStoppedAsync`. `StandardError.ReadToEndAsync`
starts immediately so stderr is drained while responses are awaited. The stdin
writer uses UTF-8 without a BOM and LF newlines. `JsonElement.Clone` keeps a response
usable after its temporary parser is disposed.

For the eventual Windows Godot C# game, keep this launch/encoding/termination
sequence in a small process client and await commands from its session controller.
Keep scene updates on Godot's main thread. Configure the executable and durable
demo-data paths as absolute Linux paths, with data inside WSL rather than `/mnt/c`.
The probe uses `/tmp` only for disposable verification.

## Observed verification

Built and executed on Windows using the installed .NET 8.0.131 SDK. The build
succeeded without warnings or errors using local framework packs and an empty
package-source configuration. All three sessions and both crash/recovery scenarios
passed. This WSL launcher reported exit code **9** after SIGKILL and **0** after
normal quit. The proof of termination is the Linux PID absence plus launcher exit
and stdout EOF, not an assumed shell-style exit code of 137.

The earlier Linux/C++ tests remain separate. This probe proves the Windows C#
process boundary; it does **not** prove Godot scene integration or physical
power-loss durability. No Godot scene has been built. The remaining editor check
is to build/run one empty C# scene in Windows Godot before adding the shop UI.
