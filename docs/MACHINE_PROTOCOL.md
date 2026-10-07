# Shell machine interface, version 1

Run the existing shell with a separate demo directory:

```sh
./build/miniwaldb_shell --machine --dir /tmp/storm-shop-demo
```

No flags still starts the human REPL using `./dbdata`. `--dir DIRECTORY` also
works in human mode. Argument errors print usage on stderr and exit with status 2,
before opening a database; they produce no protocol message.

## Requests and responses

Requests use the existing command syntax, **not JSON**: one UTF-8 command line at
a time. LF and CRLF line endings are accepted in machine mode. Send the next
command only after reading the previous response. There are no request IDs.

After `Db` construction and recovery succeed, stdout receives one flushed line:

```json
{"status":"ready","protocol":1,"pid":12345}
```

`pid` is the actual **Linux shell/database PID**, not a Windows `wsl.exe` PID.
There is no banner or prompt. Readiness is separate from command responses.

Each command line, including a blank line, receives one complete, flushed JSON
object on stdout while the output pipe is usable:

| Request | Response |
| --- | --- |
| `begin`, `insert ID TEXT`, `update ID TEXT`, `delete ID`, `commit`, `abort`, `checkpoint` | `{"status":"ok"}` |
| `get 1` when present | `{"status":"ok","row":{"id":1,"value":"10"}}` |
| `get 99` when missing | `{"status":"ok","row":null}` |
| `scan`, `select-value TEXT` | `{"status":"ok","rows":[{"id":1,"value":"10"}]}` |
| `ids` | `{"status":"ok","ids":[1,2]}` |
| `values` | `{"status":"ok","values":["10","0"]}` |
| `help` | `{"status":"ok","text":"..."}` |
| `quit`, `exit` | `{"status":"bye"}`, then exit 0 |
| Ordinary command error | `{"status":"error","message":"error: not in transaction"}` |
| Fatal persistence/startup/input error | `{"status":"fatal","message":"..."}`, then exit 1 |

Empty result lists are `[]`. IDs are signed 64-bit JSON integers, and values remain
strings. Error message wording is diagnostic text; use `status` for control flow.
Diagnostics also go to stderr, which a client must drain independently of stdout.
There are no progress messages or literal WAL records in this protocol.

The existing text parser removes leading separator whitespace and preserves
internal/trailing spaces. Quotes and backslashes in a request are literal text,
not escapes. TEXT must be nonempty. Values in machine output must be valid UTF-8
text; arbitrary invalid-UTF-8 binary values from the C++ API are outside this
protocol's scope. JSON output escapes quotes, backslashes, and all control bytes
U+0000–U+001F; valid UTF-8 sequences are preserved. Machine CRLF handling removes
the final CR, so a literal trailing CR cannot be represented through this mode.
Human-mode text handling is unchanged.

EOF is not a command: it closes the session with exit 0 and no final JSON message.
Quit, exit, and EOF never commit or implicitly abort pending changes. Startup
failure sends `fatal` without `ready`. A broken output pipe or process interruption
can prevent a response; clients must also watch process exit and pipe EOF.

## Purchase and interruption

Use one long-lived child process for this sequence, waiting for `ok` after every
mutation/transaction command:

```text
begin
insert 1 10
insert 2 0
commit
begin
update 1 7
update 2 1
commit
get 1
get 2
quit
```

Restart against the same directory; reads return `7` and `1`. Initialize only a
fresh, empty demonstration database. The shell does not implement shop rules.

To stage a purchase, issue `begin` and `update 1 4`, receive their acknowledgments,
and send no commit. Reads in that transaction return pending `4`/`1`. Terminate
the database, confirm exit, and reopen: reads return committed `7`/`1`. Terminating
after a successful commit response instead preserves the committed purchase.

`commit` calls `Db::commit()` before building its success response. `Db` appends
COMMIT, synchronizes the WAL with its existing POSIX implementation, publishes
working state, and returns. Flushing stdout only delivers response bytes to the
client; it does not synchronize database storage.

An ordinary parser/constraint/transaction error leaves the session running and
does not automatically end the transaction. A persistence failure stops the
database instance. A failed commit or lost response is **not** proof of rollback:
reopen and inspect recovered state before deciding what to do. Do not automatically
retry a purchase after an uncertain outcome.

These checks demonstrate process interruption with the OS still running, not
physical power loss. The WAL format, recovery, snapshots, and synchronous commit
ordering are unchanged.

## Windows Godot C# and WSL

The selected arrangement is **Windows Godot C# with miniwaldb in WSL2**. For this
environment, the database runs in WSL's `Ubuntu` distribution. A Windows client
launches this command using `System.Diagnostics.Process`, with
`UseShellExecute = false` and stdin/stdout/stderr redirected:

```text
wsl.exe --distribution Ubuntu --exec /absolute/linux/path/miniwaldb_shell --machine --dir /absolute/linux/path/demo-data
```

Use `ProcessStartInfo.ArgumentList` in the Windows Godot client, rather than building
a shell command string. Set the stdin writer to UTF-8 without a BOM (and preferably
`NewLine = "\n"`). Read responses and stderr asynchronously to keep Godot responsive.
The PID returned by the Windows Process object belongs to the WSL launcher;
readiness supplies the Linux PID.

For a deliberate crash, invoke a second WSL process:

```text
wsl.exe --distribution Ubuntu --exec /bin/kill -KILL LINUX_PID_FROM_READY
```

Require the kill command to succeed, then await exit of the original launcher
and stdout EOF. Also confirm `/proc/LINUX_PID` is absent through a second WSL
invocation of `/usr/bin/test ! -e /proc/LINUX_PID` before reopening the directory.
If confirmation fails, stop instead of starting a second owner. Normal shutdown sends `quit` and
waits for `bye` and process exit. Do not kill the WSL distribution or rely on killing
only the Windows launcher. Do not start another database owner for the directory
before the first exits.

A Windows `System.Diagnostics.Process` smoke check was run for this milestone:
readiness and command responses over WSL pipes, Windows CRLF requests, a staged
coin update, killing the reported Linux PID, waiting for launcher exit, and reopening
to committed `10`/`0`. Windows SDKs 6, 7, 8, and 10 were found; Godot was not on
PATH. **Godot itself has not been tested.** Before the next milestone, open your
Godot .NET editor on Windows and build/run one empty C# scene. Verify the editor's
required SDK.

The follow-up [Windows C# probe](../tools/windows_wsl_probe/README.md) has also been
built and executed on Windows. It verified readiness, persistent commands, JSON
and UTF-8, an absolute WSL data path containing spaces, both pre-commit and
post-acknowledgment crashes, confirmed Linux process termination, recovery, and
normal shutdown. This is the integration route for the game; Linux Godot and a
native Windows database port are unnecessary. The future game's long-lived data
directory should be an absolute path inside WSL, separate from personal data;
the probe's `/tmp` directory is deliberately disposable.

## Verification and code-reading order

`tests/test_machine_repl.cpp` tests structured results, escaping, ordinary errors,
commit acknowledgment ordering, injected sync failure, and startup corruption.
`tests/test_machine_pipes.py` uses actual pipes with disposable directories; it
checks flushing while alive, transaction state, abort, normal/EOF restart,
pre-commit SIGKILL, post-acknowledgment SIGKILL, human CLI, and argument errors.
The standard-library Python check is registered in CTest when Python 3 is found:

```sh
python3 tests/test_machine_pipes.py --shell /absolute/path/to/miniwaldb_shell
```

Read these pieces in order:

1. `tools/shell.cpp`: select mode and directory, then call `run_shell`.
2. `tools/repl.h`: `ShellMode` and default human-mode arguments.
3. `tools/repl.cpp`: `run_shell` owns a local `Db`; `run_repl` borrows it by reference.
   Follow the one command loop, response buffer, JSON helpers, and error handling.
4. `src/table/items_table.cpp`: existence checks and delegation to `Db`.
5. `src/db/db.cpp`: working-copy reads and commit ordering, unchanged here.
6. The two new tests: failure injection versus actual process interruption.

`Db&` means a reference to the same object, not a new database for each command.
`ShellMode::Machine` selects an enum value. `std::ostringstream` builds one response
in memory. `std::flush` delivers buffered stream output. `::getpid()` calls the
POSIX function from the global namespace. The anonymous namespace keeps JSON and
presentation helpers private to this source file.
