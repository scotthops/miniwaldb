"""Exercise the real Linux shell through persistent pipes, using disposable data."""

import argparse
import json
from pathlib import Path
import queue
import signal
import struct
import subprocess
import tempfile
import threading
import unittest


class Shell:
    def __init__(self, executable, directory):
        self.process = subprocess.Popen(
            [str(executable), "--machine", "--dir", str(directory)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.lines = queue.Queue()
        self.diagnostics = b""
        self.output_thread = threading.Thread(target=self._read_output, daemon=True)
        self.error_thread = threading.Thread(target=self._read_errors, daemon=True)
        self.output_thread.start()
        self.error_thread.start()

    def _read_output(self):
        for line in self.process.stdout:
            self.lines.put(line)
        self.lines.put(None)

    def _read_errors(self):
        # Continuously drain stderr, even while waiting for a stdout response.
        self.diagnostics = self.process.stderr.read()

    def response(self):
        # A deadline detects missing flushes; it does not coordinate crash timing.
        line = self.lines.get(timeout=5)
        assert line is not None, "database exited without a response"
        assert line.endswith(b"\n"), "response is not a complete line"
        result = json.loads(line.decode("utf-8"))
        assert isinstance(result, dict), result
        return result

    def ready(self):
        result = self.response()
        assert result == {"status": "ready", "protocol": 1, "pid": self.process.pid}, result
        assert self.process.poll() is None, "database exited after readiness"

    def command(self, line):
        self.process.stdin.write((line + "\n").encode("utf-8"))
        self.process.stdin.flush()
        result = self.response()
        if result["status"] != "bye":
            assert self.process.poll() is None, "database exited after a command"
        return result

    def ok(self, line):
        assert self.command(line) == {"status": "ok"}, line

    def pair(self):
        coins = self.command("get 1")
        supplies = self.command("get 2")
        assert coins["status"] == supplies["status"] == "ok"
        return coins["row"]["value"], supplies["row"]["value"]

    def initialize(self):
        for line in ("begin", "insert 1 10", "insert 2 0", "commit"):
            self.ok(line)

    def purchase(self):
        for line in ("begin", "update 1 7", "update 2 1", "commit"):
            self.ok(line)

    def wait(self, expected):
        assert self.process.wait(timeout=5) == expected
        self.output_thread.join(timeout=5)
        self.error_thread.join(timeout=5)
        assert not self.output_thread.is_alive()
        assert not self.error_thread.is_alive()
        # Every response was consumed; only EOF may remain, never an extra prompt/line.
        assert self.lines.get(timeout=5) is None
        assert self.lines.empty()

    def quit(self):
        assert self.command("quit") == {"status": "bye"}
        self.wait(0)

    def kill(self):
        # This Popen owns the Linux database itself, not a Windows WSL launcher.
        self.process.kill()
        self.wait(-signal.SIGKILL)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=5)
        self.output_thread.join(timeout=5)
        self.error_thread.join(timeout=5)
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            stream.close()


class MachinePipes(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="miniwaldb-pipes-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / "data with spaces"

    def shell(self):
        return Shell(EXECUTABLE, self.directory)

    def test_live_responses_errors_json_and_abort(self):
        with self.shell() as shell:
            shell.ready()
            self.assertEqual(shell.command("get 99"), {"status": "ok", "row": None})
            shell.initialize()
            for command in ("", "commit", "get 1 extra", "update 9 missing", 'unknown"\\'):
                result = shell.command(command)
                self.assertEqual(result["status"], "error")
                self.assertTrue(result["message"].startswith("error:"))
            shell.ok("begin")
            shell.ok("update 1 7")
            self.assertEqual(shell.pair(), ("7", "0"))  # Read-your-writes.
            shell.ok("abort")
            self.assertEqual(shell.pair(), ("10", "0"))
            shell.ok("begin\r")
            shell.ok("update 1 8\r")
            self.assertEqual(shell.pair(), ("8", "0"))  # Windows CRLF is framing.
            shell.ok("abort\r")
            shell.ok("begin")
            value = 'quote" backslash\\ tab\t café \x01  '
            shell.ok("insert 3 " + value)
            self.assertEqual(shell.command("get 3"), {
                "status": "ok", "row": {"id": 3, "value": value},
            })
            self.assertEqual(shell.command("ids"), {"status": "ok", "ids": [1, 2, 3]})
            self.assertEqual(shell.command("values"), {"status": "ok", "values": ["10", "0", value]})
            self.assertEqual(len(shell.command("scan")["rows"]), 3)
            self.assertEqual(shell.command("select-value " + value)["rows"], [{"id": 3, "value": value}])
            self.assertIn("begin | commit", shell.command("help")["text"])
            shell.ok("delete 3")
            shell.ok("abort")
            shell.quit()
            self.assertIn(b"error:", shell.diagnostics)

    def test_committed_purchase_survives_normal_restart_and_eof(self):
        with self.shell() as shell:
            shell.ready()
            shell.initialize()
            shell.purchase()
            self.assertEqual(shell.pair(), ("7", "1"))
            shell.quit()
            self.assertEqual(shell.diagnostics, b"")
        with self.shell() as shell:
            shell.ready()
            self.assertEqual(shell.pair(), ("7", "1"))
            shell.ok("begin")
            shell.ok("update 1 4")
            shell.process.stdin.close()  # EOF is not an implicit commit or a command.
            shell.wait(0)
        with self.shell() as shell:
            shell.ready()
            self.assertEqual(shell.pair(), ("7", "1"))
            shell.quit()

    def test_acknowledged_precommit_interruption_recovers_previous_pair(self):
        with self.shell() as shell:
            shell.ready()
            shell.initialize()
            shell.ok("begin")
            shell.ok("update 1 7")
            self.assertEqual(shell.pair(), ("7", "0"))
            shell.kill()  # No COMMIT is sent; no sleep or timing race.
        with self.shell() as shell:
            shell.ready()
            self.assertEqual(shell.pair(), ("10", "0"))
            shell.purchase()  # Recovery also permits another transaction.
            shell.quit()

    def test_kill_after_commit_response_preserves_purchase(self):
        with self.shell() as shell:
            shell.ready()
            shell.initialize()
            shell.purchase()  # Includes receiving the COMMIT response.
            shell.kill()
        with self.shell() as shell:
            shell.ready()
            self.assertEqual(shell.pair(), ("7", "1"))
            shell.quit()

    def test_startup_corruption_is_fatal_without_ready(self):
        self.directory.mkdir()
        wal = self.directory / "wal.log"
        corrupt = bytes([1, 0, 0, 0])
        wal.write_bytes(corrupt)
        with self.shell() as shell:
            result = shell.response()
            self.assertEqual(result["status"], "fatal")
            self.assertIn("WAL corruption", result["message"])
            shell.wait(1)
            self.assertIn(b"WAL corruption", shell.diagnostics)
        self.assertEqual(wal.read_bytes(), corrupt)

    def test_existing_snapshot_escapes_all_control_bytes(self):
        self.directory.mkdir()
        value = "".join(chr(i) for i in range(32)) + '"\\ café'
        encoded = value.encode("utf-8")
        # Independent fixture in the existing MWS1 format: count, key, length, bytes.
        original = struct.pack("<4sIqI", b"MWS1", 1, 1, len(encoded)) + encoded
        snapshot = self.directory / "snapshot.dat"
        snapshot.write_bytes(original)
        with self.shell() as shell:
            shell.ready()
            self.assertEqual(shell.command("get 1"), {
                "status": "ok", "row": {"id": 1, "value": value},
            })
            shell.quit()
            self.assertEqual(shell.diagnostics, b"")
        self.assertEqual(snapshot.read_bytes(), original)

    def test_human_cli_and_argument_errors(self):
        human = subprocess.run(
            [str(EXECUTABLE), "--dir", str(self.directory)],
            input="begin\ninsert 1 10\ncommit\nget 1\nexit\n", text=True,
            capture_output=True, timeout=5, cwd=self.temp.name,
        )
        self.assertEqual(human.returncode, 0)
        self.assertIn("miniwaldb: items", human.stdout)
        self.assertIn("> 1 | 10\n", human.stdout)
        self.assertEqual(human.stderr, "")
        for arguments in (["--dir"], ["--dir", ""], ["--dir", "--machine"], ["--unknown"]):
            result = subprocess.run(
                [str(EXECUTABLE)] + arguments, capture_output=True, text=True,
                timeout=5, cwd=self.temp.name,
            )
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")
            self.assertIn("usage:", result.stderr)
        default = subprocess.run(
            [str(EXECUTABLE)], input="exit\n", capture_output=True, text=True,
            timeout=5, cwd=self.temp.name,
        )
        self.assertEqual(default.returncode, 0)
        self.assertTrue((Path(self.temp.name) / "dbdata" / "wal.log").is_file())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shell", required=True, type=Path)
    EXECUTABLE = parser.parse_args().shell.resolve()
    unittest.main(argv=[__file__], verbosity=2)
