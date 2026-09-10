#!/usr/bin/env python3

import argparse
import array
import os
import selectors
import socket
import struct
import subprocess
import sys
import threading
import time

MARKER_PLAIN = b"pmux"
MARKER_VALIDATE = b"pmuv"

V_WHO = 1
V_NAK = 2
V_ACK = 3
V_ID = 1

APP = "comdb2"
SERVICE = "replication"

READY_TIMEOUT = 180
IO_TIMEOUT = 30


class TestFailure(Exception):
    pass


class FakePmux:
    def __init__(self, unix_path, tcp_port, service_port):
        self.unix_path = unix_path
        self.tcp_port = tcp_port
        self.service_port = service_port
        self.registered = {}
        self.ports = {}
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self._stop = False

        try:
            os.unlink(unix_path)
        except FileNotFoundError:
            pass

        self.usock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.usock.bind(unix_path)
        self.usock.listen(16)

        self.tsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tsock.bind(("127.0.0.1", tcp_port))
        self.tsock.listen(16)

    def start(self):
        threading.Thread(target=self._serve, daemon=True).start()
        self.ready.wait(10)

    def stop(self):
        self._stop = True

    def _serve(self):
        sel = selectors.DefaultSelector()
        sel.register(self.usock, selectors.EVENT_READ, self._accept_unix)
        sel.register(self.tsock, selectors.EVENT_READ, self._accept_tcp)
        self.ready.set()
        while not self._stop:
            for key, _ in sel.select(timeout=0.25):
                key.data(key.fileobj)

    @staticmethod
    def _readline(conn):
        line = b""
        while not line.endswith(b"\n"):
            try:
                chunk = conn.recv(1)
            except OSError:
                return None
            if not chunk:
                return None
            line += chunk
        return line.decode().strip()

    def _accept_unix(self, sock):
        conn, _ = sock.accept()
        threading.Thread(target=self._handle_reg, args=(conn,), daemon=True).start()

    def _handle_reg(self, conn):
        line = self._readline(conn)
        if line is None:
            return
        parts = line.split()
        if len(parts) != 2 or parts[0] != "reg":
            conn.sendall(b"-1\n")
            return
        with self.lock:
            self.registered[parts[1]] = conn
            self.ports.setdefault(parts[1], self.service_port)
        conn.sendall(("%d\n" % self.service_port).encode())
        while not self._stop:
            try:
                if not conn.recv(1, socket.MSG_PEEK):
                    break
            except OSError:
                break

    def _accept_tcp(self, sock):
        conn, _ = sock.accept()
        threading.Thread(target=self._handle_cmd, args=(conn,), daemon=True).start()

    def _handle_cmd(self, conn):
        while True:
            line = self._readline(conn)
            if line is None:
                conn.close()
                return
            parts = line.split()
            if not parts:
                conn.sendall(b"-1 missing command\n")
                continue
            cmd, args = parts[0], parts[1:]

            if cmd in ("reg", "get", "use"):
                if not args:
                    conn.sendall(b"-1 missing service name\n")
                    continue
                with self.lock:
                    port = self.ports.setdefault(args[0], self.service_port)
                conn.sendall(("%d\n" % port).encode())
                continue

            if cmd == "del":
                with self.lock:
                    self.ports.pop(args[0], None)
                conn.sendall(b"0\n")
                continue

            if cmd != "rte" or not args:
                conn.sendall(b"-1 unknown command\n")
                continue

            with self.lock:
                dest = self.registered.get(args[0])
            if dest is None:
                conn.sendall(b"-1\n")
                conn.close()
                return
            validate = len(args) > 1 and args[1] == "v"
            marker = MARKER_VALIDATE if validate else MARKER_PLAIN
            dest.sendmsg([marker], [(socket.SOL_SOCKET, socket.SCM_RIGHTS,
                                     array.array("i", [conn.fileno()]))])
            conn.close()
            return


class Database:
    def __init__(self, exe, dbname, testdir, unix_path):
        self.exe = exe
        self.dbname = dbname
        self.dbdir = os.path.join(testdir, dbname)
        self.lrl = os.path.join(self.dbdir, dbname + ".lrl")
        self.logpath = os.path.join(testdir, dbname + ".db")
        self.initlog = os.path.join(testdir, dbname + ".init")
        self.unix_path = unix_path
        self.pmux_port = free_port()
        self.service_port = free_port()
        self.proc = None
        self.log = None

    def write_lrl(self):
        os.makedirs(self.dbdir, exist_ok=True)
        with open(self.lrl, "w") as f:
            f.write("name %s\n" % self.dbname)
            f.write("dir %s\n" % self.dbdir)
            f.write("portmux_port %d\n" % self.pmux_port)
            f.write("portmux_bind_path %s\n" % self.unix_path)
            f.write("eventlog_nkeep 0\n")
            f.write("logmsg level info\n")

    def create(self):
        with open(self.initlog, "w") as log:
            rc = subprocess.call(
                [self.exe, "--create", self.dbname, "--no-global-lrl", "--lrl", self.lrl],
                stdout=log, stderr=subprocess.STDOUT, cwd=self.dbdir)
        if rc != 0:
            sys.stderr.write(read_tail(self.initlog))
            raise TestFailure("failed to create database, rc=%d" % rc)

    def start(self):
        self.log = open(self.logpath, "w")
        self.proc = subprocess.Popen(
            [self.exe, self.dbname, "--no-global-lrl", "--lrl", self.lrl],
            stdout=self.log, stderr=subprocess.STDOUT, cwd=self.dbdir)

    def wait_ready(self):
        deadline = time.time() + READY_TIMEOUT
        while time.time() < deadline:
            if self.proc.poll() is not None:
                sys.stderr.write(read_tail(self.logpath))
                raise TestFailure("database exited during startup")
            if "I AM READY" in self.read_log():
                return
            time.sleep(0.25)
        sys.stderr.write(read_tail(self.logpath))
        raise TestFailure("database did not become ready")

    def read_log(self):
        try:
            with open(self.logpath, errors="replace") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self.log:
            self.log.close()


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def read_tail(path, limit=4000):
    try:
        with open(path, errors="replace") as f:
            return f.read()[-limit:]
    except FileNotFoundError:
        return ""


class Connection:
    def __init__(self, pmux_port, dbname, validate):
        self.dbname = dbname
        self.sock = socket.create_connection(("127.0.0.1", pmux_port), timeout=IO_TIMEOUT)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.triplet = "%s/%s/%s" % (APP, SERVICE, dbname)
        suffix = " v" if validate else ""
        self.sock.sendall(("rte %s%s\n" % (self.triplet, suffix)).encode())
        ack = self.recv_exact(2)
        expected = b"1\n" if validate else b"0\n"
        if ack != expected:
            raise TestFailure("route ack was %r, expected %r" % (ack, expected))

    def recv_exact(self, count):
        out = b""
        while len(out) < count:
            chunk = self.sock.recv(count - len(out))
            if not chunk:
                raise TestFailure("connection closed after %d of %d bytes" % (len(out), count))
            out += chunk
        return out

    def recv_line(self):
        out = b""
        while not out.endswith(b"\n"):
            chunk = self.sock.recv(1)
            if not chunk:
                raise TestFailure("connection closed while reading a line, got %r" % out)
            out += chunk
        return out

    def identify(self):
        self.sock.sendall(bytes([V_WHO]))
        response = self.recv_exact(1)[0]
        if response != V_ID:
            raise TestFailure("expected V_ID, got %d" % response)
        size = struct.unpack("!I", self.recv_exact(4))[0]
        payload = self.recv_exact(size)
        if self.triplet.encode() not in payload:
            raise TestFailure("identity %r does not contain %r" % (payload, self.triplet))

    def send(self, data):
        self.sock.sendall(data)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def expect_line(conn, want, label):
    got = conn.recv_line()
    if got != want:
        raise TestFailure("%s: got %r, expected %r" % (label, got, want))


def check_logdelete4(conn, label):
    expect_line(conn, b"log file deletion disabled\n", label + " handshake")
    conn.send(b"copy_complete\n")
    expect_line(conn, b"ok\n", label + " copy_complete")


def case_validated_uncoalesced(db):
    conn = Connection(db.pmux_port, db.dbname, validate=True)
    try:
        conn.identify()
        conn.send(bytes([V_ACK]))
        time.sleep(0.2)
        conn.send(b"logdelete4\n")
        check_logdelete4(conn, "uncoalesced")
    finally:
        conn.close()


def case_validated_coalesced(db):
    conn = Connection(db.pmux_port, db.dbname, validate=True)
    try:
        conn.identify()
        conn.send(bytes([V_ACK]) + b"logdele")
        time.sleep(0.2)
        conn.send(b"te4\n")
        check_logdelete4(conn, "coalesced")
    finally:
        conn.close()


def case_validated_coalesced_arbitrary(db):
    token = "pmuvcoalescecheck"
    before = len(db.read_log())
    conn = Connection(db.pmux_port, db.dbname, validate=True)
    try:
        conn.identify()
        payload = token.encode() + b"\n"
        conn.send(bytes([V_ACK]) + payload[:7])
        time.sleep(0.2)
        conn.send(payload[7:])
        expect_line(conn, b"Error: -1 #unknown command\n", "arbitrary appsock")
    finally:
        conn.close()
    time.sleep(0.5)
    produced = db.read_log()[before:]
    want = "appsock '%s' not supported" % token
    if want not in produced:
        truncated = "appsock '%s' not supported" % token[7:]
        if truncated in produced:
            raise TestFailure("dispatcher saw the truncated token %r" % token[7:])
        raise TestFailure("dispatcher never reported %r" % token)


def case_plain_route(db):
    conn = Connection(db.pmux_port, db.dbname, validate=False)
    try:
        conn.send(b"logdelete4\n")
        check_logdelete4(conn, "plain route")
    finally:
        conn.close()


CASES = [
    ("plain route, no validation", case_plain_route),
    ("validated route, V_ACK sent alone", case_validated_uncoalesced),
    ("validated route, V_ACK coalesced with logdelete4", case_validated_coalesced),
    ("validated route, V_ACK coalesced with arbitrary appsock", case_validated_coalesced_arbitrary),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--comdb2-exe", required=True)
    parser.add_argument("--testdir", required=True)
    parser.add_argument("--dbname", required=True)
    args = parser.parse_args()

    tmpdir = os.path.join(args.testdir, "tmp")
    os.makedirs(tmpdir, exist_ok=True)
    unix_path = os.path.join(tmpdir, args.dbname + ".pmuv.socket")

    db = Database(args.comdb2_exe, args.dbname, args.testdir, unix_path)
    pmux = FakePmux(unix_path, db.pmux_port, db.service_port)
    pmux.start()

    failures = []
    try:
        db.write_lrl()
        db.create()
        db.start()
        db.wait_ready()

        for name, fn in CASES:
            try:
                fn(db)
            except (TestFailure, OSError) as exc:
                failures.append((name, exc))
                print("FAIL %s: %s" % (name, exc), flush=True)
            else:
                print("PASS %s" % name, flush=True)
    except TestFailure as exc:
        print("FAIL setup: %s" % exc, flush=True)
        failures.append(("setup", exc))
    finally:
        db.stop()
        pmux.stop()

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
