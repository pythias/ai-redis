#!/usr/bin/env python3
"""Integration test for ai-redis PubSub commands."""

import subprocess
import socket
import time
import sys

def send_command(s, *args):
    """Send a RESP command."""
    parts = [f"*{len(args)}\r\n"]
    for arg in args:
        arg = str(arg)
        parts.append(f"${len(arg)}\r\n{arg}\r\n")
    s.send("".join(parts).encode())

def recv_response(s):
    """Receive a RESP response."""
    s.settimeout(2)
    data = b""
    while True:
        try:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
            break
        except socket.timeout:
            break
    return data

def test_pubsub():
    # Start server
    server = subprocess.Popen(
        ["./target/debug/ai-redis"],
        cwd="/Users/chenjie5/Desktop/claw/code/ai-redis",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(1)

    try:
        s = socket.socket()
        s.connect(("127.0.0.1", 6379))
        s.settimeout(3)

        results = []

        # PUBLISH to a channel (no subscribers yet)
        send_command(s, "PUBLISH", "news", "hello world")
        resp = recv_response(s)
        results.append(("PUBLISH (no sub)", resp == b":0\r\n", resp))

        # PUBSUB CHANNELS (empty)
        send_command(s, "PUBSUB", "CHANNELS")
        resp = recv_response(s)
        results.append(("PUBSUB CHANNELS", resp == b"*0\r\n", resp))

        # PUBSUB NUMSUB
        send_command(s, "PUBSUB", "NUMSUB", "news")
        resp = recv_response(s)
        results.append(("PUBSUB NUMSUB news", b":0\r\n" in resp, resp))

        # PUBSUB NUMPAT
        send_command(s, "PUBSUB", "NUMPAT")
        resp = recv_response(s)
        results.append(("PUBSUB NUMPAT", resp == b":0\r\n", resp))

        # SET a key (to test normal commands still work)
        send_command(s, "SET", "foo", "bar")
        resp = recv_response(s)
        results.append(("SET foo bar", resp == b"+OK\r\n", resp))

        # GET the key
        send_command(s, "GET", "foo")
        resp = recv_response(s)
        results.append(("GET foo", b"$3\r\nbar\r\n" in resp, resp))

        # SHUTDOWN
        send_command(s, "SHUTDOWN", "NOSAVE")
        recv_response(s)

        s.close()
    finally:
        server.terminate()
        server.wait(timeout=5)

    print("\n=== PubSub Integration Test ===")
    all_pass = True
    for name, passed, resp in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {name}")
        if not passed:
            print(f"       got: {resp!r}")
            all_pass = False

    print()
    if all_pass:
        print("All tests passed!")
        return 0
    else:
        print("Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(test_pubsub())
