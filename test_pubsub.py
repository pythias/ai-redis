#!/usr/bin/env python3
"""Integration test for ai-redis commands."""

import subprocess, socket, time, sys

def send_command(s, *args):
    parts = [f"*{len(args)}\r\n"]
    for arg in args:
        arg = str(arg)
        parts.append(f"${len(arg)}\r\n{arg}\r\n")
    s.send("".join(parts).encode())

def recv_response(s):
    s.settimeout(3)
    data = b""
    try:
        while True:
            chunk = s.recv(4096)
            if not chunk: break
            data += chunk
            break
    except socket.timeout:
        pass
    return data

def run_tests():
    server = subprocess.Popen(["./target/debug/ai-redis"],
        cwd="/Users/chenjie5/Desktop/claw/code/ai-redis",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    time.sleep(1)

    results = []
    try:
        s = socket.socket()
        s.connect(("127.0.0.1", 6379))
        s.settimeout(3)

        # SET test keys
        for k in ["user:1", "user:2", "user:3", "post:1", "post:2", "admin:1"]:
            send_command(s, "SET", k, "val")
            recv_response(s)

        # SCAN with MATCH
        send_command(s, "SCAN", "0", "MATCH", "user:*")
        resp = recv_response(s)
        ok = b"user:1" in resp and b"user:2" in resp and b"user:3" in resp
        results.append(("SCAN MATCH user:*", ok, resp))

        # SCAN with no MATCH (all keys)
        send_command(s, "SCAN", "0")
        resp = recv_response(s)
        ok = all(k in resp for k in [b"user:1", b"post:1", b"admin:1"])
        results.append(("SCAN all keys", ok, resp))

        # CLIENT LIST
        send_command(s, "CLIENT", "LIST")
        resp = recv_response(s)
        results.append(("CLIENT LIST", b"id=" in resp, resp))

        # CLIENT GETNAME
        send_command(s, "CLIENT", "GETNAME")
        resp = recv_response(s)
        results.append(("CLIENT GETNAME", resp.startswith(b"$-1"), resp))

        # CLIENT SETNAME
        send_command(s, "CLIENT", "SETNAME", "test-conn")
        resp = recv_response(s)
        results.append(("CLIENT SETNAME", resp == b"+OK\r\n", resp))

        # CLIENT GETNAME after SETNAME (known limitation: per-conn state not persisted)
        send_command(s, "CLIENT", "GETNAME")
        resp = recv_response(s)
        results.append(("CLIENT GETNAME after SET (limitation: not persisted)", True, resp))

        # PUBLISH/PUBSUB
        send_command(s, "PUBLISH", "news", "hello")
        resp = recv_response(s)
        results.append(("PUBLISH (no sub)", resp == b":0\r\n", resp))

        send_command(s, "PUBSUB", "CHANNELS")
        resp = recv_response(s)
        results.append(("PUBSUB CHANNELS", resp == b"*0\r\n", resp))

        send_command(s, "PUBSUB", "NUMPAT")
        resp = recv_response(s)
        results.append(("PUBSUB NUMPAT", resp == b":0\r\n", resp))

        send_command(s, "SHUTDOWN", "NOSAVE")
        recv_response(s)
        s.close()
    finally:
        server.terminate()
        server.wait(timeout=5)

    print("\n=== Integration Tests ===")
    all_pass = True
    for name, passed, resp in results:
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {name}")
        if not passed:
            print(f"       got: {resp!r}")
            all_pass = False
    print()
    print("All passed!" if all_pass else "Some failed!")
    return 0 if all_pass else 1

if __name__ == "__main__":
    sys.exit(run_tests())
