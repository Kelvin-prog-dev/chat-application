"""
============================================================
  Distributed Chat Application - Load Test Script
============================================================
  Spawns N concurrent CLI clients that connect and send
  messages at a configurable rate. Useful for Step 6
  (Testing under load) in the assignment.

  Usage:
      python load_test.py [num_clients] [messages_each]

  Example:
      python load_test.py 20 50
============================================================
"""

import threading
import time
import sys
import socket
import json
import random
import string


def send_msg(sock, msg):
    data = json.dumps(msg).encode("utf-8")
    sock.sendall(len(data).to_bytes(4, "big") + data)


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def recv_msg(sock):
    try:
        raw = recv_exact(sock, 4)
        if raw is None:
            return None
        length = int.from_bytes(raw, "big")
        data = recv_exact(sock, length)
        return json.loads(data) if data else None
    except Exception:
        return None


SERVERS = [("127.0.0.1", 9001), ("127.0.0.1", 9002)]
results_lock = threading.Lock()
results = {"sent": 0, "errors": 0, "connected": 0}


def bot_worker(bot_id: int, num_messages: int):
    username = f"Bot_{bot_id:03d}"
    server = SERVERS[bot_id % len(SERVERS)]
    try:
        sock = socket.create_connection(server, timeout=5)
        send_msg(sock, {"type": "join", "username": username})
        ack = recv_msg(sock)
        if not ack or ack.get("type") != "join_ack":
            with results_lock:
                results["errors"] += 1
            return

        with results_lock:
            results["connected"] += 1

        # Drain any history messages first (non-blocking)
        sock.settimeout(0.5)
        try:
            while True:
                m = recv_msg(sock)
                if m is None:
                    break
        except Exception:
            pass
        sock.settimeout(None)

        # Send messages
        for i in range(num_messages):
            msg_text = "".join(random.choices(string.ascii_letters + " ", k=random.randint(10, 60)))
            try:
                send_msg(sock, {"type": "message", "text": f"[{username}] {msg_text}"})
                with results_lock:
                    results["sent"] += 1
            except Exception:
                with results_lock:
                    results["errors"] += 1
            time.sleep(random.uniform(0.05, 0.2))

        send_msg(sock, {"type": "leave"})
        sock.close()
    except Exception as e:
        with results_lock:
            results["errors"] += 1


def run_load_test(num_clients: int, messages_each: int):
    print(f"\n{'='*50}")
    print(f"  Load Test: {num_clients} clients × {messages_each} messages")
    print(f"{'='*50}\n")

    threads = []
    start = time.time()

    for i in range(num_clients):
        t = threading.Thread(target=bot_worker, args=(i, messages_each), daemon=True)
        threads.append(t)
        t.start()
        time.sleep(0.05)  # slight stagger to avoid thundering herd

    for t in threads:
        t.join(timeout=60)

    elapsed = time.time() - start
    print(f"\n{'='*50}")
    print(f"  Connected  : {results['connected']}/{num_clients}")
    print(f"  Messages   : {results['sent']} sent")
    print(f"  Errors     : {results['errors']}")
    print(f"  Duration   : {elapsed:.2f}s")
    print(f"  Throughput : {results['sent']/elapsed:.1f} msg/s")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    n_clients = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    n_messages = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    run_load_test(n_clients, n_messages)
