"""
============================================================
  Distributed Chat Application - CLI Client
============================================================
  A lightweight command-line client — useful for testing
  multiple simultaneous connections without extra windows.

  Usage:
      python cli_client.py <username> [host:port [host:port ...]]

  Commands while connected:
      /pm <user> <msg>   – Private message
      /users             – List users on current server
      /quit              – Disconnect and exit
============================================================
"""

import socket
import threading
import json
import sys
import time


# ─────────────────────────────────────────────────────────────
#  Wire Protocol (mirrors server.py)
# ─────────────────────────────────────────────────────────────
def send_msg(sock, msg: dict):
    data = json.dumps(msg).encode("utf-8")
    sock.sendall(len(data).to_bytes(4, "big") + data)


def recv_msg(sock):
    try:
        raw_len = recv_exact(sock, 4)
        if raw_len is None:
            return None
        length = int.from_bytes(raw_len, "big")
        raw = recv_exact(sock, length)
        return json.loads(raw.decode("utf-8")) if raw else None
    except Exception:
        return None


def recv_exact(sock, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ─────────────────────────────────────────────────────────────
#  CLI Client
# ─────────────────────────────────────────────────────────────
class CLIChatClient:
    def __init__(self, servers: list, username: str):
        self.servers = servers
        self.username = username
        self.sock = None
        self.current_idx = 0
        self._running = False

    def connect(self) -> bool:
        for i, (host, port) in enumerate(self.servers):
            try:
                print(f"[*] Connecting to {host}:{port}…")
                s = socket.create_connection((host, port), timeout=5)
                send_msg(s, {"type": "join", "username": self.username})
                ack = recv_msg(s)
                if ack and ack.get("type") == "join_ack":
                    self.sock = s
                    self.current_idx = i
                    print(f"[✓] {ack.get('text', 'Connected.')}")
                    return True
                elif ack and ack.get("type") == "error":
                    print(f"[✗] {ack.get('text')}")
                    s.close()
            except Exception as e:
                print(f"[✗] {host}:{port} – {e}")
        return False

    def _recv_loop(self):
        while self._running:
            msg = recv_msg(self.sock)
            if msg is None:
                if self._running:
                    print("\n[!] Disconnected. Attempting failover…")
                    if not self._failover():
                        print("[✗] All servers unreachable. Exiting.")
                        self._running = False
                break
            self._display(msg)

    def _failover(self) -> bool:
        tried = 0
        total = len(self.servers)
        while tried < total:
            self.current_idx = (self.current_idx + 1) % total
            host, port = self.servers[self.current_idx]
            try:
                print(f"[*] Trying {host}:{port}…")
                s = socket.create_connection((host, port), timeout=5)
                send_msg(s, {"type": "join", "username": self.username})
                ack = recv_msg(s)
                if ack and ack.get("type") == "join_ack":
                    old = self.sock
                    self.sock = s
                    try:
                        old.close()
                    except Exception:
                        pass
                    print(f"[✓] Reconnected to {host}:{port}")
                    t = threading.Thread(target=self._recv_loop, daemon=True)
                    t.start()
                    return True
            except Exception as e:
                print(f"[✗] {host}:{port} – {e}")
            tried += 1
            time.sleep(2)
        return False

    def _display(self, msg: dict):
        mtype = msg.get("type")
        ts = msg.get("timestamp", "")
        if mtype == "broadcast":
            uname = msg.get("username", "")
            text = msg.get("text", "")
            if uname == "SERVER":
                print(f"\n  *** {text} ***")
            elif uname == self.username:
                print(f"\n  [{ts}] [You]: {text}")
            else:
                print(f"\n  [{ts}] {uname}: {text}")
        elif mtype == "history":
            uname = msg.get("username", "")
            text = msg.get("text", "")
            ts2 = msg.get("timestamp", "")
            if uname != "SERVER":
                print(f"  [HISTORY] [{ts2}] {uname}: {text}")
        elif mtype == "private":
            uname = msg.get("username", "")
            text = msg.get("text", "")
            print(f"\n  [PRIVATE] [{ts}] {uname}: {text}")
        elif mtype == "error":
            print(f"\n  [ERROR] {msg.get('text')}")
        elif mtype == "user_list":
            users = msg.get("users", [])
            print(f"\n  Users online: {', '.join(users)}")
        print("  > ", end="", flush=True)

    def run(self):
        if not self.connect():
            print("[✗] Could not connect to any server.")
            return

        self._running = True
        t = threading.Thread(target=self._recv_loop, daemon=True)
        t.start()

        print("Commands: /pm <user> <msg>  /users  /quit")
        try:
            while self._running:
                line = input("  > ").strip()
                if not line:
                    continue
                if line == "/quit":
                    break
                elif line.startswith("/pm "):
                    parts = line[4:].split(" ", 1)
                    if len(parts) == 2:
                        send_msg(self.sock, {"type": "private", "to": parts[0], "text": parts[1]})
                    else:
                        print("  Usage: /pm <user> <message>")
                elif line == "/users":
                    send_msg(self.sock, {"type": "list_users"})
                elif line.startswith("/"):
                    print(f"  Unknown command: {line}")
                else:
                    try:
                        send_msg(self.sock, {"type": "message", "text": line})
                    except Exception as e:
                        print(f"  [ERROR] {e}")
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            self._running = False
            if self.sock:
                try:
                    send_msg(self.sock, {"type": "leave"})
                except Exception:
                    pass
                try:
                    self.sock.close()
                except Exception:
                    pass
            print("\n[*] Disconnected.")


# ─────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python cli_client.py <username> [host:port ...]")
        sys.exit(1)

    uname = sys.argv[1]
    srvs = [("127.0.0.1", 9001), ("127.0.0.1", 9002)]  # defaults

    if len(sys.argv) > 2:
        srvs = []
        for arg in sys.argv[2:]:
            h, p = arg.rsplit(":", 1)
            srvs.append((h, int(p)))

    CLIChatClient(srvs, uname).run()
