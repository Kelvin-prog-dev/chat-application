"""
============================================================
  Distributed Chat Application - Server Node
============================================================
  Each server node:
    - Accepts TCP client connections
    - Broadcasts messages to all locally connected clients
    - Forwards messages to peer servers (server-to-server sync)
    - Maintains a message log for fault tolerance / persistence
    - Handles client disconnections gracefully
============================================================
"""

import socket
import threading
import json
import logging
import os
import sys
import time
from datetime import datetime

# ─────────────────────────────────────────────────────────────
#  Logging Setup
# ─────────────────────────────────────────────────────────────
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)


def get_logger(server_id: str) -> logging.Logger:
    logger = logging.getLogger(f"server_{server_id}")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # File handler (per-server log file)
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"server_{server_id}.log"))
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


# ─────────────────────────────────────────────────────────────
#  Message Log (Persistence / Fault Tolerance)
# ─────────────────────────────────────────────────────────────
class MessageLog:
    """Appends every chat message to a local JSON-lines file."""

    def __init__(self, server_id: str):
        self.path = os.path.join(LOG_DIR, f"messages_{server_id}.jsonl")

    def append(self, msg: dict):
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(msg) + "\n")

    def read_all(self) -> list:
        if not os.path.exists(self.path):
            return []
        messages = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        messages.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return messages


# ─────────────────────────────────────────────────────────────
#  Chat Server
# ─────────────────────────────────────────────────────────────
class ChatServer:
    """
    A single server node in the distributed chat system.

    Parameters
    ----------
    server_id : str       Human-readable identifier (e.g. "A", "B")
    host      : str       IP address to bind on
    port      : int       Port for client connections
    peers     : list      List of (host, port) tuples for peer servers
    """

    def __init__(self, server_id: str, host: str, port: int, peers: list):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.peers = peers  # [(host, port), ...]

        self.logger = get_logger(server_id)
        self.msg_log = MessageLog(server_id)

        # Thread-safe client registry: {username: socket}
        self.clients: dict[str, socket.socket] = {}
        self.clients_lock = threading.Lock()

        # Peer sockets (outgoing connections to other servers)
        self.peer_sockets: list[socket.socket] = []
        self.peer_lock = threading.Lock()

        self._running = False

    # ── Startup ──────────────────────────────────────────────

    def start(self):
        self._running = True
        self.logger.info(f"Server {self.server_id} starting on {self.host}:{self.port}")

        # Connect to peer servers in background
        for peer_host, peer_port in self.peers:
            t = threading.Thread(
                target=self._connect_to_peer,
                args=(peer_host, peer_port),
                daemon=True,
            )
            t.start()

        # Accept clients
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.port))
            srv.listen(50)
            srv.settimeout(1.0)
            self.logger.info(f"Listening for clients on port {self.port}…")
            while self._running:
                try:
                    conn, addr = srv.accept()
                    self.logger.info(f"New connection from {addr}")
                    t = threading.Thread(
                        target=self._handle_client, args=(conn,), daemon=True
                    )
                    t.start()
                except socket.timeout:
                    continue
                except Exception as e:
                    if self._running:
                        self.logger.error(f"Accept error: {e}")

    def stop(self):
        self._running = False

    # ── Peer (server-to-server) connection ───────────────────

    def _connect_to_peer(self, peer_host: str, peer_port: int):
        """Attempt to connect to a peer server; retry on failure."""
        while self._running:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((peer_host, peer_port))
                self.logger.info(f"Connected to peer {peer_host}:{peer_port}")

                # Identify ourselves as a peer server
                self._send_msg(sock, {"type": "peer_hello", "server_id": self.server_id})

                with self.peer_lock:
                    self.peer_sockets.append(sock)

                # Listen for forwarded messages from the peer
                self._listen_to_peer(sock, peer_host, peer_port)
            except ConnectionRefusedError:
                self.logger.debug(
                    f"Peer {peer_host}:{peer_port} not reachable yet – retrying in 5 s"
                )
            except Exception as e:
                self.logger.warning(f"Peer connection error ({peer_host}:{peer_port}): {e}")

            # Remove socket and retry
            with self.peer_lock:
                if sock in self.peer_sockets:
                    self.peer_sockets.remove(sock)
            try:
                sock.close()
            except Exception:
                pass

            if self._running:
                time.sleep(5)

    def _listen_to_peer(self, sock: socket.socket, peer_host: str, peer_port: int):
        """Read messages forwarded by a peer and relay them to local clients."""
        while self._running:
            msg = self._recv_msg(sock)
            if msg is None:
                self.logger.warning(f"Peer {peer_host}:{peer_port} disconnected")
                break
            if msg.get("type") == "broadcast":
                # Don't re-forward to peers (avoid loops), only deliver locally
                self._deliver_to_local_clients(msg, exclude_username=None)

    # ── Client handling ───────────────────────────────────────

    def _handle_client(self, conn: socket.socket):
        """
        Handshake → register client → relay messages → cleanup.
        """
        username = None
        try:
            # Step 1: receive JOIN
            msg = self._recv_msg(conn)
            if msg is None or msg.get("type") != "join":
                conn.close()
                return

            username = msg.get("username", "").strip()
            if not username:
                self._send_msg(conn, {"type": "error", "text": "Username cannot be empty."})
                conn.close()
                return

            # Check for duplicate username on this server
            with self.clients_lock:
                if username in self.clients:
                    self._send_msg(
                        conn,
                        {"type": "error", "text": f"Username '{username}' is already taken."},
                    )
                    conn.close()
                    return
                self.clients[username] = conn

            self.logger.info(f"'{username}' joined (server {self.server_id})")

            # Acknowledge join
            self._send_msg(
                conn,
                {
                    "type": "join_ack",
                    "server_id": self.server_id,
                    "text": f"Welcome, {username}! You are connected to Server {self.server_id}.",
                },
            )

            # Deliver message history (last 50 messages)
            history = self.msg_log.read_all()[-50:]
            for h in history:
                self._send_msg(conn, {"type": "history", **h})

            # Notify everyone
            join_event = {
                "type": "broadcast",
                "username": "SERVER",
                "server_id": self.server_id,
                "text": f"*** {username} has joined the chat ***",
                "timestamp": self._ts(),
            }
            self._broadcast(join_event, exclude_username=username)

            # Step 2: message loop
            while self._running:
                msg = self._recv_msg(conn)
                if msg is None:
                    break  # client disconnected

                mtype = msg.get("type")

                if mtype == "message":
                    payload = {
                        "type": "broadcast",
                        "username": username,
                        "server_id": self.server_id,
                        "text": msg.get("text", ""),
                        "timestamp": self._ts(),
                    }
                    self.msg_log.append(payload)
                    self._broadcast(payload, exclude_username=None)
                    self._forward_to_peers(payload)

                elif mtype == "private":
                    target = msg.get("to", "")
                    payload = {
                        "type": "private",
                        "username": username,
                        "server_id": self.server_id,
                        "text": msg.get("text", ""),
                        "timestamp": self._ts(),
                    }
                    with self.clients_lock:
                        target_sock = self.clients.get(target)
                    if target_sock:
                        self._send_msg(target_sock, payload)
                        # Echo back to sender
                        self._send_msg(
                            conn,
                            {**payload, "text": f"[PM → {target}] {payload['text']}"},
                        )
                    else:
                        self._send_msg(
                            conn,
                            {
                                "type": "error",
                                "text": f"User '{target}' not found on this server.",
                            },
                        )

                elif mtype == "list_users":
                    with self.clients_lock:
                        users = list(self.clients.keys())
                    self._send_msg(conn, {"type": "user_list", "users": users})

                elif mtype == "leave":
                    break

        except Exception as e:
            self.logger.error(f"Error handling client '{username}': {e}")
        finally:
            if username:
                with self.clients_lock:
                    self.clients.pop(username, None)
                self.logger.info(f"'{username}' left (server {self.server_id})")
                leave_event = {
                    "type": "broadcast",
                    "username": "SERVER",
                    "server_id": self.server_id,
                    "text": f"*** {username} has left the chat ***",
                    "timestamp": self._ts(),
                }
                self._broadcast(leave_event, exclude_username=None)
                self._forward_to_peers(leave_event)
            try:
                conn.close()
            except Exception:
                pass

    # ── Broadcasting & Forwarding ─────────────────────────────

    def _broadcast(self, msg: dict, exclude_username=None):
        """Send msg to all locally connected clients."""
        self._deliver_to_local_clients(msg, exclude_username)

    def _deliver_to_local_clients(self, msg: dict, exclude_username):
        dead = []
        with self.clients_lock:
            snapshot = dict(self.clients)
        for uname, sock in snapshot.items():
            if uname == exclude_username:
                continue
            try:
                self._send_msg(sock, msg)
            except Exception:
                dead.append(uname)
        # Clean up dead connections
        if dead:
            with self.clients_lock:
                for uname in dead:
                    self.clients.pop(uname, None)

    def _forward_to_peers(self, msg: dict):
        """Forward a message to all peer servers."""
        dead = []
        with self.peer_lock:
            snapshot = list(self.peer_sockets)
        for sock in snapshot:
            try:
                self._send_msg(sock, msg)
            except Exception:
                dead.append(sock)
        if dead:
            with self.peer_lock:
                for s in dead:
                    if s in self.peer_sockets:
                        self.peer_sockets.remove(s)

    # ── Wire Protocol (length-prefixed JSON) ──────────────────

    @staticmethod
    def _send_msg(sock: socket.socket, msg: dict):
        data = json.dumps(msg).encode("utf-8")
        length = len(data)
        sock.sendall(length.to_bytes(4, "big") + data)

    @staticmethod
    def _recv_msg(sock: socket.socket):
        """Receive one length-prefixed JSON message; return None on disconnect."""
        try:
            raw_len = ChatServer._recv_exact(sock, 4)
            if raw_len is None:
                return None
            length = int.from_bytes(raw_len, "big")
            raw_data = ChatServer._recv_exact(sock, length)
            if raw_data is None:
                return None
            return json.loads(raw_data.decode("utf-8"))
        except Exception:
            return None

    @staticmethod
    def _recv_exact(sock: socket.socket, n: int):
        """Read exactly n bytes from socket."""
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    @staticmethod
    def _ts() -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    """
    Usage:
        python server.py <server_id> <host> <port> [peer_host:peer_port ...]

    Examples:
        python server.py A 127.0.0.1 9001 127.0.0.1:9002
        python server.py B 127.0.0.1 9002 127.0.0.1:9001
    """
    if len(sys.argv) < 4:
        print("Usage: python server.py <id> <host> <port> [peer_host:peer_port ...]")
        sys.exit(1)

    srv_id = sys.argv[1]
    srv_host = sys.argv[2]
    srv_port = int(sys.argv[3])

    peer_list = []
    for p in sys.argv[4:]:
        ph, pp = p.split(":")
        peer_list.append((ph, int(pp)))

    server = ChatServer(srv_id, srv_host, srv_port, peer_list)
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down…")
        server.stop()
