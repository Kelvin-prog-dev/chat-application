"""
============================================================
  Distributed Chat Application - GUI Client
============================================================
  Connects to one of the server nodes (with automatic
  failover to a backup server if the primary goes down).

  Features:
    - Tkinter-based graphical interface
    - Automatic server failover
    - Private messaging  (/pm <user> <message>)
    - User list command  (/users)
    - Threaded receive loop (non-blocking UI)
============================================================
"""

import socket
import threading
import json
import tkinter as tk
from tkinter import scrolledtext, messagebox, ttk, simpledialog
import sys
import time
from datetime import datetime


# ─────────────────────────────────────────────────────────────
#  Wire Protocol Helpers (shared with server)
# ─────────────────────────────────────────────────────────────
def send_msg(sock: socket.socket, msg: dict):
    data = json.dumps(msg).encode("utf-8")
    sock.sendall(len(data).to_bytes(4, "big") + data)


def recv_msg(sock: socket.socket):
    try:
        raw_len = recv_exact(sock, 4)
        if raw_len is None:
            return None
        length = int.from_bytes(raw_len, "big")
        raw_data = recv_exact(sock, length)
        if raw_data is None:
            return None
        return json.loads(raw_data.decode("utf-8"))
    except Exception:
        return None


def recv_exact(sock: socket.socket, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ─────────────────────────────────────────────────────────────
#  ChatClient (network layer)
# ─────────────────────────────────────────────────────────────
class ChatClient:
    """
    Manages the TCP connection to a server node.
    Supports automatic failover to backup servers.
    """

    def __init__(self, servers: list, username: str, on_message, on_status):
        """
        servers    : list of (host, port) – tried in order
        username   : display name
        on_message : callback(msg_dict) called on each inbound message
        on_status  : callback(text) for status updates
        """
        self.servers = servers
        self.username = username
        self.on_message = on_message
        self.on_status = on_status

        self.sock: socket.socket | None = None
        self.current_server_idx = 0
        self._running = False
        self._connected = False

    # ── Connection ────────────────────────────────────────────

    def connect(self):
        """Try each server in order until one succeeds."""
        for i, (host, port) in enumerate(self.servers):
            try:
                self.on_status(f"Connecting to Server {chr(65+i)} ({host}:{port})…")
                s = socket.create_connection((host, port), timeout=5)
                self.sock = s
                self.current_server_idx = i

                # Handshake
                send_msg(s, {"type": "join", "username": self.username})
                ack = recv_msg(s)
                if ack is None:
                    raise ConnectionError("No handshake response.")
                if ack.get("type") == "error":
                    raise ConnectionError(ack.get("text", "Server error"))

                self._connected = True
                self._running = True
                self.on_status(
                    f"✓ Connected to Server {chr(65+self.current_server_idx)} ({host}:{port})"
                )
                return ack
            except Exception as e:
                self.on_status(f"✗ Could not reach {host}:{port} – {e}")
                if self.sock:
                    try:
                        self.sock.close()
                    except Exception:
                        pass
                    self.sock = None

        raise ConnectionError("All servers are unreachable.")

    def start_receiving(self):
        """Start the background receive thread."""
        t = threading.Thread(target=self._recv_loop, daemon=True)
        t.start()

    def _recv_loop(self):
        """Receive messages in background; attempt failover on disconnect."""
        while self._running:
            msg = recv_msg(self.sock)
            if msg is None:
                if not self._running:
                    break
                self.on_status("⚠ Disconnected from server. Attempting failover…")
                self._connected = False
                if not self._failover():
                    self.on_status("✗ All servers unreachable. Connection lost.")
                    self._running = False
                break
            self.on_message(msg)

    def _failover(self) -> bool:
        """Try the next server(s) in the list."""
        tried = 0
        total = len(self.servers)
        while tried < total:
            self.current_server_idx = (self.current_server_idx + 1) % total
            host, port = self.servers[self.current_server_idx]
            try:
                self.on_status(
                    f"Failing over to Server {chr(65+self.current_server_idx)} ({host}:{port})…"
                )
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
                    self._connected = True
                    self.on_status(
                        f"✓ Reconnected to Server {chr(65+self.current_server_idx)} ({host}:{port})"
                    )
                    # Restart receive loop
                    t = threading.Thread(target=self._recv_loop, daemon=True)
                    t.start()
                    return True
            except Exception as e:
                self.on_status(f"✗ Failover to {host}:{port} failed – {e}")
            tried += 1
            time.sleep(2)
        return False

    # ── Sending ───────────────────────────────────────────────

    def send_chat(self, text: str):
        if not self._connected or not self.sock:
            raise ConnectionError("Not connected.")
        send_msg(self.sock, {"type": "message", "text": text})

    def send_private(self, to: str, text: str):
        if not self._connected or not self.sock:
            raise ConnectionError("Not connected.")
        send_msg(self.sock, {"type": "private", "to": to, "text": text})

    def request_user_list(self):
        if self._connected and self.sock:
            send_msg(self.sock, {"type": "list_users"})

    def disconnect(self):
        self._running = False
        self._connected = False
        if self.sock:
            try:
                send_msg(self.sock, {"type": "leave"})
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────
#  GUI
# ─────────────────────────────────────────────────────────────
class ChatApp(tk.Tk):
    """Main application window."""

    COLORS = {
        "bg": "#1e1e2e",
        "sidebar": "#181825",
        "input_bg": "#313244",
        "text": "#cdd6f4",
        "dim": "#585b70",
        "accent": "#89b4fa",
        "success": "#a6e3a1",
        "warning": "#f9e2af",
        "error": "#f38ba8",
        "server_msg": "#fab387",
        "private": "#cba6f7",
        "history": "#6c7086",
        "timestamp": "#45475a",
    }

    def __init__(self, servers: list, username: str):
        super().__init__()
        self.servers = servers
        self.username = username

        self.title(f"DistChat – {username}")
        self.geometry("900x620")
        self.minsize(700, 480)
        self.configure(bg=self.COLORS["bg"])
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_ui()
        self._client = ChatClient(
            servers, username, self._on_message, self._on_status
        )
        self._connect()

    # ── UI Construction ───────────────────────────────────────

    def _build_ui(self):
        c = self.COLORS

        # ── Top bar ──
        top = tk.Frame(self, bg=c["sidebar"], height=48)
        top.pack(fill="x")
        top.pack_propagate(False)

        tk.Label(
            top,
            text=" 💬 DistChat",
            bg=c["sidebar"],
            fg=c["accent"],
            font=("Segoe UI", 14, "bold"),
        ).pack(side="left", padx=14, pady=10)

        self.status_var = tk.StringVar(value="Connecting…")
        tk.Label(
            top,
            textvariable=self.status_var,
            bg=c["sidebar"],
            fg=c["dim"],
            font=("Segoe UI", 9),
        ).pack(side="right", padx=14)

        # ── Main content ──
        content = tk.Frame(self, bg=c["bg"])
        content.pack(fill="both", expand=True)

        # ── Sidebar ──
        sidebar = tk.Frame(content, bg=c["sidebar"], width=180)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)

        tk.Label(
            sidebar,
            text="ONLINE USERS",
            bg=c["sidebar"],
            fg=c["dim"],
            font=("Segoe UI", 8, "bold"),
        ).pack(anchor="w", padx=12, pady=(14, 4))

        self.user_list_var = tk.StringVar()
        self.user_listbox = tk.Listbox(
            sidebar,
            listvariable=self.user_list_var,
            bg=c["sidebar"],
            fg=c["text"],
            selectbackground=c["input_bg"],
            selectforeground=c["accent"],
            font=("Segoe UI", 10),
            relief="flat",
            bd=0,
            highlightthickness=0,
            activestyle="none",
        )
        self.user_listbox.pack(fill="both", expand=True, padx=8, pady=4)
        self.user_listbox.bind("<Double-Button-1>", self._start_pm)

        refresh_btn = tk.Button(
            sidebar,
            text="⟳ Refresh",
            bg=c["input_bg"],
            fg=c["text"],
            activebackground=c["accent"],
            activeforeground=c["bg"],
            font=("Segoe UI", 9),
            relief="flat",
            bd=0,
            pady=6,
            cursor="hand2",
            command=self._refresh_users,
        )
        refresh_btn.pack(fill="x", padx=8, pady=8)

        # ── Chat area ──
        chat_frame = tk.Frame(content, bg=c["bg"])
        chat_frame.pack(side="left", fill="both", expand=True)

        self.chat_box = scrolledtext.ScrolledText(
            chat_frame,
            bg=c["bg"],
            fg=c["text"],
            font=("Consolas", 11),
            relief="flat",
            bd=0,
            wrap="word",
            state="disabled",
            padx=14,
            pady=10,
        )
        self.chat_box.pack(fill="both", expand=True, padx=(0, 4), pady=(4, 0))

        # Configure text tags for coloring
        self.chat_box.tag_config("timestamp", foreground=c["timestamp"])
        self.chat_box.tag_config("username", foreground=c["accent"], font=("Consolas", 11, "bold"))
        self.chat_box.tag_config("server_msg", foreground=c["server_msg"], font=("Consolas", 10, "italic"))
        self.chat_box.tag_config("error_msg", foreground=c["error"])
        self.chat_box.tag_config("private_msg", foreground=c["private"])
        self.chat_box.tag_config("history_msg", foreground=c["history"])
        self.chat_box.tag_config("own_msg", foreground=c["success"])
        self.chat_box.tag_config("status_msg", foreground=c["dim"], font=("Consolas", 9, "italic"))

        # ── Input bar ──
        input_frame = tk.Frame(chat_frame, bg=c["input_bg"], pady=8)
        input_frame.pack(fill="x", padx=4, pady=8)

        self.msg_entry = tk.Entry(
            input_frame,
            bg=c["input_bg"],
            fg=c["text"],
            insertbackground=c["text"],
            font=("Segoe UI", 12),
            relief="flat",
            bd=0,
        )
        self.msg_entry.pack(side="left", fill="x", expand=True, padx=(12, 6), ipady=6)
        self.msg_entry.bind("<Return>", self._send_message)
        self.msg_entry.bind("<Up>", self._history_up)
        self.msg_entry.bind("<Down>", self._history_down)

        send_btn = tk.Button(
            input_frame,
            text="Send ➤",
            bg=c["accent"],
            fg=c["bg"],
            activebackground=c["success"],
            activeforeground=c["bg"],
            font=("Segoe UI", 10, "bold"),
            relief="flat",
            bd=0,
            padx=16,
            pady=6,
            cursor="hand2",
            command=self._send_message,
        )
        send_btn.pack(side="right", padx=(0, 10))

        # Help label
        tk.Label(
            chat_frame,
            text='/pm <user> <msg>  •  /users  •  /clear  •  Double-click user for private message',
            bg=c["bg"],
            fg=c["dim"],
            font=("Segoe UI", 8),
        ).pack(anchor="w", padx=14, pady=(0, 4))

        # Message history (↑↓ navigation)
        self._msg_history: list[str] = []
        self._history_idx = -1

    # ── Networking ────────────────────────────────────────────

    def _connect(self):
        def do_connect():
            try:
                ack = self._client.connect()
                self._client.start_receiving()
                # Show welcome message from server
                if ack:
                    self._append_line(
                        f"[SERVER] {ack.get('text', '')}", "server_msg"
                    )
            except Exception as e:
                messagebox.showerror("Connection Failed", str(e))
                self.destroy()

        t = threading.Thread(target=do_connect, daemon=True)
        t.start()

    # ── Message callbacks (called from recv thread) ───────────

    def _on_message(self, msg: dict):
        """Dispatch inbound server message to the UI (thread-safe)."""
        self.after(0, self._process_message, msg)

    def _on_status(self, text: str):
        self.after(0, self.status_var.set, text)

    def _process_message(self, msg: dict):
        mtype = msg.get("type")
        ts = msg.get("timestamp", "")

        if mtype == "broadcast":
            uname = msg.get("username", "")
            text = msg.get("text", "")
            if uname == "SERVER":
                self._append_line(f"{text}", "server_msg")
            elif uname == self.username:
                self._append_chat(ts, f"[You]", text, "own_msg")
            else:
                self._append_chat(ts, uname, text, "username")

        elif mtype == "history":
            uname = msg.get("username", "")
            text = msg.get("text", "")
            ts2 = msg.get("timestamp", "")
            if uname == "SERVER":
                self._append_line(f"  {text}", "history_msg")
            else:
                self._append_chat(ts2, uname, text, "history_msg")

        elif mtype == "private":
            uname = msg.get("username", "")
            text = msg.get("text", "")
            self._append_chat(ts, f"🔒 {uname}", text, "private_msg")

        elif mtype == "error":
            self._append_line(f"[ERROR] {msg.get('text')}", "error_msg")

        elif mtype == "user_list":
            users = msg.get("users", [])
            self.user_list_var.set(users)

    # ── Sending ───────────────────────────────────────────────

    def _send_message(self, event=None):
        text = self.msg_entry.get().strip()
        if not text:
            return
        self.msg_entry.delete(0, "end")

        # Save to history
        self._msg_history.append(text)
        self._history_idx = len(self._msg_history)

        try:
            if text.startswith("/pm "):
                # /pm <username> <message>
                parts = text[4:].split(" ", 1)
                if len(parts) < 2:
                    self._append_line("Usage: /pm <username> <message>", "error_msg")
                    return
                self._client.send_private(parts[0], parts[1])

            elif text == "/users":
                self._client.request_user_list()

            elif text == "/clear":
                self.chat_box.configure(state="normal")
                self.chat_box.delete("1.0", "end")
                self.chat_box.configure(state="disabled")

            elif text.startswith("/"):
                self._append_line(
                    f"Unknown command: {text}  (try /pm, /users, /clear)", "error_msg"
                )

            else:
                self._client.send_chat(text)

        except ConnectionError as e:
            self._append_line(f"[ERROR] {e}", "error_msg")

    def _refresh_users(self):
        self._client.request_user_list()

    def _start_pm(self, event=None):
        """Double-click on a user in the sidebar to open a PM dialog."""
        sel = self.user_listbox.curselection()
        if not sel:
            return
        target = self.user_listbox.get(sel[0])
        if target == self.username:
            return
        msg = simpledialog.askstring(
            "Private Message",
            f"Send a private message to {target}:",
            parent=self,
        )
        if msg:
            try:
                self._client.send_private(target, msg)
            except ConnectionError as e:
                self._append_line(f"[ERROR] {e}", "error_msg")

    # ── Chat display helpers ──────────────────────────────────

    def _append_chat(self, timestamp: str, username: str, text: str, name_tag: str):
        self.chat_box.configure(state="normal")
        self.chat_box.insert("end", f"[{timestamp}] ", "timestamp")
        self.chat_box.insert("end", f"{username}: ", name_tag)
        self.chat_box.insert("end", f"{text}\n")
        self.chat_box.configure(state="disabled")
        self.chat_box.see("end")

    def _append_line(self, line: str, tag: str = ""):
        self.chat_box.configure(state="normal")
        if tag:
            self.chat_box.insert("end", line + "\n", tag)
        else:
            self.chat_box.insert("end", line + "\n")
        self.chat_box.configure(state="disabled")
        self.chat_box.see("end")

    # ── Keyboard message history ──────────────────────────────

    def _history_up(self, event=None):
        if not self._msg_history:
            return
        self._history_idx = max(0, self._history_idx - 1)
        self.msg_entry.delete(0, "end")
        self.msg_entry.insert(0, self._msg_history[self._history_idx])

    def _history_down(self, event=None):
        if not self._msg_history:
            return
        self._history_idx = min(len(self._msg_history), self._history_idx + 1)
        self.msg_entry.delete(0, "end")
        if self._history_idx < len(self._msg_history):
            self.msg_entry.insert(0, self._msg_history[self._history_idx])

    # ── Close ─────────────────────────────────────────────────

    def _on_close(self):
        self._client.disconnect()
        self.destroy()


# ─────────────────────────────────────────────────────────────
#  Login Window
# ─────────────────────────────────────────────────────────────
class LoginWindow(tk.Tk):
    """Splash / login screen shown before the main chat window."""

    COLORS = {
        "bg": "#1e1e2e",
        "panel": "#181825",
        "input_bg": "#313244",
        "text": "#cdd6f4",
        "dim": "#585b70",
        "accent": "#89b4fa",
        "label": "#a6adc8",
    }

    def __init__(self, default_servers: list):
        super().__init__()
        self.default_servers = default_servers
        self.result = None

        self.title("DistChat – Connect")
        self.geometry("460x400")
        self.resizable(False, False)
        self.configure(bg=self.COLORS["bg"])
        self._build_ui()

    def _build_ui(self):
        c = self.COLORS

        tk.Label(
            self,
            text="💬 DistChat",
            bg=c["bg"],
            fg=c["accent"],
            font=("Segoe UI", 22, "bold"),
        ).pack(pady=(30, 4))

        tk.Label(
            self,
            text="Distributed Chat Application",
            bg=c["bg"],
            fg=c["dim"],
            font=("Segoe UI", 10),
        ).pack()

        panel = tk.Frame(self, bg=c["panel"], padx=24, pady=20)
        panel.pack(fill="both", expand=True, padx=30, pady=20)

        def field(label, default="", show=""):
            tk.Label(panel, text=label, bg=c["panel"], fg=c["label"],
                     font=("Segoe UI", 10)).pack(anchor="w", pady=(8, 2))
            e = tk.Entry(panel, bg=c["input_bg"], fg=c["text"],
                         insertbackground=c["text"], font=("Segoe UI", 11),
                         relief="flat", bd=0, show=show)
            e.insert(0, default)
            e.pack(fill="x", ipady=6, pady=(0, 4))
            return e

        self.username_entry = field("Username", "")

        tk.Label(panel, text="Servers (host:port, comma-separated)",
                 bg=c["panel"], fg=c["label"],
                 font=("Segoe UI", 10)).pack(anchor="w", pady=(8, 2))
        self.server_entry = tk.Entry(
            panel, bg=c["input_bg"], fg=c["text"],
            insertbackground=c["text"], font=("Segoe UI", 11),
            relief="flat", bd=0,
        )
        default_srv = ", ".join(f"{h}:{p}" for h, p in self.default_servers)
        self.server_entry.insert(0, default_srv)
        self.server_entry.pack(fill="x", ipady=6)

        tk.Button(
            panel,
            text="Connect",
            bg=c["accent"],
            fg=c["bg"],
            activebackground="#b4d0ff",
            activeforeground=c["bg"],
            font=("Segoe UI", 11, "bold"),
            relief="flat",
            bd=0,
            pady=8,
            cursor="hand2",
            command=self._submit,
        ).pack(fill="x", pady=(16, 0))

        self.username_entry.bind("<Return>", lambda e: self._submit())

    def _submit(self):
        username = self.username_entry.get().strip()
        if not username:
            messagebox.showwarning("Input Required", "Please enter a username.", parent=self)
            return

        servers = []
        for part in self.server_entry.get().split(","):
            part = part.strip()
            if part:
                try:
                    h, p = part.rsplit(":", 1)
                    servers.append((h.strip(), int(p.strip())))
                except ValueError:
                    messagebox.showerror(
                        "Invalid Server", f"'{part}' is not a valid host:port.", parent=self
                    )
                    return

        if not servers:
            messagebox.showwarning("No Servers", "Enter at least one server.", parent=self)
            return

        self.result = (username, servers)
        self.destroy()

    def show(self):
        self.mainloop()
        return self.result


# ─────────────────────────────────────────────────────────────
#  Entry Point
# ─────────────────────────────────────────────────────────────
DEFAULT_SERVERS = [
    ("127.0.0.1", 9001),  # Server A
    ("127.0.0.1", 9002),  # Server B
]

if __name__ == "__main__":
    login = LoginWindow(DEFAULT_SERVERS)
    result = login.show()

    if result:
        username, servers = result
        app = ChatApp(servers, username)
        app.mainloop()
