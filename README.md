# DistChat — Distributed Chat Application

> **DS Group Assignment** · Building a Simple Distributed Chat Application  
> Submission: 3rd November 2026

---

## 📁 Project Structure

```
chat application/
├── server/
│   └── server.py          # Server node implementation
├── client/
│   ├── client.py          # GUI client (Tkinter)
│   └── cli_client.py      # CLI client (terminal)
├── tests/
│   ├── test_chat.py       # Automated test suite (6 test cases)
│   └── load_test.py       # Multi-client load/stress test
├── logs/                  # Auto-created: server logs + message persistence
├── start_server_A.bat     # Quick-launch Server A  (Windows)
├── start_server_B.bat     # Quick-launch Server B  (Windows)
├── start_client.bat       # Quick-launch GUI Client (Windows)
└── README.md
```

---

## ⚙️ Requirements

| Requirement | Version |
|---|---|
| Python | ≥ 3.11 |
| tkinter | Bundled with Python (no extra install needed) |

No third-party packages are required — the application uses only Python standard library modules (`socket`, `threading`, `json`, `tkinter`, `logging`).

---

## 🚀 Quick Start (Windows)

### Step 1 — Start both server nodes

Open **two separate terminals** (or double-click the `.bat` files):

**Terminal 1 — Server A**
```bat
start_server_A.bat
```
or manually:
```powershell
python server\server.py A 127.0.0.1 9001 127.0.0.1:9002
```

**Terminal 2 — Server B**
```bat
start_server_B.bat
```
or manually:
```powershell
python server\server.py B 127.0.0.1 9002 127.0.0.1:9001
```

> **Wait for the message** `Listening for clients on port …` before connecting clients.

---

### Step 2 — Connect clients

**GUI Client** (recommended):
```bat
start_client.bat
```
or:
```powershell
python client\client.py
```

A login window appears. Enter your username and click **Connect**.

**CLI Client** (for testing multiple users quickly):
```powershell
python client\cli_client.py Alice
python client\cli_client.py Bob
```

---

## 💬 Chat Commands

| Command | Description |
|---|---|
| *(type anything)* | Send a public message to all users |
| `/pm <username> <message>` | Send a private message to a specific user |
| `/users` | List users connected to the current server |
| `/clear` | Clear the chat window (GUI only) |
| `↑` / `↓` | Navigate message history (GUI only) |
| `/quit` | Disconnect (CLI only) |

> **GUI tip:** Double-click a username in the sidebar to open a private message dialog.

---

## 🔁 Server Failover (Fault Tolerance)

The system implements **automatic server failover**:

1. Each client holds a prioritised list of servers (Server A first, Server B second).
2. If the current server disconnects, the client automatically attempts to reconnect to the next server in the list, retrying every 2 seconds.
3. Servers also continuously retry peer-to-peer connections; if Server B restarts, Server A will reconnect automatically within ~5 seconds.

**To test failover:**
1. Start both servers and connect a client.
2. Close / kill the terminal running Server A.
3. The client will display `⚠ Disconnected … Attempting failover…` and reconnect to Server B automatically.

---

## 💾 Message Persistence

Every message broadcasted through a server is appended to a local **JSON-Lines log file**:
```
logs/messages_A.jsonl   # Server A's log
logs/messages_B.jsonl   # Server B's log
```

When a new client connects, the **last 50 messages** from the log are sent as history so the user can catch up on prior conversation.

---

## 🧪 Running Tests

**Automated test suite** (requires both test servers to start on ports 19901/19902):
```powershell
python tests\test_chat.py
```
Covers: connect, broadcast, private messaging, user list, duplicate username rejection, failover.

**Load/stress test** (servers must be running on ports 9001/9002):
```powershell
# 20 clients, 50 messages each
python tests\load_test.py 20 50
```
Reports connected clients, messages sent, errors, total duration, and throughput (msg/s).

---

## 🌐 Running on Multiple Machines (LAN)

To run servers on different machines on the same network:

**Machine 1 (IP: 192.168.1.10)**
```bash
python server/server.py A 0.0.0.0 9001 192.168.1.20:9002
```

**Machine 2 (IP: 192.168.1.20)**
```bash
python server/server.py B 0.0.0.0 9002 192.168.1.10:9001
```

**Clients** — enter both server IPs in the login window:
```
192.168.1.10:9001, 192.168.1.20:9002
```

---

## 🏗️ Architecture Overview

```
  Client A ──────────┐              ┌────────── Client C
  Client B ──────────┤  Server A ───┤ (peer sync) Server B ──── Client D
                     │  (port 9001) │  (port 9002)
                     └─────────────┘
```

- **Wire protocol:** Length-prefixed JSON over TCP — a 4-byte big-endian integer followed by a UTF-8 JSON payload.
- **Concurrency:** Each client connection runs in its own thread; server-to-server sync also runs in dedicated threads.
- **Message flow:** Client → Server A → local clients + forward to Server B → Server B's local clients.

---

## 📄 License

Educational use — DS Group Assignment 2026.
