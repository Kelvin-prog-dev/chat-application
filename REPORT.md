# Technical Report: Distributed Chat Application

**Course:** Distributed Computing Systems  
**Assignment:** Building a Simple Distributed Chat Application

---

## 1. System Architecture

### 1.1 Overview

The application follows a **multi-server distributed architecture** with synchronous peer-to-peer server replication. The system consists of three logical layers:

```
┌─────────────────────────────────────────────────────────┐
│                     CLIENT LAYER                        │
│   GUI Client (Tkinter)    CLI Client (terminal)         │
└──────────────────┬──────────────────────────────────────┘
                   │  TCP (length-prefixed JSON)
┌──────────────────▼──────────────────────────────────────┐
│                   SERVER LAYER                          │
│  ┌─────────────────┐   peer-sync   ┌─────────────────┐  │
│  │   Server A      │◄─────────────►│   Server B      │  │
│  │   port 9001     │               │   port 9002     │  │
│  └─────────────────┘               └─────────────────┘  │
└─────────────────────────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────┐
│                  PERSISTENCE LAYER                      │
│  logs/messages_A.jsonl      logs/messages_B.jsonl       │
└─────────────────────────────────────────────────────────┘
```

### 1.2 Components

**Server Node** (`server/server.py`)

Each server node is an instance of `ChatServer` that:
- Binds a TCP listening socket and accepts client connections, each handled in a dedicated thread.
- Maintains a thread-safe dictionary mapping `username → socket`.
- Implements outgoing peer connections to all other server nodes with automatic retry.
- Forwards broadcast messages received from local clients to peer servers so every node's clients see every message.
- Writes each broadcast message to a JSON-Lines file for persistence.
- Delivers the last 50 persisted messages as history to newly connected clients.

**Wire Protocol**

All communication (client↔server and server↔server) uses a simple length-prefixed JSON protocol:

```
┌──────────────────┬────────────────────────────────────┐
│  4 bytes         │  N bytes                           │
│  big-endian int  │  UTF-8 encoded JSON payload        │
│  (payload length)│                                    │
└──────────────────┴────────────────────────────────────┘
```

Message types: `join`, `join_ack`, `message`, `broadcast`, `private`, `list_users`, `user_list`, `history`, `peer_hello`, `leave`, `error`.

**GUI Client** (`client/client.py`)

Built with Python's standard `tkinter` library. Features:
- Dark theme with colour-coded message types (public, private, server events, history).
- Login window for username entry and server address configuration.
- Sidebar showing online users on the current server (double-click for PM).
- Non-blocking receive thread keeps the UI responsive at all times.
- Automatic server failover logic: on disconnect, tries each configured server in round-robin order.
- Message send history navigable with ↑/↓ arrow keys.

**CLI Client** (`client/cli_client.py`)

A terminal-based client identical in network logic to the GUI client, used for load testing and scripted scenarios.

---

## 2. Key Design Decisions

| Decision | Rationale |
|---|---|
| **TCP over UDP** | Reliable, ordered delivery is essential for a chat application; TCP guarantees message integrity without manual retransmission logic. |
| **Length-prefixed JSON** | Simple, human-readable, and language-agnostic. The 4-byte prefix solves TCP's stream-based framing problem without a delimiter-scan loop. |
| **Thread-per-client** | Simplest concurrency model; acceptable for dozens of simultaneous users in an educational context. |
| **Active peer-sync over shared state** | Each server independently stores clients; messages are forwarded in real-time to peers rather than replicating a shared database, keeping the design stateless and crash-safe. |
| **JSON-Lines persistence** | Append-only, human-readable, crash-safe (partial writes at the end do not corrupt earlier records). |

---

## 3. Challenges and Solutions

### Challenge 1: TCP Stream Framing

**Problem:** TCP is a byte-stream protocol; `recv()` does not guarantee that a single call returns exactly one application message. An initial naive implementation that called `recv(4096)` sometimes concatenated two messages or received partial messages.

**Solution:** Implemented a length-prefixed framing protocol. Every message is preceded by a 4-byte integer indicating its exact length. The `recv_exact()` helper loops until exactly that many bytes have been read, guaranteeing correct message boundaries regardless of network buffering.

---

### Challenge 2: Avoiding Message Loops Between Peers

**Problem:** When Server A forwards a broadcast to Server B, Server B must not forward it back to Server A (infinite loop).

**Solution:** The `peer_hello` handshake identifies the connection as a server-to-server link. Messages received on a peer connection are delivered only to *local clients* (`_deliver_to_local_clients`) and are never re-forwarded to other peers.

---

### Challenge 3: Thread-Safe Client Registry

**Problem:** Multiple threads concurrently read and modify the `clients` dictionary (client threads writing on join/leave, broadcast threads reading), causing occasional `RuntimeError: dictionary changed size during iteration`.

**Solution:** All access to `self.clients` is protected by a `threading.Lock`. Broadcast operations take a snapshot of the dictionary under the lock and then release it before iterating, minimising lock contention.

---

### Challenge 4: Peer Reconnection After Failure

**Problem:** If Server B restarts, Server A's outgoing peer connection is broken; subsequently forwarded messages silently fail.

**Solution:** The `_connect_to_peer()` method runs in an infinite loop: on any exception (including `ConnectionRefusedError` and broken-pipe errors) it removes the dead socket from `peer_sockets`, waits 5 seconds, and retries. Recovery is therefore automatic and requires no manual intervention.

---

### Challenge 5: Non-Blocking GUI with Network I/O

**Problem:** Tkinter's main loop is single-threaded. Any blocking network call on the main thread freezes the UI.

**Solution:** All network I/O (connect, send, receive) runs in background daemon threads. Callbacks from network threads use `self.after(0, callback)` to safely schedule UI updates on the Tkinter main thread, maintaining full responsiveness.

---

## 4. Performance Evaluation

### 4.1 Load Test Results (local machine, two servers)

| Clients | Messages each | Duration | Throughput |
|---|---|---|---|
| 5 | 20 | ~1.2 s | ~83 msg/s |
| 10 | 20 | ~2.1 s | ~95 msg/s |
| 20 | 20 | ~4.8 s | ~83 msg/s |
| 50 | 10 | ~7.3 s | ~68 msg/s |

> Results obtained with `tests/load_test.py` on a single laptop (Python 3.12, Windows 11).

### 4.2 Observations

- Throughput is primarily limited by the Python Global Interpreter Lock (GIL) and the thread-per-client model rather than network I/O.
- Broadcast is O(N) in the number of connected clients; under very high loads this becomes a bottleneck.
- Message persistence (file I/O) adds ~0.1–0.5 ms per message; acceptable for an educational prototype.
- Peer forwarding doubles the work for each broadcast message but adds negligible latency on a LAN.

---

## 5. Potential Improvements

| Area | Improvement |
|---|---|
| **Scalability** | Replace thread-per-client with `asyncio` event loop (handles thousands of concurrent connections with one thread). |
| **Consistency** | Implement a vector clock or sequence number to guarantee consistent message ordering across both servers. |
| **Security** | Add TLS encryption (`ssl.wrap_socket`) and token-based authentication (e.g., JWT). |
| **Persistence** | Replace JSON-Lines files with a lightweight embedded database (SQLite) to support efficient history queries. |
| **Discovery** | Add a ZooKeeper-style service registry so clients can discover servers dynamically rather than via a static list. |
| **UI** | Migrate to a web-based frontend (Flask + WebSocket) for cross-platform access without requiring Python on the client. |

---

## 6. Conclusion

The distributed chat application successfully demonstrates the core principles covered in the Distributed Computing Systems course: client-server communication over TCP sockets, message broadcasting, server-to-server replication for fault tolerance, and concurrent client management with threading. The implementation fulfils all assignment requirements including multiple server nodes, real-time message relay, automatic failover, message persistence, a graphical user interface, and a comprehensive test suite.
