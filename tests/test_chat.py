"""
============================================================
  Distributed Chat Application - Automated Test Suite
============================================================
  Tests:
    1. Single-server connect & message echo
    2. Multi-client broadcast
    3. Private messaging
    4. User list
    5. Duplicate username rejection
    6. Server failover (simulated)
============================================================
"""

import socket
import threading
import json
import time
import sys
import os

# Add parent directory to path so we can import server
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from server.server import ChatServer

# ─────────────────────────────────────────────────────────────
#  Wire Protocol Helpers
# ─────────────────────────────────────────────────────────────
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


def recv_msg(sock, timeout=3):
    sock.settimeout(timeout)
    try:
        raw = recv_exact(sock, 4)
        if raw is None:
            return None
        length = int.from_bytes(raw, "big")
        data = recv_exact(sock, length)
        return json.loads(data) if data else None
    except socket.timeout:
        return None
    except Exception:
        return None
    finally:
        sock.settimeout(None)


def drain(sock, timeout=0.5):
    """Read and discard all pending messages."""
    msgs = []
    while True:
        m = recv_msg(sock, timeout=timeout)
        if m is None:
            break
        msgs.append(m)
    return msgs


def quick_connect(host, port, username):
    """Connect and complete the JOIN handshake. Returns socket."""
    s = socket.create_connection((host, port), timeout=5)
    send_msg(s, {"type": "join", "username": username})
    ack = recv_msg(s)
    assert ack and ack.get("type") == "join_ack", f"Expected join_ack, got {ack}"
    drain(s)  # discard history
    return s


# ─────────────────────────────────────────────────────────────
#  Test Fixture
# ─────────────────────────────────────────────────────────────
TEST_PORT_A = 19901
TEST_PORT_B = 19902
HOST = "127.0.0.1"

_servers = []
_server_threads = []


def start_test_servers():
    srv_a = ChatServer("T_A", HOST, TEST_PORT_A, [(HOST, TEST_PORT_B)])
    srv_b = ChatServer("T_B", HOST, TEST_PORT_B, [(HOST, TEST_PORT_A)])
    _servers.extend([srv_a, srv_b])
    for s in _servers:
        t = threading.Thread(target=s.start, daemon=True)
        t.start()
        _server_threads.append(t)
    time.sleep(0.5)  # Wait for servers to bind


def stop_test_servers():
    for s in _servers:
        s.stop()


# ─────────────────────────────────────────────────────────────
#  Individual Tests
# ─────────────────────────────────────────────────────────────
PASS = "[PASS]"
FAIL = "[FAIL]"


def test_connect():
    """TC-01: Client can connect and receive join_ack."""
    s = socket.create_connection((HOST, TEST_PORT_A), timeout=5)
    send_msg(s, {"type": "join", "username": "tc01_user"})
    ack = recv_msg(s)
    s.close()
    assert ack and ack.get("type") == "join_ack"
    return True


def test_broadcast():
    """TC-02: Message from one client is received by another."""
    alice = quick_connect(HOST, TEST_PORT_A, "tc02_alice")
    bob = quick_connect(HOST, TEST_PORT_A, "tc02_bob")
    drain(alice)  # alice gets bob's join notification

    send_msg(alice, {"type": "message", "text": "Hello Bob!"})
    # Bob should receive the broadcast
    msg = None
    for _ in range(10):
        m = recv_msg(bob, timeout=1)
        if m and m.get("type") == "broadcast" and "Hello Bob!" in m.get("text", ""):
            msg = m
            break
    alice.close()
    bob.close()
    assert msg is not None, "Bob did not receive Alice's message"
    return True


def test_private_message():
    """TC-03: Private message delivered only to target."""
    sender = quick_connect(HOST, TEST_PORT_A, "tc03_sender")
    receiver = quick_connect(HOST, TEST_PORT_A, "tc03_receiver")
    bystander = quick_connect(HOST, TEST_PORT_A, "tc03_bystander")
    drain(sender); drain(receiver); drain(bystander)

    send_msg(sender, {"type": "private", "to": "tc03_receiver", "text": "Secret!"})
    time.sleep(0.3)

    pm = None
    for _ in range(5):
        m = recv_msg(receiver, timeout=1)
        if m and m.get("type") == "private":
            pm = m
            break

    # Bystander should NOT receive it
    bs_msgs = drain(bystander, timeout=0.5)
    sender.close(); receiver.close(); bystander.close()

    assert pm is not None, "Receiver did not get private message"
    assert not any(m.get("type") == "private" for m in bs_msgs), \
        "Bystander received a private message it should not have"
    return True


def test_user_list():
    """TC-04: /users returns a list including connected clients."""
    alice = quick_connect(HOST, TEST_PORT_A, "tc04_alice")
    bob = quick_connect(HOST, TEST_PORT_A, "tc04_bob")
    drain(alice); drain(bob)

    send_msg(alice, {"type": "list_users"})
    ul = None
    for _ in range(5):
        m = recv_msg(alice, timeout=1)
        if m and m.get("type") == "user_list":
            ul = m
            break
    alice.close(); bob.close()
    assert ul is not None
    assert "tc04_alice" in ul["users"] and "tc04_bob" in ul["users"]
    return True


def test_duplicate_username():
    """TC-05: Second connection with same username is rejected."""
    s1 = socket.create_connection((HOST, TEST_PORT_A), timeout=5)
    send_msg(s1, {"type": "join", "username": "tc05_dup"})
    ack1 = recv_msg(s1)
    assert ack1 and ack1.get("type") == "join_ack"

    s2 = socket.create_connection((HOST, TEST_PORT_A), timeout=5)
    send_msg(s2, {"type": "join", "username": "tc05_dup"})
    ack2 = recv_msg(s2)
    s1.close(); s2.close()
    assert ack2 and ack2.get("type") == "error"
    return True


def test_failover():
    """TC-06: Client can reconnect to Server B when Server A stops."""
    # Connect to server A via raw socket
    s = socket.create_connection((HOST, TEST_PORT_A), timeout=5)
    send_msg(s, {"type": "join", "username": "tc06_failover"})
    ack = recv_msg(s)
    assert ack and ack.get("type") == "join_ack"
    drain(s)

    # Simulate Server A going down by stopping it
    _servers[0].stop()
    time.sleep(0.3)
    s.close()

    # Now try connecting to Server B (the backup)
    s2 = socket.create_connection((HOST, TEST_PORT_B), timeout=5)
    send_msg(s2, {"type": "join", "username": "tc06_failover_2"})
    ack2 = recv_msg(s2)
    s2.close()
    assert ack2 and ack2.get("type") == "join_ack", "Could not connect to backup server"
    return True


# ─────────────────────────────────────────────────────────────
#  Test Runner
# ─────────────────────────────────────────────────────────────
TESTS = [
    test_connect,
    test_broadcast,
    test_private_message,
    test_user_list,
    test_duplicate_username,
    test_failover,
]


def run_all():
    print("\n" + "=" * 52)
    print("  DistChat Test Suite")
    print("=" * 52)

    start_test_servers()
    passed = 0
    failed = 0

    for test_fn in TESTS:
        name = test_fn.__doc__ or test_fn.__name__
        try:
            test_fn()
            print(f"  {PASS}  {name}")
            passed += 1
        except Exception as e:
            print(f"  {FAIL}  {name}")
            print(f"       └─ {e}")
            failed += 1

    stop_test_servers()

    print("=" * 52)
    print(f"  Results: {passed} passed, {failed} failed")
    print("=" * 52 + "\n")
    return failed == 0


if __name__ == "__main__":
    ok = run_all()
    sys.exit(0 if ok else 1)
