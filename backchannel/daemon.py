#!/usr/bin/env python3
"""
backchanneld — daemon process for Backchannel

One daemon per agent identity, managed by systemd.

Capabilities:
  1. Binds PULL — receives messages from other agents
  2. Binds PUB  — broadcasts protocol replies (SYN-ACK, FIN-ACK)
  3. REP socket — allows the agent process to query for pending messages
  4. Session tracking — counts active sessions, accumulates message queues

The agent polls the REP socket for pending messages.
No filesystem polling, no cron, no external services.

Usage:
  backchanneld <agent-name>

Configuration:
  BACKCHANNEL_PEERS  env var or /etc/backchannel/peers.conf
"""

import sys, os, json, time, signal, logging
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from backchannel import Bus, SOCKET_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [backchanneld:%(name)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("backchanneld")

import zmq

TZ = timezone(timedelta(hours=8))


def load_peers() -> list[str]:
    if "BACKCHANNEL_PEERS" in os.environ:
        return os.environ["BACKCHANNEL_PEERS"].split(",")
    conf = Path("/etc/backchannel/peers.conf")
    if conf.exists():
        return [line.strip() for line in conf.read_text().splitlines()
                if line.strip() and not line.strip().startswith("#")]
    return []


active_sessions: dict[str, dict] = {}
pending_messages: dict[str, list[dict]] = defaultdict(list)


def handle_syn(bus: Bus, msg: dict):
    session_id = msg["session_id"]
    sender = msg["from"]
    task = msg.get("task", msg.get("content", ""))
    if session_id not in active_sessions:
        active_sessions[session_id] = {
            "session_id": session_id, "peer": sender,
            "task": task[:200], "created_at": time.time(), "msg_count": 0,
        }
    ack = {"type": "SYN-ACK", "session_id": session_id,
           "from": bus.name, "to": sender, "timestamp": time.time()}
    bus.pub.send_multipart([sender.encode(), json.dumps(ack).encode()])
    logger.info("session established: %s <-> %s task=%s", bus.name, sender, task[:50])


def handle_data(bus: Bus, msg: dict):
    session_id = msg.get("session_id", "")
    sender = msg.get("from", "?")
    if session_id in active_sessions:
        active_sessions[session_id]["msg_count"] += 1
        active_sessions[session_id]["last_msg_at"] = time.time()
    pending_messages[session_id].append({
        "from": sender, "content": msg.get("content", ""),
        "timestamp": msg.get("timestamp", time.time()),
        "msg_id": msg.get("msg_id", ""),
    })
    logger.info("%s <- %s [%s]: %s", bus.name, sender, session_id[:8],
                msg.get("content", "")[:80])


def handle_fin(bus: Bus, msg: dict):
    session_id = msg["session_id"]
    sender = msg["from"]
    reason = msg.get("reason", "unknown")
    fin_ack = {"type": "FIN-ACK", "session_id": session_id,
               "from": bus.name, "to": sender, "timestamp": time.time()}
    bus.pub.send_multipart([sender.encode(), json.dumps(fin_ack).encode()])
    if session_id in active_sessions:
        info = active_sessions.pop(session_id)
        logger.info("session closed: %s <-> %s msgs=%d reason=%s",
                    bus.name, sender, info["msg_count"], reason)
    pending_messages.pop(session_id, None)


def handle_rep_query(rep_socket, raw_parts, daemon_name: str):
    request = ""
    try:
        if raw_parts:
            request = raw_parts[-1].decode("utf-8")
    except (UnicodeDecodeError, IndexError):
        pass
    parts = request.strip().split(":", 1)
    cmd = parts[0].upper()
    arg = parts[1] if len(parts) > 1 else ""

    if cmd in ("QUERY", "STATUS"):
        sessions_list = [{
            "session_id": sid, "peer": info["peer"],
            "task": info["task"][:100], "msg_count": info["msg_count"],
            "created_at": info["created_at"],
        } for sid, info in active_sessions.items()]
        pending_counts = {sid: len(msgs) for sid, msgs in pending_messages.items()}
        rep_socket.send_string(json.dumps({
            "status": "ok", "agent": daemon_name,
            "active_sessions": sessions_list,
            "pending_message_counts": pending_counts,
            "total_pending": sum(pending_counts.values()),
            "timestamp": time.time(),
        }, ensure_ascii=False))
    elif cmd == "ACK":
        sid = arg
        if sid in pending_messages:
            cleared = len(pending_messages[sid])
            pending_messages.pop(sid, None)
            rep_socket.send_string(json.dumps({"status": "ok", "cleared": cleared}))
            logger.info("agent ACK: %s cleared %d messages", sid[:8], cleared)
        else:
            rep_socket.send_string(json.dumps({
                "status": "ok", "cleared": 0, "note": "no pending messages"}))
    elif cmd == "PING":
        rep_socket.send_string(json.dumps({
            "status": "pong", "agent": daemon_name,
            "active_sessions": len(active_sessions),
            "pending_messages": sum(len(v) for v in pending_messages.values()),
        }))
    else:
        rep_socket.send_string(json.dumps({
            "status": "error",
            "message": f"Unknown command: {cmd}. Use QUERY, ACK:<sid>, or PING",
        }))


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <agent-name>", file=sys.stderr)
        sys.exit(1)

    agent_name = sys.argv[1]
    peers = load_peers()
    logger.info("backchanneld starting — agent: %s, peers: %s", agent_name, peers)

    bus = Bus(agent_name, peers=peers)
    bus.start(daemon_mode=True)

    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    rep_path = str(SOCKET_DIR / f"{agent_name}.rep")
    if os.path.exists(rep_path):
        os.unlink(rep_path)
    rep.bind(f"ipc://{rep_path}")
    os.chmod(rep_path, 0o600)
    logger.info("REP query interface: %s", rep_path)

    running = True

    def shutdown(sig, frame):
        nonlocal running
        logger.info("shutting down (%d active, %d pending)...",
                    len(active_sessions),
                    sum(len(v) for v in pending_messages.values()))
        running = False

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    handlers = {"SYN": handle_syn, "DATA": handle_data, "FIN": handle_fin,
                "connect": handle_syn, "message": handle_data, "disconnect": handle_fin}
    logger.info("backchanneld ready — agent: %s", agent_name)

    rep_poller = zmq.Poller()
    rep_poller.register(rep, zmq.POLLIN)

    while running:
        socks = dict(rep_poller.poll(timeout=200))
        if rep in socks:
            raw = rep.recv_multipart(zmq.NOBLOCK)
            handle_rep_query(rep, raw, agent_name)

        msg = bus.receive(timeout=0)
        if msg:
            handled = False
            msg_type = msg.get("type", "")
            if msg_type in handlers:
                handlers[msg_type](bus, msg)
                handled = True
            if not handled:
                content = msg.get("content", "")
                try:
                    inner = json.loads(content)
                    inner_type = inner.get("type", "")
                    if inner_type in handlers:
                        handlers[inner_type](bus, inner)
                        handled = True
                except (json.JSONDecodeError, TypeError):
                    pass
            if not handled:
                sender = msg.get("from", "?")
                logger.info("%s <- %s: %s", agent_name, sender,
                            msg.get("content", "")[:80])

    for sid, info in list(active_sessions.items()):
        bus.send(info["peer"], json.dumps({
            "type": "FIN", "session_id": sid,
            "from": agent_name, "to": info["peer"],
            "reason": "daemon_shutdown", "timestamp": time.time(),
        }, ensure_ascii=False), msg_type="fin")
    active_sessions.clear()
    pending_messages.clear()
    rep.close()
    ctx.term()
    bus.stop()
    logger.info("daemon stopped")


if __name__ == "__main__":
    main()
