"""
Connection-oriented sessions for inter-agent messaging.

Inspired by TCP's three-way handshake:

  Initiator                    Target
     |  ---- SYN ---->          |
     |  <-- SYN-ACK ---         |
     |                          |
     |  <-- DATA ---->          |   (bidirectional)
     |                          |
     |  ---- FIN ---->          |
     |  <-- FIN-ACK ---         |

State machine:
  IDLE -> SYN_SENT  -> CONNECTED -> FIN_SENT -> CLOSED
  IDLE -> SYN_RCVD  -> CONNECTED -> FIN_RCVD -> CLOSED
"""

import json, time, uuid, logging, threading
from enum import Enum
from collections import defaultdict
from typing import Optional, Callable

from backchannel import SOCKET_DIR
import zmq as _zmq

logger = logging.getLogger("backchannel.session")


class SessionState(Enum):
    IDLE = "idle"
    SYN_SENT = "syn_sent"
    SYN_RCVD = "syn_rcvd"
    CONNECTED = "connected"
    FIN_SENT = "fin_sent"
    FIN_RCVD = "fin_rcvd"
    CLOSED = "closed"


class Session:
    def __init__(self, session_id: str, initiator: str, target: str,
                 task: str = ""):
        self.session_id = session_id
        self.initiator = initiator
        self.target = target
        self.task = task
        self.state = SessionState.IDLE
        self.created_at = time.time()
        self.last_active = time.time()
        self.messages: list[dict] = []
        self._ack_event = threading.Event()

    def touch(self):
        self.last_active = time.time()

    def is_active(self) -> bool:
        return self.state == SessionState.CONNECTED

    def is_pending(self) -> bool:
        return self.state in (SessionState.SYN_SENT, SessionState.SYN_RCVD)

    def summary(self) -> str:
        return (f"Session({self.session_id[:8]} {self.initiator}<->{self.target} "
                f"{self.state.value} task={self.task[:40]})")


class SessionManager:
    def __init__(self, agent_name: str, bus):
        self.agent = agent_name
        self.bus = bus
        self.sessions: dict[str, Session] = {}
        self._handlers: dict[str, Callable] = {}
        self._running = False
        self._recv_thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def on_established(self, handler: Callable):
        self._handlers["established"] = handler

    def on_data(self, handler: Callable):
        self._handlers["data"] = handler

    def on_closed(self, handler: Callable):
        self._handlers["closed"] = handler

    def connect(self, target: str, task: str, timeout: float = 30) -> Optional[Session]:
        session_id = uuid.uuid4().hex[:12]
        session = Session(session_id, self.agent, target, task)
        session.state = SessionState.SYN_SENT

        with self._lock:
            self.sessions[session_id] = session

        tmp_ctx = _zmq.Context()
        tmp_sub = tmp_ctx.socket(_zmq.SUB)
        pub_path = f"ipc://{SOCKET_DIR}/{target}.pub"
        tmp_sub.connect(pub_path)
        tmp_sub.setsockopt_string(_zmq.SUBSCRIBE, self.agent)
        time.sleep(0.15)

        syn = json.dumps({
            "type": "SYN", "session_id": session_id,
            "from": self.agent, "to": target,
            "task": task, "timestamp": time.time(),
        }, ensure_ascii=False)
        self._push(target, syn)
        logger.info("[session] SYN %s -> %s: %s", self.agent, target, task[:50])

        deadline = time.time() + timeout
        poller = _zmq.Poller()
        poller.register(tmp_sub, _zmq.POLLIN)
        while time.time() < deadline:
            socks = dict(poller.poll(timeout=200))
            if tmp_sub in socks:
                raw = tmp_sub.recv_multipart(_zmq.NOBLOCK)
                if len(raw) >= 2:
                    try:
                        inner = json.loads(raw[-1].decode())
                        if (inner.get("type") == "SYN-ACK"
                                and inner.get("session_id") == session_id):
                            session.state = SessionState.CONNECTED
                            session.touch()
                            logger.info("[session] established: %s", session.summary())
                            handler = self._handlers.get("established")
                            if handler: handler(session)
                            tmp_sub.close()
                            tmp_ctx.term()
                            return session
                    except json.JSONDecodeError:
                        continue

        tmp_sub.close()
        tmp_ctx.term()
        session.state = SessionState.CLOSED
        logger.warning("[session] timeout: %s", session.summary())
        return None

    def _push(self, target: str, body: str):
        ctx = _zmq.Context()
        push = ctx.socket(_zmq.PUSH)
        pull_path = f"ipc://{SOCKET_DIR}/{target}.pull"
        push.connect(pull_path)
        push.send_string(body)
        time.sleep(0.01)
        push.close()
        ctx.term()

    def _pub(self, target: str, msg: dict):
        body = json.dumps(msg, ensure_ascii=False)
        if self.bus.pub:
            self.bus.pub.send_multipart([target.encode(), body.encode()])
        else:
            self._push(target, body)

    def accept(self, session: Session):
        session.state = SessionState.CONNECTED
        session.touch()
        self._pub(session.initiator, {
            "type": "SYN-ACK", "session_id": session.session_id,
            "from": self.agent, "to": session.initiator,
            "timestamp": time.time(),
        })
        logger.info("[session] accepted: %s", session.summary())
        handler = self._handlers.get("established")
        if handler: handler(session)

    def reject(self, session: Session, reason: str = ""):
        self._pub(session.initiator, {
            "type": "SYN-NACK", "session_id": session.session_id,
            "from": self.agent, "to": session.initiator,
            "reason": reason, "timestamp": time.time(),
        })
        session.state = SessionState.CLOSED
        logger.info("[session] rejected: %s reason=%s", session.summary(), reason)

    def send_data(self, session: Session, content: str):
        if session.state != SessionState.CONNECTED:
            raise RuntimeError(f"Session not connected: {session.state}")
        session.touch()
        to = (session.target if self.agent == session.initiator
              else session.initiator)
        data = {
            "type": "DATA", "session_id": session.session_id,
            "from": self.agent, "to": to,
            "content": content, "timestamp": time.time(),
        }
        self._push(to, json.dumps(data, ensure_ascii=False))
        session.messages.append({"role": self.agent, "content": content})
        logger.debug("[session] DATA %s -> %s: %s", self.agent, to, content[:60])

    def close(self, session: Session, reason: str = "task_complete"):
        if session.state != SessionState.CONNECTED:
            return
        session.state = SessionState.FIN_SENT
        session.touch()
        to = (session.target if self.agent == session.initiator
              else session.initiator)
        self._pub(to, {
            "type": "FIN", "session_id": session.session_id,
            "from": self.agent, "to": to,
            "reason": reason, "timestamp": time.time(),
        })
        logger.info("[session] FIN %s -> %s: %s", self.agent, to, reason)

    def _handle_syn(self, msg: dict):
        session_id = msg["session_id"]
        initiator = msg["from"]
        task = msg.get("task", "")
        with self._lock:
            if session_id in self.sessions:
                return
            session = Session(session_id, initiator, self.agent, task)
            session.state = SessionState.SYN_RCVD
            self.sessions[session_id] = session
        logger.info("[session] SYN_RCVD %s <- %s: %s", self.agent, initiator, task[:50])
        self.accept(session)

    def _handle_syn_ack(self, msg: dict):
        session_id = msg["session_id"]
        with self._lock:
            session = self.sessions.get(session_id)
        if session and session.state == SessionState.SYN_SENT:
            session.state = SessionState.CONNECTED
            session.touch()
            session._ack_event.set()

    def _handle_syn_nack(self, msg: dict):
        session_id = msg["session_id"]
        reason = msg.get("reason", "")
        with self._lock:
            session = self.sessions.get(session_id)
        if session:
            session.state = SessionState.CLOSED
            session._ack_event.set()
        logger.info("[session] rejected by peer: %s", reason)

    def _handle_data(self, msg: dict):
        session_id = msg["session_id"]
        content = msg.get("content", "")
        sender = msg["from"]
        with self._lock:
            session = self.sessions.get(session_id)
        if not session: return
        session.touch()
        session.messages.append({"role": sender, "content": content})
        logger.info("[session] DATA %s <- %s: %s", self.agent, sender, content[:80])
        handler = self._handlers.get("data")
        if handler: handler(session, content)

    def _handle_fin(self, msg: dict):
        session_id = msg["session_id"]
        reason = msg.get("reason", "unknown")
        with self._lock:
            session = self.sessions.get(session_id)
        if not session: return
        session.state = SessionState.FIN_RCVD
        self._pub(msg["from"], {
            "type": "FIN-ACK", "session_id": session_id,
            "from": self.agent, "to": msg["from"],
            "timestamp": time.time(),
        })
        session.state = SessionState.CLOSED
        logger.info("[session] CLOSED: %s reason=%s", session.summary(), reason)
        handler = self._handlers.get("closed")
        if handler: handler(session, reason)

    def _handle_fin_ack(self, msg: dict):
        session_id = msg["session_id"]
        with self._lock:
            session = self.sessions.get(session_id)
        if session and session.state == SessionState.FIN_SENT:
            session.state = SessionState.CLOSED
        logger.info("[session] CLOSED confirmed: %s", session_id[:8])

    def _process_message(self, raw_msg: dict):
        try:
            content = raw_msg.get("content", "")
            if not content: return
            msg = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return
        dispatch = {
            "SYN": self._handle_syn, "SYN-ACK": self._handle_syn_ack,
            "SYN-NACK": self._handle_syn_nack, "DATA": self._handle_data,
            "FIN": self._handle_fin, "FIN-ACK": self._handle_fin_ack,
        }
        handler = dispatch.get(msg.get("type", ""))
        if handler: handler(msg)

    def start(self):
        self._running = True
        self._recv_thread = threading.Thread(target=self._loop, daemon=True)
        self._recv_thread.start()
        logger.info("[session:%s] manager started", self.agent)

    def stop(self):
        self._running = False
        if self._recv_thread:
            self._recv_thread.join(timeout=3)
        logger.info("[session:%s] manager stopped", self.agent)

    def _loop(self):
        while self._running:
            msg = self.bus.receive(timeout=0.3)
            if msg:
                self._process_message(msg)

    def active_sessions(self) -> list[Session]:
        with self._lock:
            return [s for s in self.sessions.values() if s.is_active()]

    def status(self) -> dict:
        with self._lock:
            total = len(self.sessions)
            active = sum(1 for s in self.sessions.values() if s.is_active())
            pending = sum(1 for s in self.sessions.values() if s.is_pending())
            closed = sum(1 for s in self.sessions.values()
                         if s.state == SessionState.CLOSED)
        return {"agent": self.agent, "total": total,
                "active": active, "pending": pending, "closed": closed}
