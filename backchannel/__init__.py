"""
Backchannel — private, sub-millisecond IPC for AI agents

Each agent process runs a daemon that listens on a Unix domain socket.
Other agents send messages via transient PUSH connections — no persistent
connections, no broker, no open ports.

Usage:
  from backchannel import Bus, quick_send

  bus = Bus("analyst", peers=["analyst", "executor", "reviewer"])
  bus.start()

  quick_send("analyst", "executor", "Need a code review on PR #42")
"""

import json, os, time, uuid, threading, logging
from pathlib import Path
from collections import deque

import zmq

logger = logging.getLogger("backchannel")

SOCKET_DIR = Path(os.environ.get("BACKCHANNEL_DIR", "/tmp/backchannel/sockets"))
MAX_QUEUE = 1000


class Bus:
    """Inter-agent message bus (daemon mode: binds PULL).

    One daemon per agent identity.  Peers connect transiently to send.
    """

    def __init__(self, agent_name: str, peers: list[str] | None = None):
        self.name = agent_name
        self.peers = tuple(peers) if peers else ()
        self.ctx: zmq.Context | None = None
        self.pull: zmq.Socket | None = None
        self.pub: zmq.Socket | None = None
        self.sub: zmq.Socket | None = None
        self._recv_queue: deque = deque(maxlen=MAX_QUEUE)
        self._recv_thread: threading.Thread | None = None
        self._running = False
        self._lock = threading.Lock()

    def start(self, daemon_mode: bool = True):
        SOCKET_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.ctx = zmq.Context()

        if daemon_mode:
            pull_path = str(SOCKET_DIR / f"{self.name}.pull")
            if os.path.exists(pull_path):
                os.unlink(pull_path)
            self.pull = self.ctx.socket(zmq.PULL)
            self.pull.bind(f"ipc://{pull_path}")
            os.chmod(pull_path, 0o600)
            logger.info("[backchannel:%s] PULL bound %s", self.name, pull_path)

            pub_path = str(SOCKET_DIR / f"{self.name}.pub")
            if os.path.exists(pub_path):
                os.unlink(pub_path)
            self.pub = self.ctx.socket(zmq.PUB)
            self.pub.bind(f"ipc://{pub_path}")
            os.chmod(pub_path, 0o600)
            logger.info("[backchannel:%s] PUB bound %s", self.name, pub_path)

            self.sub = self.ctx.socket(zmq.SUB)
            for other in self.peers:
                if other == self.name:
                    continue
                sub_path = str(SOCKET_DIR / f"{other}.pub")
                self.sub.connect(f"ipc://{sub_path}")
            self.sub.setsockopt_string(zmq.SUBSCRIBE, self.name)
            self.sub.setsockopt_string(zmq.SUBSCRIBE, "all")
            logger.info("[backchannel:%s] SUB connected to peers", self.name)

            time.sleep(0.3)

        self._running = True
        if daemon_mode:
            self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
            self._recv_thread.start()

        logger.info("[backchannel:%s] bus started (IPC, portless)", self.name)

    def stop(self):
        self._running = False
        if self._recv_thread:
            self._recv_thread.join(timeout=2)
        for sock in [self.sub, self.pub, self.pull]:
            if sock:
                sock.close()
        if self.ctx:
            self.ctx.term()
        logger.info("[backchannel:%s] bus stopped", self.name)

    def _recv_loop(self):
        poller = zmq.Poller()
        if self.pull:
            poller.register(self.pull, zmq.POLLIN)
        if self.sub:
            poller.register(self.sub, zmq.POLLIN)

        while self._running:
            try:
                socks = dict(poller.poll(timeout=200))
                for sock in [self.pull, self.sub]:
                    if sock and sock in socks:
                        raw = sock.recv_multipart(zmq.NOBLOCK)
                        body = raw[-1].decode("utf-8")
                        try:
                            msg = json.loads(body)
                            self._recv_queue.append(msg)
                        except json.JSONDecodeError:
                            logger.debug("[backchannel:%s] bad json: %s",
                                         self.name, body[:100])
            except zmq.Again:
                continue
            except zmq.ZMQError as e:
                if self._running:
                    logger.debug("[backchannel:%s] recv error: %s", self.name, e)
            except Exception:
                logger.exception("[backchannel:%s] recv loop error", self.name)

    def send(self, to: str, content: str, msg_type: str = "task",
             reply_to: str | None = None) -> str:
        msg = {
            "from": self.name, "to": to, "type": msg_type,
            "content": content, "timestamp": time.time(),
            "msg_id": uuid.uuid4().hex[:12],
        }
        if reply_to:
            msg["reply_to"] = reply_to
        body = json.dumps(msg, ensure_ascii=False)

        if to == "all" and self.peers:
            if self.pub:
                self.pub.send_multipart([b"all", body.encode("utf-8")])
            else:
                for p in self.peers:
                    if p != self.name:
                        self._push_to(p, body)
        else:
            self._push_to(to, body)

        logger.debug("[backchannel:%s] -> %s: %s", self.name, to, content[:80])
        return msg["msg_id"]

    def _push_to(self, target: str, body: str):
        pull_path = str(SOCKET_DIR / f"{target}.pull")
        push = self.ctx.socket(zmq.PUSH)
        push.connect(f"ipc://{pull_path}")
        push.send_string(body)
        time.sleep(0.01)
        push.close()

    def receive(self, timeout: float = 0) -> dict | None:
        if timeout > 0:
            deadline = time.time() + timeout
            while time.time() < deadline:
                with self._lock:
                    if self._recv_queue:
                        return self._recv_queue.popleft()
                time.sleep(0.01)
            return None
        with self._lock:
            if self._recv_queue:
                return self._recv_queue.popleft()
        return None

    def receive_all(self) -> list[dict]:
        msgs = []
        with self._lock:
            while self._recv_queue:
                msgs.append(self._recv_queue.popleft())
        return msgs

    def broadcast(self, content: str, msg_type: str = "broadcast") -> str:
        return self.send("all", content, msg_type)


def quick_send(from_agent: str, to: str, content: str,
               msg_type: str = "task",
               peers: list[str] | None = None) -> str:
    msg = {
        "from": from_agent, "to": to, "type": msg_type,
        "content": content, "timestamp": time.time(),
        "msg_id": uuid.uuid4().hex[:12],
    }
    body = json.dumps(msg, ensure_ascii=False)

    ctx = zmq.Context()
    targets = list(peers) if to == "all" and peers else [to]
    for target in targets:
        if target == from_agent:
            continue
        pull_path = str(SOCKET_DIR / f"{target}.pull")
        push = ctx.socket(zmq.PUSH)
        push.connect(f"ipc://{pull_path}")
        push.send_string(body)
        push.close()
        time.sleep(0.005)
    ctx.term()

    logger.debug("[backchannel:quick] %s -> %s: %s", from_agent, to, content[:80])
    return msg["msg_id"]
