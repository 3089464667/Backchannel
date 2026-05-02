# Backchannel

**Private, sub-millisecond communication between AI agents.  Zero open ports.  Nothing to configure.**

I built this because I had three agents running as separate processes on one server, and they couldn't talk to each other.  Here's what I tried and why each one failed:

| What I tried | Why it sucked |
|---|---|
| **File polling** (cron every 60s) | An agent asks a question, waits a full minute for the answer |
| **HTTP** | Now I'm managing ports, auth tokens, and serialization.  For same-machine messages. |
| **Redis pub/sub** | External dependency, more attack surface, overkill for 3 processes |
| **Discord bots** | My local agent needs the internet to talk to the agent next door? |

Backchannel is none of those.  It's a tiny daemon that binds a Unix domain socket.  Sending a message is a transient connection — push, send, disconnect.  No persistent connections, no broker, no open ports.

<p align="center">
  <img src="demo.gif" alt="Backchannel demo" width="700">
</p>

## In one minute

```bash
pip install backchannel

# Tell it who your agents are
echo "analyst"  > /etc/backchannel/peers.conf
echo "executor" >> /etc/backchannel/peers.conf

# Start daemons
backchanneld analyst &
backchanneld executor &

# Send a message — from any process, anywhere on the machine
BACKCHANNEL_SENDER=analyst bc executor "review PR #42, auth module"
```

That's it.  The executor gets it in under a millisecond.

## What you actually get

| | File polling | HTTP | Backchannel |
|---|---|---|---|
| Latency | 1–60 seconds | 5–20 ms | **< 1 ms** |
| Open ports | 0 | 1+ | **0** |
| External deps | 0 | 0 | none beyond pip |
| Session tracking | no | DIY | **SYN/ACK/FIN built in** |
| Know if delivered | no | yes (200) | **yes (SYN-ACK)** |

## The session thing

This is the part I actually care about.  Most agent communication is fire-and-forget — you send a string into the void and hope.  Backchannel has a TCP-style handshake:

```
analyst                          executor
  |  ---- SYN  + task --------->  |
  |  <--- SYN-ACK --------------  |
  |                                |
  |  ---- DATA: "review PR #42" -> |
  |  <--- DATA: "found a bug" ---  |
  |                                |
  |  ---- FIN: "done" -----------> |
  |  <--- FIN-ACK ---------------  |
```

You call `session = mgr.connect("executor", "code review")` and you actually know if they accepted.  You know the session is alive.  You know when it closes.  This isn't revolutionary — it's what TCP has done since 1974.  But nobody built it into agent communication before.

## Code

```python
from backchannel import Bus
from backchannel.session import SessionManager

bus = Bus("analyst", peers=["analyst", "executor"])
bus.start(daemon_mode=True)

mgr = SessionManager("analyst", bus)
mgr.on_data(lambda session, content: print(f"Got: {content}"))
mgr.start()

# Connect to another agent
session = mgr.connect("executor", "security audit for auth module")
if session:
    mgr.send_data(session, "starting review now")
    # ... work happens ...
    mgr.close(session, "audit complete")
```

## Architecture

```
┌──────────┐    transient push     ┌──────────┐
│ analyst  │ ────────────────────> │ executor │
│ daemon   │                       │ daemon   │
│          │                       │          │
│ PULL <───│───────── ...          │ PULL <───│─── ...
│ PUB  ────│─── ...                │ PUB  ────│─── ...
│ REP  <───│── query               │ REP  <───│── query
└──────────┘                       └──────────┘
```

Each daemon binds PULL to receive.  Senders transiently connect PUSH to send.  PUB/SUB carries protocol handshakes.  The agent polls its own daemon's REP socket: "any messages for me?"  No files, no cron, no polling the filesystem.

## Security model

Unix domain sockets with `0600` permissions.  No TCP ports.  Only processes running as the same user can connect.  Messages are plain JSON — if you need encryption you're probably doing cross-machine communication, and this isn't for that.

The threat model is simple: you're running agent processes on the same box and you trust them.  If you don't trust your own processes you have bigger problems than IPC.

## When *not* to use it

- **Cross-machine.**  Use HTTP, gRPC, or NATS.  This is same-machine only.
- **Thousands of messages per second.**  The library can handle it, but this was designed for agent collaboration — tens of messages a minute, not millions.
- **You need a durable message queue.**  Messages are held in memory.  Daemon restart = queue lost.  If you need persistence, layer it on top.

## Why not just use `multiprocessing.Queue`?

Because my agents aren't child processes.  They're independent systemd services with their own Python interpreters.  `multiprocessing` doesn't work across independent processes.

## Why Unix sockets instead of a message broker?

I tried Redis.  I tried NATS.  Both are great.  Both also mean another service to install, configure, secure, and monitor.  For three processes on one machine, that's infrastructure bloat.  Unix sockets are built into the kernel.  They've been there since 1983.  No daemon needed beyond the agent's own.

## License

MIT
