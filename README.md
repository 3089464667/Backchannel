# AgentIPC

**Sub-millisecond inter-agent communication over Unix domain sockets.  Zero open ports.**

I built this because my three AI agents (running as separate processes on one machine) needed to talk to each other.  The obvious options all sucked:

- **File polling** — one agent writes a file, the other's cron picks it up 60 seconds later.  Too slow.
- **HTTP** — now I'm binding ports, managing auth, parsing headers.  Too heavy.
- **Redis / NATS / RabbitMQ** — external dependency, more attack surface.

AgentIPC is none of those.  It's a thin layer over ZeroMQ PUSH/PULL sockets.  Each agent runs a tiny daemon that binds a Unix domain socket.  Sending a message is a one-shot PUSH connect, send, disconnect.  The receiving daemon polls and queues.  That's it.

## Quick start

```bash
pip install agentipc   # or: pip install -e .

# Tell it who your agents are
echo "analyst"  > /etc/agentipc/peers.conf
echo "executor" >> /etc/agentipc/peers.conf
echo "reviewer" >> /etc/agentipc/peers.conf

# Start daemons (one per agent process)
systemctl enable --now agentipc@analyst
systemctl enable --now agentipc@executor

# Send a message from anywhere
AGENTIPC_SENDER=analyst agentc executor "review PR #42, auth module"
```

Or without systemd, just run it directly:

```bash
python3 -m agentipc.daemon analyst &
python3 -m agentipc.daemon executor &

agentc executor "hello from analyst"
```

## What it gives you

| | File polling | HTTP | AgentIPC |
|---|---|---|---|
| Latency | 1-60 seconds | 5-20 ms | **<1 ms** |
| Open ports | 0 | 1+ | **0** |
| External deps | 0 | 0 | libzmq |
| Session tracking | no | yes (DIY) | **SYN/ACK/FIN** |
| Multi-agent broadcast | no | needs pubsub | **PUB/SUB built-in** |

## Architecture

```
┌──────────┐    PUSH (transient)    ┌──────────┐
│ analyst  │ ──────────────────────>│ executor │
│ daemon   │                        │ daemon   │
│          │                        │          │
│ PULL ◄───│──────────────┐         │ PULL ◄───│─── ...
│ PUB  ────│─────┐        │         │ PUB  ────│─── ...
│ REP  ◄───│──┐  │        │         │ REP  ◄───│──┐
└──────────┘  │  │        │         └──────────┘  │
              │  │        └── SUB ────────────────┘
              │  └─────────── SUB ────────────────┘
              └──── REP query: "any messages for me?"
```

- **PULL** — every agent daemon binds one.  Other agents PUSH to it.
- **PUB/SUB** — broadcast channel.  Used for protocol replies (SYN-ACK, FIN-ACK) so the sender doesn't have to keep a PULL bound.
- **REP** — the agent process queries its own daemon: "any pending messages?"  No polling files, no cron.

## The session protocol

AgentIPC has a TCP-style session handshake.  This matters because without it you can't tell if the other side actually received your message, or if you're talking into a void.

```
analyst                        executor
  │  ── SYN + task ──────────>  │
  │  <── SYN-ACK ─────────────  │
  │                              │
  │  ── DATA: "review PR #42" ─> │
  │  <── DATA: "found a bug" ──  │
  │                              │
  │  ── FIN: "done" ──────────> │
  │  <── FIN-ACK ──────────────  │
```

The daemon handles SYN/SYN-ACK/FIN/FIN-ACK automatically.

## Security

- Unix domain sockets with `0600` permissions.  Only root (or the same user) can connect.
- Zero TCP ports.  Nothing listens on the network.
- Messages are plain JSON.  If you need encryption you're probably doing cross-machine communication, and AgentIPC isn't for that.

The threat model is simple: you're running multiple agent processes on the same box and you trust them.  If you don't trust your own processes you have bigger problems than IPC.

## When NOT to use it

- **Cross-machine communication** — use HTTP, gRPC, or NATS.  AgentIPC is same-machine only.
- **Thousands of messages per second** — ZMQ can handle it, but this tool is built for agent collaboration (tens of messages per minute, not millions).
- **You need a message broker with persistence** — use RabbitMQ.  AgentIPC delivers or it doesn't; there's no disk-backed queue.

## Why ZeroMQ instead of raw Unix sockets?

I tried raw sockets first.  You end up rewriting message framing, dealing with partial reads, managing poll loops, and still getting it wrong.  ZMQ handles all of that.  `pip install pyzmq` and you're done.

## License

MIT
