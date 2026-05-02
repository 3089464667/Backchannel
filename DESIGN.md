# Backchannel — Design & Rationale

## The problem that started this

Late 2025.  I had three AI agents, three tmux windows, one server.  They needed to collaborate — one researches, one runs experiments, one writes.  They had no way to speak to each other.

I hacked together file-based messaging first.  Agent A drops a markdown file in a shared directory.  Agent B's cron picks it up 60 seconds later.  It worked.  But when an agent needs a decision and the other is waiting, 60 seconds feels like an hour.  The whole point of agents is autonomy — why are they bottlenecked on a cron job?

Then I tried TCP.  Then I tried Discord bots.  Then I realized I was solving the wrong problem.  I didn't need a messaging platform.  I needed the Unix equivalent of leaning over to the next desk and saying "hey, look at this."

## What Backchannel is

Backchannel is a transport layer.  It delivers bytes from process A to process B on the same machine.  It doesn't know what the bytes mean, doesn't care what LLM you're using, doesn't orchestrate workflows.

It's not CrewAI, AutoGen, or LangGraph.  Those handle task allocation, agent roles, and execution flow.  Backchannel sits underneath — it's the wire they could use instead of HTTP or in-process function calls.

Think of it as the TCP of agent communication.  TCP doesn't know about HTTP, SMTP, or SSH.  It delivers bytes reliably.  Backchannel doesn't know about prompts, tool calls, or reasoning chains.  It delivers messages between agent processes on the same machine, sub-millisecond, with session tracking.

## The transport: PUSH/PULL over Unix domain sockets

Why PUSH/PULL instead of request-reply?  Because agents don't always need an immediate answer.  An agent might fire off a progress update and keep working.  It might ask a question that takes the other agent 30 seconds of reasoning to answer.  Request-reply forces synchronous waiting.  PUSH/PULL is one-way — push and forget.  If you need a reply, start a session and wait for DATA frames.

Why Unix domain sockets instead of TCP?  Because if I'm binding a port — even on localhost — I've created an attack surface.  Unix sockets are files.  `chmod 0600` and only the same user can touch them.  No firewall rules, no port conflicts, no "is this port already in use" at 3 AM.

The key design decision: **the sender never binds a socket.**  It connects transiently, pushes the message, and disconnects.  Only the daemon binds.  This eliminates connection management entirely — no reconnection logic, no half-open sockets, no "who's connected to whom" state.

## Why there's a daemon

"Why not just have the agent process open a socket directly?"

Three reasons:

1. **Crash resilience.**  The agent process is an LLM — it can crash, OOM, or get stuck in an infinite loop.  Messages sent during downtime are queued by the daemon.  When the agent comes back, it polls: "what did I miss?"

2. **Separation of concerns.**  The agent shouldn't know about socket management.  It asks its daemon "any messages?" and gets a JSON response.  The daemon handles polling, queue management, and protocol handshakes.

3. **Systemd integration.**  A daemon means `systemctl enable backchanneld@analyst`.  Automatic restart on crash.  Logging to journald.  Standard lifecycle management.  No custom supervisor scripts.

## The session protocol: why it matters

Most agent communication tools are fire-and-forget.  You send a message and hope.  I wanted to know if the other side actually received it.

The protocol is a TCP-style handshake stripped to its essentials:

```
SYN     — "I want to start a session.  Here's the task."
SYN-ACK — "Got it.  Session established."
DATA    — bidirectional payloads
FIN     — "Session done."
FIN-ACK — "Acknowledged.  Cleaning up."
```

Why include the task in the SYN?  Because the receiver should know what this session is about before the first message arrives.  If an agent gets a SYN that says "security audit for auth module," it can prepare context, load relevant files, even reject the session if it's too busy.  The SYN is a negotiation, not a demand.

Why FIN-ACK instead of just dropping the connection?  Because state leaks.  If one side closes a session and the other doesn't know, it keeps polling, keeps a session object alive in memory, keeps waiting for messages that will never come.  FIN-ACK is a contract: "we both agree this is over."

### Edge cases we handle

- **Duplicate SYN:** If the same session_id arrives twice, ignore it.  Don't create a duplicate session.
- **Daemon crash during session:** Sessions are in-memory only.  When the daemon restarts, it starts fresh.  The agent must re-establish sessions.  This is deliberate — Backchannel is not a durable message queue.  If you need guaranteed delivery across restarts, you need a persistence layer.
- **SYN timeout:** 30 seconds.  If the target daemon doesn't respond, the session is abandoned.  No hanging connections.

## What I'd do differently

1. **Disk-backed queues.**  If the daemon restarts, pending messages are gone.  For most agent workflows this is fine — the agent reconnects and the sender retries.  But a SQLite-backed queue would make it more robust without adding infrastructure.

2. **Message compression.**  Agent messages can get long — full reasoning traces, code diffs, tool outputs.  Currently sent as plain JSON.  Compression would be trivial to add.

3. **Observability.**  Prometheus metrics for message counts, latency histograms, session duration.  Right now you're blind without `journalctl`.

4. **Multi-user isolation.**  Currently any process that can access the socket directory can send messages.  Fine for single-user machines.  Not fine for shared servers.  Unix socket credentials (SO_PEERCRED) could verify the sender's UID.

## Comparisons

### vs. MCP (Model Context Protocol)

MCP is a client-server protocol for LLMs to call tools.  It uses stdio or HTTP as transport.  Backchannel is a transport for agent-to-agent messages.  They solve different problems, but they could compose — MCP over Backchannel instead of MCP over stdio.

### vs. Google A2A

A2A is an application-layer protocol: how agents describe capabilities, negotiate tasks, return artifacts.  Backchannel is a transport layer.  A2A could use Backchannel as its wire protocol instead of HTTP, and everything above the transport would stay the same.

### vs. NATS / Redis

Both are excellent message brokers with features Backchannel doesn't have — persistence, clustering, auth, multi-tenancy.  They're also separate services you have to run.  Backchannel is a library plus a tiny daemon.  Trade infrastructure complexity for features.

## Why I stopped looking and shipped this

I spent two weeks evaluating options.  At some point I realized: three processes, one machine, need to send strings to each other.  This shouldn't require a research project.  Unix has had sockets since 1983.  The hard part isn't the technology — it's the session tracking, the daemon architecture, the protocol design.  Once those were right, the transport was the easy part.
