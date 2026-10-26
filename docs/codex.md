# Codex

todou identifies Codex writes with `CODEX_THREAD_ID` and uses the `codex`
credential profile, falling back to `harness`. `--follow=codex` delivers
tracker activity into the Codex conversation the watch was started from. No
plugin installation or replacement Codex launcher is required.

```sh
todou login https://todou.example --profile codex
todou agent can-i-follow
```

Run the second command from the Codex conversation so it inherits the thread
id. It names the channel this conversation has, or says there is none.

## Background delivery

`--follow=codex` keeps a subscription to the original conversation through its
App Server control socket and sends batches with **`codex queue`**. This keeps
the conversation loaded after the phone or desktop view disconnects. The watch,
Codex daemon, and host must remain running.

Use the same OS account and `CODEX_HOME` as the conversation. The watch finds
`codex` at an absolute `TODOU_CODEX_BIN` or the first executable on an absolute
PATH entry. It finds the socket at
`$CODEX_HOME/app-server-control/app-server-control.sock` (default home:
`$HOME/.codex`); `TODOU_CODEX_SOCKET` can select another absolute socket path.
The executable and socket must belong to the same Codex installation and home.

A compatible daemon socket is required, including when starting from a terminal.
`agent can-i-follow` checks local availability; starting the watch verifies the
subscription and state APIs. If these are unavailable, use `--poll`.

Start at least one turn so Codex has saved the conversation, and attach the watch
while that conversation is loaded. Codex 0.154.0 can change a cold-resumed
thread's sandbox to the daemon default. The watch therefore joins only an
already loaded thread, with no model, directory, approval, or sandbox overrides.
If the target has unloaded, open it in Codex before starting the watch.

## Follow tracker activity

```sh
todou issue watch T-16 --follow=codex
todou watch -p todou --follow=codex
```

Start the watch from the conversation with `exec_command` and leave the command
running. From a separate terminal or supervisor, set `CODEX_THREAD_ID` to the
original conversation's id and `CODEX_HOME` to the one Codex uses. The watch
keeps that id throughout its lifetime.

While the conversation is running or waiting for approval/input, changes collect
locally without adding queue messages. Once it is idle and its queue is empty,
all pending changes become one message. Changes arriving during that submission
form the next batch, which waits until the previous message has left the queue
and the conversation is idle again. Existing queued messages are left intact.

When already idle, the watch collects for 60 seconds by default; `--debounce 0`
removes that delay. Finishing a busy turn releases its pending batch without
another full debounce window. Codex checks admitted messages periodically
(about ten seconds in 0.154.0), so a queue receipt can precede the next turn.

The watch preserves distinct comments, spec reviews, and question activity with
the existing filters, including exclusion of the current session's own writes.
Avoid overlapping watches if duplicate notifications would be distracting.
An oversized batch becomes a `since`/`cursor` range with instructions to re-read
all its changes, keeping local memory and the queue argument bounded.

Status checks and queue admission are separate operations. If another client
starts a turn between them, one already-submitting message may wait in the
queue; later changes continue collecting locally. Use Codex directly for an
urgent stop or correction.

A pushed batch carries nothing but the activity. To add your own words before
or after it, set `follow_header` or `follow_footer` on the server's entry in the
CLI config; the README shows where.

## Sandboxes

The watch needs access to the tracker, the local daemon socket, and Codex's
queue storage. Sandbox restrictions can prevent any of these operations. A
watch that cannot initialize or confirm delivery stops and reports the reason.
Background command output alone does not wake Codex; with `--poll`, read the
output and resume from the printed cursor.

## Disconnects and recovery

A lost socket pauses admission while changes continue collecting. The watch
retries after 1, 2, and 5 seconds, then refreshes the original thread and queue
before sending. Requests time out after ten seconds. Protocol errors, a closed
or archived thread, exhausted retries, and an unconfirmed queue receipt stop
the watch. A daemon restart that leaves the thread unloaded requires opening
that thread before restarting the watch.

An admission receipt confirms storage. It does not confirm that a turn completed
or that every card was handled. Confirmed batches are not replayed after a
reconnect. When stopping, the watch prints unconfirmed and locally pending
changes with their `since`/`cursor` range; it does not enqueue them to drain its
buffer. For a summarized batch, re-read the full printed range.

A failed or cancelled queue command may already have stored its message. Inspect
the conversation and queue before replaying that range. After processing the
printed events, resume with `--since <cursor>`. Normal shutdown also prints the
latest cursor. SIGINT, SIGTERM, and SIGHUP flush pending output; SIGKILL or host
failure cannot preserve the in-memory buffer. Keep the starting cursor for
recovery; a new watch without `--since` starts at the current position.

The watch leaves approval and input requests for the user to answer in Codex.
Stopping the watch releases its subscription without interrupting the active turn.

## Compatibility and verification

The subscription, state APIs, and `codex queue` were verified with Codex CLI
**0.154.0**. Queue state uses the App Server's experimental API; later Codex
versions may change it.

`node scripts/codex-follow-smoke.mjs` uses a real daemon, local todou server,
and scripted model. It checks delivery with only the watcher subscribed,
continuous cursor ranges, zero admission while busy, a single combined batch
after completion, and pending output on shutdown. It needs no model login.

Add `--idle-unload-check` to wait until a separate, unsubscribed control thread
unloads before posting the first event to the watched thread (31-minute timeout).
`--mutate subscription` replaces subscription with a read and must fail that
check. `--mutate busy` reports an active thread as idle and must fail the
busy check; `--mutate queue` removes admission confirmation and must also fail.
`--codex-bin /path/to/codex` selects the tested executable; `--keep` retains logs.
