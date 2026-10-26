import { execFile } from "node:child_process";
import { accessSync, constants, statSync } from "node:fs";
import { homedir } from "node:os";
import { delimiter, isAbsolute, join } from "node:path";
import { type Clock, systemClock } from "./clock.ts";
import { type CodexSession, openCodexSession } from "./codex-session.ts";
import type { Env } from "./config.ts";
import type { PeerPush, Rejection } from "./peer-push.ts";

/** Native queue delivery for one explicit conversation on this Codex host. */
export type CodexTarget = { threadId: string; bin: string; socket: string };

const THREAD_ID = /^[a-zA-Z0-9-]{1,200}$/;

/** Only the explicitly identified thread on this host; never a recent rollout. */
export function codexTarget(env: Env): CodexTarget | undefined {
  const threadId = env.CODEX_THREAD_ID;
  if (!threadId || !THREAD_ID.test(threadId)) return;
  const bin = codexBin(env);
  const socket =
    env.TODOU_CODEX_SOCKET ||
    join(
      env.CODEX_HOME || join(env.HOME || homedir(), ".codex"),
      "app-server-control",
      "app-server-control.sock",
    );
  if (bin === undefined || !isAbsolute(socket)) return;
  try {
    if (!statSync(socket).isSocket()) return;
  } catch {
    return;
  }
  return { threadId, bin, socket };
}

/**
 * The `codex` executable `codex queue` runs from: `TODOU_CODEX_BIN`, or the
 * first one on PATH. Only absolute PATH entries count, so a relative entry
 * cannot make the working directory decide what runs.
 */
function codexBin(env: Env): string | undefined {
  const candidates = env.TODOU_CODEX_BIN
    ? [env.TODOU_CODEX_BIN]
    : (env.PATH ?? "")
        .split(delimiter)
        .filter((dir) => isAbsolute(dir))
        .map((dir) => join(dir, "codex"));
  for (const candidate of candidates) {
    if (!isAbsolute(candidate)) continue;
    try {
      if (!statSync(candidate).isFile()) continue;
      accessSync(candidate, constants.X_OK);
      return candidate;
    } catch {
      // Not there, or not executable: try the next entry.
    }
  }
  return undefined;
}

/** Collect continuously; submit one batch only when the subscribed thread is idle. */
export async function openCodex<T>(opts: {
  target: CodexTarget;
  render: (
    items: T[],
    since: string | undefined,
    cursor: string | undefined,
  ) => string;
  signal?: AbortSignal;
  run?: QueueRunner;
  clock?: Clock;
  debounceSec?: number;
  note?: (message: string) => void;
  openSession?: typeof openCodexSession;
}): Promise<PeerPush<T>> {
  if (opts.signal?.aborted) throw new Error("Codex connection cancelled");
  const clock = opts.clock ?? systemClock;
  type Batch = {
    items: T[];
    bytes: number;
    since: string | undefined;
    cursor: string | undefined;
    summaryOnly: boolean;
  };
  let held: Batch | undefined;
  let flight: Batch | undefined;
  let queuedId: string | undefined;
  let keys = new Set<string>();
  let due = 0;
  let wasBusy = false;
  let closed = false;
  let running = false;
  let again = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let session: CodexSession | undefined;
  let rejected: Rejection | null = null;
  let announce = () => {};
  const whenRejected = new Promise<void>((resolve) => {
    announce = resolve;
  });
  const fail = (reason: string) => {
    if (rejected || closed) return;
    rejected = { status: "unreachable", reason };
    clearTimeout(timer);
    announce();
  };
  const queue = openCodexQueuePush<T>({
    ...opts.target,
    run: opts.run,
    signal: opts.signal,
    render: (items, since, cursor) =>
      flight?.summaryOnly
        ? `Tracker batch too large; re-read all changes from since through cursor.\n${opts.render([], since, cursor)}`
        : opts.render(items, since, cursor),
    onQueued: (id) => {
      queuedId = id;
    },
  });
  void queue.whenRejected.then(() =>
    fail(queue.rejected?.reason ?? "Codex queue failed"),
  );

  function wake(delay = 0) {
    if (closed || rejected) return;
    if (running) {
      again = true;
      return;
    }
    clearTimeout(timer);
    timer = setTimeout(() => {
      void pump();
    }, delay);
    timer.unref();
  }

  async function pump() {
    if (!session || closed || rejected || running) return;
    running = true;
    again = false;
    try {
      const state = await session.refresh();
      if (closed || rejected || session.rejected) return;
      if (
        state.status !== "idle" ||
        state.queuedIds === null ||
        state.queuedIds.length > 0
      ) {
        wasBusy = true;
        return;
      }
      // A fresh, empty queue read observes consumption of our last receipt too.
      queuedId = undefined;
      if (!held) {
        wasBusy = false;
        return;
      }
      if (!wasBusy && clock.now() < due) {
        const delay = due - clock.now();
        timer = setTimeout(() => {
          void pump();
        }, delay);
        timer.unref();
        return;
      }
      flight = held;
      held = undefined;
      keys = new Set();
      wasBusy = false;
      await queue.send(flight.items, flight.since, flight.cursor);
      if (closed) return;
      if (queue.rejected) fail(queue.rejected.reason ?? "Codex queue failed");
      else {
        flight = undefined;
        // An admission receipt, not a stale idle snapshot, blocks the next batch.
        wasBusy = queuedId !== undefined;
      }
    } catch {
      fail("Codex batch delivery failed; inspect the queue before replaying");
    } finally {
      running = false;
      if (again && !closed && !rejected) wake();
    }
  }

  const abort = () =>
    fail(
      "Codex watch cancelled; inspect the queue before replaying unconfirmed events",
    );
  opts.signal?.addEventListener("abort", abort, { once: true });
  try {
    session = await (opts.openSession ?? openCodexSession)({
      socket: opts.target.socket,
      threadId: opts.target.threadId,
      signal: opts.signal,
      clock,
      note: opts.note,
      onChange: () => wake(),
    });
  } catch (error) {
    clearTimeout(timer);
    queue.close();
    opts.signal?.removeEventListener("abort", abort);
    throw error;
  }
  void session.whenRejected.then(() =>
    fail(session?.rejected?.reason ?? "Codex subscription failed"),
  );
  wake();
  return {
    async send(items, since, cursor) {
      if (items.length === 0) return;
      if (!held) {
        held = { items: [], bytes: 0, since, cursor, summaryOnly: false };
        due = clock.now() + (opts.debounceSec ?? 0) * 1000;
      }
      held.cursor = cursor;
      if (!held.summaryOnly) {
        for (const item of items) {
          const event = item as {
            id?: unknown;
            type?: unknown;
            issue_number?: unknown;
            project?: unknown;
          };
          const key =
            event && typeof event === "object" && event.id !== undefined
              ? JSON.stringify([
                  event.project,
                  event.issue_number,
                  event.type,
                  event.id,
                ])
              : JSON.stringify(item);
          if (!keys.has(key)) {
            keys.add(key);
            held.items.push(item);
            held.bytes += Buffer.byteLength(JSON.stringify(item) ?? "", "utf8");
          }
        }
        if (
          held.bytes > QUEUE_MESSAGE_MAX_BYTES ||
          Buffer.byteLength(
            opts.render(held.items, held.since, held.cursor),
            "utf8",
          ) > QUEUE_MESSAGE_MAX_BYTES
        ) {
          held.items = [];
          held.summaryOnly = true;
          keys.clear();
        }
      }
      const state = session?.snapshot();
      if (
        state?.status !== "idle" ||
        state.queuedIds?.length !== 0 ||
        queuedId !== undefined
      )
        wasBusy = true;
      wake();
    },
    get rejected() {
      return rejected;
    },
    whenRejected,
    unconfirmed: () => ({
      items: [...(flight?.items ?? []), ...(held?.items ?? [])],
      since: flight ? flight.since : held?.since,
      cursor: held?.cursor ?? flight?.cursor,
      summaryOnly: Boolean(flight?.summaryOnly || held?.summaryOnly),
    }),
    replies: () => ({ discarded: 0, unbounced: 0 }),
    close() {
      if (closed) return;
      closed = true;
      clearTimeout(timer);
      opts.signal?.removeEventListener("abort", abort);
      session?.close();
      queue.close();
    },
  };
}

/** Runs `codex queue` once; resolves with its stdout or rejects. */
export type QueueRunner = (
  bin: string,
  args: string[],
  signal?: AbortSignal,
) => Promise<string>;

const runQueue: QueueRunner = (bin, args, signal) =>
  new Promise((resolve, reject) => {
    // Queue admission can take seconds; stop a hung command after a minute.
    execFile(bin, args, { timeout: 60_000, signal }, (error, stdout) => {
      if (error) reject(error);
      else resolve(stdout);
    });
  });

/**
 * Linux caps one argv string at 128 KiB (MAX_ARG_STRLEN), and `codex queue`
 * takes the message only as an argument; a larger batch would fail to spawn
 * rather than arrive.
 */
const QUEUE_MESSAGE_MAX_BYTES = 96 * 1024;

/**
 * Files a batch in Codex's queue. The caller keeps the target subscribed
 * and checks its state before admission; a receipt confirms storage only.
 */
export function openCodexQueuePush<T>(opts: {
  threadId: string;
  bin: string;
  render: (
    items: T[],
    since: string | undefined,
    cursor: string | undefined,
  ) => string;
  run?: QueueRunner;
  signal?: AbortSignal;
  onQueued?: (id: string) => void;
}): PeerPush<T> {
  if (!THREAD_ID.test(opts.threadId) || !isAbsolute(opts.bin)) {
    throw new Error(
      "Codex requires an absolute executable and an explicit thread id",
    );
  }
  const run = opts.run ?? runQueue;
  let closed = false;
  let rejected: Rejection | null = null;
  let announce = () => {};
  const whenRejected = new Promise<void>((resolve) => {
    announce = resolve;
  });
  const reject = (reason: string) => {
    if (rejected) return;
    rejected = { status: "unreachable", reason };
    announce();
  };
  // Also wake a quiet SSE watch. Aborting the poll signal alone cannot wake
  // its wait; the socket transport used to supply this notification.
  const abort = () =>
    reject(
      "Codex watch cancelled; inspect the queue before replaying unconfirmed events",
    );
  opts.signal?.addEventListener("abort", abort, { once: true });
  if (opts.signal?.aborted) abort();
  let held: {
    items: T[];
    since: string | undefined;
    cursor: string | undefined;
  } = { items: [], since: undefined, cursor: undefined };

  return {
    async send(items, since, cursor) {
      held = {
        items: [...held.items, ...items],
        since: held.since ?? since,
        cursor,
      };
      if (rejected || closed) return;
      const body = opts.render(held.items, held.since, held.cursor);
      const message =
        Buffer.byteLength(body, "utf8") <= QUEUE_MESSAGE_MAX_BYTES
          ? body
          : [
              `This batch of ${held.items.length} ${
                held.items.length === 1 ? "entry" : "entries"
              } was too large to push — re-read it from the since cursor below`,
              opts.render([], held.since, held.cursor),
            ].join("\n");
      try {
        const stdout = await run(
          opts.bin,
          ["queue", "--thread", opts.threadId, "--message", message],
          opts.signal,
        );
        const receipt = stdout
          .split(/\r?\n/)
          .map((line) =>
            new RegExp(
              `^Queued message ([a-zA-Z0-9-]+) for thread ${opts.threadId}\\.$`,
            ).exec(line),
          )
          .find(Boolean);
        const id = receipt?.[1];
        if (!id) throw new Error("codex queue did not confirm the message");
        if (!rejected && !closed) {
          held = { items: [], since: undefined, cursor: undefined };
          opts.onQueued?.(id);
        }
      } catch (error) {
        // The message text is the batch itself, so the reason names the
        // failure alone; the batch goes back out on stdout at exit.
        const code = (error as { code?: unknown }).code;
        reject(
          code === undefined
            ? "codex queue did not confirm the message; inspect the queue before replaying"
            : `codex queue failed (${String(code)}); inspect the queue before replaying`,
        );
      }
    },
    get rejected() {
      return rejected;
    },
    unconfirmed: () => held,
    replies: () => ({ discarded: 0, unbounced: 0 }),
    whenRejected,
    close() {
      opts.signal?.removeEventListener("abort", abort);
      closed = true;
    },
  };
}
