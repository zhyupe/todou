import { connect } from "node:net";
import WebSocket from "ws";
import { type Clock, systemClock } from "./clock.ts";
import type { Rejection } from "./peer-push.ts";

export type CodexSnapshot = {
  status: string;
  queuedIds: readonly string[] | null;
};

export type CodexSession = {
  snapshot(): CodexSnapshot;
  refresh(): Promise<CodexSnapshot>;
  readonly rejected: Rejection | null;
  readonly whenRejected: Promise<void>;
  close(): void;
};

type Pending = {
  resolve: (result: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
};

class Disconnected extends Error {}

/** Keeps the original daemon session subscribed without changing its settings. */
export async function openCodexSession(opts: {
  socket: string;
  threadId: string;
  signal?: AbortSignal;
  clock?: Clock;
  note?: (message: string) => void;
  onChange: () => void;
}): Promise<CodexSession> {
  const clock = opts.clock ?? systemClock;
  const stop = new AbortController();
  let ws: WebSocket | undefined;
  let closed = false;
  let ready = false;
  let recovering = false;
  let revision = 0;
  let nextId = 0;
  let state: CodexSnapshot = { status: "unknown", queuedIds: null };
  let rejected: Rejection | null = null;
  let announce = () => {};
  const whenRejected = new Promise<void>((resolve) => {
    announce = resolve;
  });
  const pending = new Map<number, Pending>();
  let refreshing: Promise<CodexSnapshot> | undefined;

  function clearRequests(error: Error) {
    for (const entry of pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    pending.clear();
  }

  function invalidate() {
    revision++;
    state = { status: "unknown", queuedIds: null };
    opts.onChange();
  }

  function close() {
    if (closed) return;
    closed = true;
    ready = false;
    stop.abort();
    clearInterval(poll);
    opts.signal?.removeEventListener("abort", abort);
    clearRequests(new Disconnected("Codex subscription closed"));
    ws?.terminate();
  }

  function fail(reason: string) {
    if (closed || rejected) return;
    rejected = { status: "unreachable", reason };
    invalidate();
    announce();
    close();
  }

  function request<T>(method: string, params: unknown): Promise<T> {
    const socket = ws;
    if (closed || socket?.readyState !== WebSocket.OPEN)
      return Promise.reject(new Disconnected("Codex disconnected"));
    const id = ++nextId;
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        socket.terminate();
        clearRequests(new Disconnected("Codex request timed out"));
      }, 10_000);
      pending.set(id, {
        resolve: (value) => resolve(value as T),
        reject,
        timer,
      });
      socket.send(JSON.stringify({ id, method, params }), (error) => {
        if (error) socket.terminate();
      });
    });
  }

  async function readState(): Promise<CodexSnapshot> {
    const start = revision;
    const ids: string[] = [];
    let cursor: string | undefined;
    const cursors = new Set<string>();
    do {
      const page = await request<{
        data: Array<{ id: string }>;
        nextCursor?: string | null;
      }>("thread/queue/list", { threadId: opts.threadId, cursor });
      if (
        !Array.isArray(page?.data) ||
        page.data.some((item) => typeof item.id !== "string")
      )
        throw new Error("Codex returned an invalid queue");
      ids.push(...page.data.map((item) => item.id));
      cursor = page.nextCursor ?? undefined;
      if (cursor !== undefined) {
        if (typeof cursor !== "string" || cursors.has(cursor))
          throw new Error("Codex returned an invalid queue cursor");
        cursors.add(cursor);
      }
    } while (cursor !== undefined);
    const read = await request<{
      thread: { id: string; status: { type: string } };
    }>("thread/read", { threadId: opts.threadId });
    if (
      read?.thread?.id !== opts.threadId ||
      typeof read.thread.status?.type !== "string"
    )
      throw new Error("Codex returned an invalid thread");
    if (start === revision && ready && !closed)
      state = { status: read.thread.status.type, queuedIds: ids };
    return state;
  }

  function refresh(): Promise<CodexSnapshot> {
    if (!ready || closed) return Promise.resolve(state);
    refreshing ??= readState()
      .catch((error: unknown) => {
        if (error instanceof Disconnected) {
          ready = false;
          invalidate();
        } else {
          fail(
            error instanceof Error ? error.message : "Codex state read failed",
          );
        }
        return state;
      })
      .finally(() => {
        refreshing = undefined;
      });
    return refreshing;
  }

  async function attach() {
    const socket = new WebSocket("ws://localhost/", {
      createConnection: () => connect(opts.socket),
      perMessageDeflate: false,
      handshakeTimeout: 10_000,
      maxPayload: 8 * 1024 * 1024,
    });
    ws = socket;
    socket.on("error", () => {});
    socket.on("close", () => {
      if (socket !== ws || closed) return;
      ready = false;
      clearRequests(new Disconnected("Codex disconnected"));
      invalidate();
      if (!recovering) void recover();
    });
    socket.on("message", (data) => {
      if (socket !== ws || closed) return;
      let message: {
        id?: number;
        method?: string;
        params?: { threadId?: string };
        result?: unknown;
        error?: { code?: number };
      };
      try {
        message = JSON.parse(String(data));
      } catch {
        fail("Codex returned invalid JSON");
        return;
      }
      if (!message || typeof message !== "object") {
        fail("Codex returned an invalid response");
        return;
      }
      if (message.method) {
        if (message.params?.threadId !== opts.threadId) return;
        if (
          ["thread/closed", "thread/archived", "thread/deleted"].includes(
            message.method,
          )
        ) {
          fail("The bound Codex thread was closed or archived");
          return;
        }
        if (message.id !== undefined)
          opts.note?.("Codex requests user interaction; answer in Codex");
        if (
          [
            "thread/status/changed",
            "thread/queue/changed",
            "turn/started",
            "turn/completed",
          ].includes(message.method)
        )
          invalidate();
        return;
      }
      const entry =
        message.id === undefined ? undefined : pending.get(message.id);
      if (!entry || message.id === undefined) return;
      pending.delete(message.id);
      clearTimeout(entry.timer);
      if (message.error)
        entry.reject(
          new Error(
            `Codex rejected a subscription request (code ${message.error.code ?? "unknown"})`,
          ),
        );
      else if (Object.hasOwn(message, "result")) entry.resolve(message.result);
      else entry.reject(new Error("Codex response omitted its result"));
    });
    await new Promise<void>((resolve, reject) => {
      socket.once("open", resolve);
      socket.once("error", () =>
        reject(new Disconnected("Cannot connect to Codex")),
      );
      socket.once("close", () =>
        reject(new Disconnected("Codex closed during connection")),
      );
    });
    await request("initialize", {
      clientInfo: { name: "todou_watch", version: "1.0.0" },
      capabilities: { experimentalApi: true },
    });
    socket.send(JSON.stringify({ method: "initialized" }));
    const read = await request<{
      thread: { id: string; status: { type: string } };
    }>("thread/read", { threadId: opts.threadId });
    if (
      read?.thread?.id !== opts.threadId ||
      !["active", "idle"].includes(read.thread.status?.type)
    )
      throw new Error(
        "Open the target thread in Codex before attaching the watch; cold resume may change its permissions",
      );
    const resumed = await request<{ thread: { id: string } }>("thread/resume", {
      threadId: opts.threadId,
      excludeTurns: true,
    });
    if (resumed?.thread?.id !== opts.threadId)
      throw new Error("Codex resumed a different thread");
    ready = true;
    await readState();
    opts.onChange();
  }

  async function recover() {
    recovering = true;
    opts.note?.(
      "Codex subscription disconnected; collecting changes while reconnecting",
    );
    for (const delay of [1_000, 2_000, 5_000]) {
      await clock.sleep(delay, stop.signal);
      if (closed) return;
      try {
        await attach();
        recovering = false;
        opts.note?.("Codex subscription restored");
        return;
      } catch (error) {
        const socket = ws;
        ws = undefined;
        socket?.terminate();
        clearRequests(new Disconnected("Codex reconnect failed"));
        if (!(error instanceof Disconnected)) {
          fail(
            error instanceof Error
              ? error.message
              : "Codex subscription failed",
          );
          return;
        }
      }
    }
    fail("Codex subscription could not reconnect");
  }

  const poll = setInterval(() => opts.onChange(), 10_000);
  poll.unref();
  const abort = () => fail("Codex watch cancelled");
  opts.signal?.addEventListener("abort", abort, { once: true });
  try {
    if (opts.signal?.aborted) throw new Error("Codex watch cancelled");
    recovering = true;
    await attach();
    recovering = false;
  } catch (error) {
    close();
    throw error;
  }
  return {
    snapshot: () => state,
    refresh,
    get rejected() {
      return rejected;
    },
    whenRejected,
    close,
  };
}
