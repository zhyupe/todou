import { afterEach, expect, it, vi } from "vitest";
import { type CodexSession, openCodexSession } from "../src/codex-session.ts";
import { codexPeer, THREAD } from "./helpers/codex-peer.ts";

const sessions: CodexSession[] = [];
afterEach(() => {
  for (const session of sessions.splice(0)) session.close();
  vi.useRealTimers();
});

it("subscribes without overrides, reads every queue page and closes its connection", async () => {
  const peer = await codexPeer();
  peer.response = async (method, params) =>
    method === "thread/queue/list"
      ? params.cursor
        ? { data: [{ id: "second" }] }
        : { data: [{ id: "first" }], nextCursor: "page2" }
      : undefined;
  const session = await openCodexSession({
    ...peer,
    threadId: THREAD,
    onChange: () => {},
  });
  sessions.push(session);
  expect(session.snapshot()).toEqual({
    status: "idle",
    queuedIds: ["first", "second"],
  });
  expect(
    peer.calls.find((call) => call.method === "thread/resume")?.params,
  ).toEqual({ threadId: THREAD, excludeTurns: true });
  expect(peer.calls[0]?.params.capabilities).toEqual({ experimentalApi: true });
  session.close();
  session.close();
});

it("refuses cold resume before it can replace the thread's permissions", async () => {
  const peer = await codexPeer();
  peer.status = "notLoaded";
  await expect(
    openCodexSession({ ...peer, threadId: THREAD, onChange: () => {} }),
  ).rejects.toThrow("cold resume");
  expect(peer.calls.some((call) => call.method === "thread/resume")).toBe(
    false,
  );
});

it("invalidates a stale idle read when an active notification overtakes it", async () => {
  const peer = await codexPeer();
  const changed = vi.fn();
  const session = await openCodexSession({
    ...peer,
    threadId: THREAD,
    onChange: changed,
  });
  sessions.push(session);
  peer.response = async (method) => {
    if (method !== "thread/read") return;
    peer.status = "active";
    peer.notify();
    return { thread: { id: THREAD, status: { type: "idle" } } };
  };
  expect((await session.refresh()).status).toBe("unknown");
  expect(changed).toHaveBeenCalled();
  peer.response = undefined;
  expect((await session.refresh()).status).toBe("active");
});

it("reconnects a dropped subscription and refreshes busy state", async () => {
  const peer = await codexPeer();
  const notes: string[] = [];
  const session = await openCodexSession({
    ...peer,
    threadId: THREAD,
    onChange: () => {},
    note: (message) => notes.push(message),
    clock: { now: Date.now, sleep: async () => {} },
  });
  sessions.push(session);
  peer.status = "active";
  peer.disconnect();
  await vi.waitFor(() =>
    expect(notes).toContain("Codex subscription restored"),
  );
  expect(session.snapshot().status).toBe("active");
  expect(
    peer.calls.filter((call) => call.method === "thread/resume"),
  ).toHaveLength(2);
});

it.each(["thread/closed", "thread/archived", "thread/deleted"])(
  "stops on %s without reopening the thread",
  async (method) => {
    const peer = await codexPeer();
    const session = await openCodexSession({
      ...peer,
      threadId: THREAD,
      onChange: () => {},
    });
    sessions.push(session);
    peer.notify(method);
    await session.whenRejected;
    expect(session.rejected).not.toBeNull();
    expect(
      peer.calls.filter((call) => call.method === "thread/resume"),
    ).toHaveLength(1);
  },
);

it("invalidates a previously idle state when its refresh times out", async () => {
  const peer = await codexPeer();
  const session = await openCodexSession({
    ...peer,
    threadId: THREAD,
    onChange: () => {},
  });
  sessions.push(session);
  peer.response = async () => new Promise(() => {});
  vi.useFakeTimers();
  const reading = session.refresh();
  await vi.advanceTimersByTimeAsync(10_000);
  expect(await reading).toEqual({ status: "unknown", queuedIds: null });
});

it("stops after bounded reconnect attempts and never treats a lost connection as idle", async () => {
  const peer = await codexPeer();
  const delays: number[] = [];
  const session = await openCodexSession({
    ...peer,
    threadId: THREAD,
    onChange: () => {},
    clock: {
      now: Date.now,
      sleep: async (ms) => {
        delays.push(ms);
      },
    },
  });
  sessions.push(session);
  peer.response = async () => {
    peer.disconnect();
  };
  peer.disconnect();
  await session.whenRejected;
  expect(delays).toEqual([1000, 2000, 5000]);
  expect(session.snapshot()).toEqual({ status: "unknown", queuedIds: null });
  expect(session.rejected?.reason).toContain("could not reconnect");
});

it("cancels a reconnect delay without reattaching", async () => {
  const peer = await codexPeer();
  const stop = new AbortController();
  let sleeping = false;
  const session = await openCodexSession({
    ...peer,
    threadId: THREAD,
    signal: stop.signal,
    onChange: () => {},
    clock: {
      now: Date.now,
      sleep: async (_, signal) => {
        if (!signal) throw new Error("reconnect delay requires cancellation");
        sleeping = true;
        await new Promise<void>((resolve) =>
          signal.addEventListener("abort", () => resolve(), { once: true }),
        );
      },
    },
  });
  sessions.push(session);
  peer.disconnect();
  await vi.waitFor(() => expect(sleeping).toBe(true));
  stop.abort();
  await session.whenRejected;
  expect(
    peer.calls.filter((call) => call.method === "thread/resume"),
  ).toHaveLength(1);
});
