import { afterEach, describe, expect, it, vi } from "vitest";
import { openCodex, type QueueRunner } from "../src/codex-push.ts";
import type { CodexSnapshot } from "../src/codex-session.ts";
import type { PeerPush } from "../src/peer-push.ts";

const THREAD = "01900000-0000-7000-8000-000000000001";
const pushes: Array<PeerPush<string>> = [];
afterEach(() => {
  for (const push of pushes.splice(0)) push.close();
  vi.useRealTimers();
});

async function fixture(
  status = "active",
  debounceSec = 60,
  run?: QueueRunner,
  renderItems = (items: string[]) => items.join("|"),
) {
  vi.useFakeTimers();
  let state: CodexSnapshot = { status, queuedIds: [] };
  let notify = () => {};
  const messages: string[] = [];
  const push = await openCodex<string>({
    target: {
      threadId: THREAD,
      socket: "/test/control.sock",
      bin: "/test/codex",
    },
    debounceSec,
    render: (items, since, cursor) =>
      `${renderItems(items)}\nsince: ${since}\ncursor: ${cursor}`,
    openSession: async (opts) => {
      notify = opts.onChange;
      return {
        snapshot: () => state,
        refresh: async () => state,
        rejected: null,
        whenRejected: new Promise(() => {}),
        close() {},
      };
    },
    run:
      run ??
      (async (_, args) => {
        const message = args[4];
        if (!message) throw new Error("queue invocation omitted its message");
        messages.push(message);
        state = { ...state, queuedIds: ["receipt"] };
        return `Queued message receipt for thread ${THREAD}.`;
      }),
  });
  pushes.push(push);
  return {
    push,
    messages,
    change(status: string, queuedIds: string[] | null = []) {
      state = { status, queuedIds };
      notify();
    },
  };
}

describe("Codex idle coalescing", () => {
  it("collects across many windows without queuing, then sends all changes once", async () => {
    const f = await fixture();
    for (let i = 0; i < 3; i++) {
      await f.push.send([`card-${i}`], `c${i}`, `c${i + 1}`);
      await vi.advanceTimersByTimeAsync(61_000);
      expect(f.messages).toHaveLength(0);
    }
    f.change("idle");
    await vi.advanceTimersByTimeAsync(1);
    expect(f.messages).toHaveLength(1);
    expect(f.messages[0]).toBe("card-0|card-1|card-2\nsince: c0\ncursor: c3");
    expect(f.push.unconfirmed().items).toEqual([]);
  });

  it("holds new changes until the earlier receipt is consumed and the thread is idle", async () => {
    const f = await fixture("idle", 0);
    await f.push.send(["first"], "c0", "c1");
    await vi.advanceTimersByTimeAsync(1);
    await f.push.send(["second"], "c1", "c2");
    await vi.advanceTimersByTimeAsync(61_000);
    expect(f.messages).toHaveLength(1);
    f.change("active");
    await f.push.send(["third"], "c2", "c3");
    await vi.advanceTimersByTimeAsync(61_000);
    expect(f.messages).toHaveLength(1);
    f.change("idle");
    f.change("idle");
    await vi.advanceTimersByTimeAsync(1);
    expect(f.messages).toHaveLength(2);
    expect(f.messages[1]).toContain("second|third\nsince: c1\ncursor: c3");
  });

  it("uses a fixed debounce window when already idle", async () => {
    const f = await fixture("idle");
    await f.push.send(["first"], "c0", "c1");
    await vi.advanceTimersByTimeAsync(30_000);
    await f.push.send(["second"], "c1", "c2");
    await vi.advanceTimersByTimeAsync(30_001);
    expect(f.messages).toHaveLength(1);
    expect(f.messages[0]).toContain("first|second");
  });

  it.each(["unknown", "notLoaded", "systemError", "active"])(
    "does not deliver while %s",
    async (status) => {
      const f = await fixture(status, 0);
      await f.push.send(["pending"], "c0", "c1");
      await vi.advanceTimersByTimeAsync(60_000);
      expect(f.messages).toEqual([]);
      expect(f.push.unconfirmed().items).toEqual(["pending"]);
      f.push.close();
      f.change("idle");
      await vi.advanceTimersByTimeAsync(60_000);
      expect(f.messages).toEqual([]);
    },
  );

  it("bounds a large pending batch while retaining its complete cursor range", async () => {
    const f = await fixture();
    await f.push.send(["x".repeat(100 * 1024)], "c0", "c1");
    await f.push.send(["last"], "c1", "c2");
    expect(f.push.unconfirmed()).toMatchObject({
      items: [],
      since: "c0",
      cursor: "c2",
      summaryOnly: true,
    });
    f.change("idle");
    await vi.advanceTimersByTimeAsync(1);
    expect(f.messages).toHaveLength(1);
    expect(f.messages[0]).toContain("re-read all changes");
    expect(f.messages[0]).toContain("since: c0\ncursor: c2");
  });

  it("retains an uncertain in-flight batch and changes arriving during its submission", async () => {
    let reject!: (reason: Error) => void;
    const f = await fixture(
      "idle",
      0,
      async () =>
        new Promise<string>((_, no) => {
          reject = no;
        }),
    );
    await f.push.send(["first"], "c0", "c1");
    await vi.advanceTimersByTimeAsync(1);
    await f.push.send(["second"], "c1", "c2");
    reject(new Error("connection lost"));
    await vi.advanceTimersByTimeAsync(1);
    expect(f.push.rejected).not.toBeNull();
    expect(f.push.unconfirmed()).toMatchObject({
      items: ["first", "second"],
      since: "c0",
      cursor: "c2",
    });
  });

  it("bounds stored bodies even when the output renderer summarizes them", async () => {
    const f = await fixture("active", 60, undefined, () => "short preview");
    await f.push.send(["x".repeat(100 * 1024)], "c0", "c1");
    expect(f.push.unconfirmed()).toMatchObject({
      items: [],
      since: "c0",
      cursor: "c1",
      summaryOnly: true,
    });
  });

  it("waits when idle but the queue state is unknown", async () => {
    const f = await fixture("idle", 0);
    f.change("idle", null);
    await f.push.send(["pending"], "c0", "c1");
    await vi.advanceTimersByTimeAsync(60_000);
    expect(f.messages).toEqual([]);
    f.change("idle");
    await vi.advanceTimersByTimeAsync(1);
    expect(f.messages).toHaveLength(1);
  });

  it("keeps later changes local when another client starts a turn during admission", async () => {
    let confirm!: (receipt: string) => void;
    const calls = vi.fn(
      async () =>
        new Promise<string>((resolve) => {
          confirm = resolve;
        }),
    );
    const f = await fixture("idle", 0, calls);
    await f.push.send(["first"], "c0", "c1");
    await vi.advanceTimersByTimeAsync(1);
    f.change("active", ["receipt"]);
    await f.push.send(["second"], "c1", "c2");
    confirm(`Queued message receipt for thread ${THREAD}.`);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(calls).toHaveBeenCalledTimes(1);
    expect(f.push.unconfirmed().items).toEqual(["second"]);
    f.change("idle");
    await vi.advanceTimersByTimeAsync(1);
    expect(calls).toHaveBeenCalledTimes(2);
    f.push.close();
    confirm(`Queued message receipt for thread ${THREAD}.`);
    await vi.advanceTimersByTimeAsync(1);
    expect(f.push.unconfirmed().items).toEqual(["second"]);
  });
});
