import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { onTestFinished } from "vitest";
import { WebSocketServer } from "ws";

export const THREAD = "01900000-0000-7000-8000-000000000001";

export async function codexPeer() {
  const dir = mkdtempSync(join(tmpdir(), "td-cx-"));
  const socket = join(dir, "s");
  const server = createServer();
  const wss = new WebSocketServer({ server });
  const calls: Array<{ method: string; params: Record<string, unknown> }> = [];
  const peer = {
    socket,
    calls,
    status: "idle",
    queued: [] as string[],
    response: undefined as
      | undefined
      | ((method: string, params: Record<string, unknown>) => Promise<unknown>),
    notify(method = "thread/status/changed") {
      for (const client of wss.clients)
        client.send(
          JSON.stringify({
            method,
            params: { threadId: THREAD, status: { type: peer.status } },
          }),
        );
    },
    disconnect() {
      for (const client of wss.clients) client.terminate();
    },
  };
  wss.on("connection", (client) => {
    client.on("message", async (raw) => {
      const message = JSON.parse(String(raw));
      calls.push(message);
      if (message.id === undefined) return;
      let result: unknown = {};
      if (
        message.method === "thread/read" ||
        message.method === "thread/resume"
      )
        result = { thread: { id: THREAD, status: { type: peer.status } } };
      if (message.method === "thread/queue/list")
        result = { data: peer.queued.map((id) => ({ id })), nextCursor: null };
      const replacement = await peer.response?.(message.method, message.params);
      if (replacement !== undefined) result = replacement;
      if (client.readyState === client.OPEN)
        client.send(JSON.stringify({ id: message.id, result }));
    });
  });
  await new Promise<void>((resolve) => server.listen(socket, resolve));
  onTestFinished(async () => {
    peer.disconnect();
    await new Promise<void>((resolve) => wss.close(() => resolve()));
    await new Promise<void>((resolve) => server.close(() => resolve()));
    rmSync(dir, { recursive: true, force: true });
  });
  return peer;
}
