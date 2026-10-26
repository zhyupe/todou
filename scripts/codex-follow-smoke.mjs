#!/usr/bin/env node
/**
 * `--follow=codex` against a real Codex app-server and a real todou server,
 * with a scripted stand-in for the model. Run from any directory:
 *   node scripts/codex-follow-smoke.mjs
 *   node scripts/codex-follow-smoke.mjs --codex-bin /path/to/codex --keep
 *   node scripts/codex-follow-smoke.mjs --cli result/bin/todou  # a built CLI
 *   node scripts/codex-follow-smoke.mjs --mutate queue   # must fail
 *   node scripts/codex-follow-smoke.mjs --mutate busy   # must fail
 *   node scripts/codex-follow-smoke.mjs --idle-unload-check
 *   node scripts/codex-follow-smoke.mjs --idle-unload-check --mutate subscription # must fail
 *
 * Detaches the observer before testing delivery, preserves cursor ranges,
 * combines changes across busy debounce windows, and checks shutdown recovery.
 * --idle-unload-check waits for an unsubscribed control thread to unload before
 * posting the first change, using the tested daemon's actual grace period.
 *
 * Mutations must fail: queue removes the admission receipt; busy falsifies the
 * watcher's status reads; subscription replaces its resume with a read. Use
 * --idle-unload-check with the subscription mutation to exercise idle unloading.
 *
 * The smoke posts as the account the watch runs as, so the watch takes
 * --any-actor; the self-filter is not what this checks.
 *
 * Needs no model login: the app-server talks to a local Responses API stub.
 * Sockets and CODEX_HOME live under os.tmpdir() because a Unix socket path
 * must fit in 108 bytes; everything else goes to a scratch dir under .tmp/.
 * Socket directories are removed on exit; --keep retains the scratch logs.
 */
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import {
  appendFileSync,
  chmodSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cliEntry = join(root, "projects/cli/src/index.ts");
const serverEntry = join(root, "projects/server/src/index.ts");
const WebSocket = createRequire(join(root, "projects/cli/package.json"))("ws");

const options = {
  "codex-bin": "codex",
  cli: "",
  mutate: "",
  keep: false,
  "idle-unload-check": false,
};
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, "");
  if (key === "keep" || key === "idle-unload-check") options[key] = true;
  else if (key in options) options[key] = process.argv[++i];
  else throw new Error(`unknown option ${process.argv[i]}`);
}
if (!["", "queue", "busy", "subscription"].includes(options.mutate)) {
  throw new Error("--mutate takes queue, busy, or subscription");
}

mkdirSync(join(root, ".tmp"), { recursive: true });
const scratch = mkdtempSync(join(root, ".tmp", "codex-follow-smoke-"));
const short = mkdtempSync(join(tmpdir(), "todou-cf-"));
const codexHome = join(short, "home");
mkdirSync(codexHome);
const appSocket = join(short, "app.sock");
const logFile = join(scratch, "smoke.log");
const children = [];
const log = (line) => {
  appendFileSync(logFile, `${new Date().toISOString()} ${line}\n`);
};
const say = (line) => {
  console.log(line);
  log(line);
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function cleanup() {
  for (const child of children) {
    if (child.exitCode === null) child.kill("SIGKILL");
  }
  rmSync(short, { recursive: true, force: true });
  if (!options.keep) rmSync(scratch, { recursive: true, force: true });
}
process.on("exit", cleanup);
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => process.exit(130));
}

async function until(what, predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await predicate();
    if (value) return value;
    await sleep(200);
  }
  throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}`);
}

function start(name, command, args, env) {
  const child = spawn(command, args, {
    cwd: root,
    env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.push(child);
  child.output = { stdout: "", stderr: "" };
  for (const stream of ["stdout", "stderr"]) {
    child[stream].on("data", (chunk) => {
      child.output[stream] += chunk;
      log(`[${name} ${stream}] ${String(chunk).trimEnd()}`);
    });
  }
  return child;
}

// --- the model -------------------------------------------------------------

/** Every Responses request, as the text of its newest input item. */
const modelRequests = [];
/** Requests whose newest item says SLOW are held until this resolves. */
let releaseSlow = () => {};
let slowGate = Promise.resolve();

const model = createServer((req, res) => {
  let body = "";
  req.on("data", (c) => {
    body += c;
  });
  req.on("end", async () => {
    if (!req.url.endsWith("/responses")) {
      res.writeHead(404).end();
      return;
    }
    const input = JSON.parse(body).input ?? [];
    const last = JSON.stringify(input.at(-1) ?? {});
    modelRequests.push(last);
    log(`[model] ${last.slice(0, 300)}`);
    if (last.includes("SLOW")) await slowGate;
    const id = `r${modelRequests.length}`;
    const events = [
      { type: "response.created", response: { id } },
      {
        type: "response.output_item.done",
        item: {
          type: "message",
          role: "assistant",
          id: `m${id}`,
          content: [{ type: "output_text", text: "ok" }],
        },
      },
      {
        type: "response.completed",
        response: {
          id,
          usage: {
            input_tokens: 0,
            input_tokens_details: null,
            output_tokens: 0,
            output_tokens_details: null,
            total_tokens: 0,
          },
        },
      },
    ];
    res.writeHead(200, { "content-type": "text/event-stream" });
    for (const ev of events) {
      res.write(`event: ${ev.type}\ndata: ${JSON.stringify(ev)}\n\n`);
    }
    res.end();
  });
});
await new Promise((r) => model.listen(0, "127.0.0.1", r));
const modelPort = model.address().port;
model.unref();

writeFileSync(
  join(codexHome, "config.toml"),
  [
    'model = "smoke-model"',
    'model_provider = "smoke"',
    'approval_policy = "never"',
    'sandbox_mode = "danger-full-access"',
    "",
    "[model_providers.smoke]",
    'name = "smoke"',
    `base_url = "http://127.0.0.1:${modelPort}/v1"`,
    'wire_api = "responses"',
    "requires_openai_auth = false",
    "supports_websockets = false",
    "",
  ].join("\n"),
);

// --- todou -----------------------------------------------------------------

writeFileSync(
  join(scratch, "server.toml"),
  [
    "[auth]",
    'mode = "single"',
    "",
    "[database]",
    `system = "pglite://${join(scratch, "db")}"`,
    "auto_migrate = true",
    "",
    "[storage]",
    'backend = "fs"',
    `path = "${join(scratch, "attachments")}"`,
    "",
  ].join("\n"),
);
const server = start(
  "server",
  process.execPath,
  [
    serverEntry,
    "serve",
    "--config",
    join(scratch, "server.toml"),
    "--port",
    "0",
  ],
  { PATH: process.env.PATH, HOME: scratch },
);
const serverPort = Number(
  (
    await until(
      "the todou server",
      () =>
        /todou server listening on :(\d+)\b/.exec(
          server.output.stdout + server.output.stderr,
        ),
      60_000,
    )
  )[1],
);
const base = `http://127.0.0.1:${serverPort}`;

async function api(path, init = {}, auth = {}) {
  const response = await fetch(`${base}${path}`, {
    ...init,
    headers: { "content-type": "application/json", ...auth, ...init.headers },
  });
  const text = await response.text();
  assert.ok(
    response.ok,
    `${init.method ?? "GET"} ${path}: ${response.status} ${text}`,
  );
  return { response, json: text ? JSON.parse(text) : null };
}
const login = await api("/api/auth/login", { method: "POST", body: "{}" });
const cookie = login.response.headers
  .getSetCookie()
  .map((c) => c.split(";")[0])
  .join("; ");
const token = (
  await api(
    "/api/me/tokens",
    { method: "POST", body: JSON.stringify({ name: "smoke" }) },
    { cookie },
  )
).json.token;
const bearer = { authorization: `Bearer ${token}` };
await api(
  "/api/projects",
  { method: "POST", body: JSON.stringify({ slug: "smoke", name: "Smoke" }) },
  bearer,
);
const issue = (
  await api(
    "/api/projects/smoke/issues",
    { method: "POST", body: JSON.stringify({ title: "follow me" }) },
    bearer,
  )
).json;
const comment = (body, number = issue.number) =>
  api(
    `/api/projects/smoke/issues/${number}/comments`,
    { method: "POST", body: JSON.stringify({ body }) },
    bearer,
  );

// --- codex -----------------------------------------------------------------

const codexBin = execFileSync(
  "sh",
  ["-c", 'command -v "$0"', options["codex-bin"]],
  {
    encoding: "utf8",
  },
).trim();
say(
  `codex: ${execFileSync(codexBin, ["--version"], { encoding: "utf8" }).trim()} at ${codexBin}`,
);
// Observe admission independently of the watcher and model.
const queueLog = join(scratch, "queued.jsonl");
writeFileSync(queueLog, "");
const queueBin = join(short, "codex-queue-observed");
writeFileSync(
  queueBin,
  `#!${process.execPath}
import { spawnSync } from 'node:child_process';
import { appendFileSync } from 'node:fs';
const args = process.argv.slice(2);
const result = spawnSync(${JSON.stringify(codexBin)}, args, { encoding: 'utf8' });
if (result.status === 0 && result.stdout.startsWith('Queued message '))
  appendFileSync(${JSON.stringify(queueLog)}, JSON.stringify(args) + '\\n');
process.stdout.write(${JSON.stringify(options.mutate === "queue")} ? '' : result.stdout || '');
process.stderr.write(result.stderr || '');
process.exit(result.status ?? 1);
`,
);
chmodSync(queueBin, 0o755);
const admitted = (marker) => readFileSync(queueLog, "utf8").includes(marker);
const codexEnv = { PATH: process.env.PATH, HOME: short, CODEX_HOME: codexHome };
start(
  "app-server",
  codexBin,
  ["app-server", "--listen", `unix://${appSocket}`],
  codexEnv,
);
await until(
  "the app-server socket",
  async () => {
    try {
      await new Promise((ok, fail) =>
        connect(appSocket)
          .once("connect", function () {
            this.destroy();
            ok();
          })
          .once("error", fail),
      );
      return true;
    } catch {
      return false;
    }
  },
  30_000,
);

// Only the watch uses this proxy; the observer always sees the real state.
let watchSocket = appSocket;
if (options.mutate === "busy" || options.mutate === "subscription") {
  watchSocket = join(short, "mutated.sock");
  const proxy = createServer();
  const peers = new WebSocket.WebSocketServer({ server: proxy });
  peers.on("connection", (client) => {
    const upstream = new WebSocket("ws://localhost/", {
      createConnection: () => connect(appSocket),
      perMessageDeflate: false,
    });
    const backlog = [];
    client.on("message", (data) => {
      const message = JSON.parse(String(data));
      if (
        options.mutate === "subscription" &&
        message.method === "thread/resume"
      ) {
        message.method = "thread/read";
        message.params = { threadId: message.params.threadId };
      }
      const encoded = JSON.stringify(message);
      if (upstream.readyState === WebSocket.OPEN) upstream.send(encoded);
      else backlog.push(encoded);
    });
    upstream.on("open", () => {
      for (const message of backlog.splice(0)) upstream.send(message);
    });
    upstream.on("message", (data) => {
      const message = JSON.parse(String(data));
      if (
        options.mutate === "busy" &&
        message.result?.thread?.status?.type === "active"
      )
        message.result.thread.status = { type: "idle" };
      if (client.readyState === WebSocket.OPEN)
        client.send(JSON.stringify(message));
    });
    client.on("close", () => upstream.terminate());
    client.on("error", () => upstream.terminate());
    upstream.on("close", () => client.terminate());
    upstream.on("error", () => client.terminate());
  });
  await new Promise((resolve) => proxy.listen(watchSocket, resolve));
}

const app = new WebSocket("ws://localhost/", {
  createConnection: () => connect(appSocket),
  perMessageDeflate: false,
});
await new Promise((ok, fail) => app.once("open", ok).once("error", fail));
let rpcId = 0;
const pending = new Map();
/** turn/started and turn/completed per thread, in arrival order. */
const turns = [];
app.on("message", (data) => {
  const message = JSON.parse(String(data));
  if (
    message.method === "turn/started" ||
    message.method === "turn/completed"
  ) {
    turns.push({
      event: message.method,
      thread: message.params.threadId,
      turn: message.params.turn?.id,
    });
  }
  if (message.id !== undefined && pending.has(message.id)) {
    pending.get(message.id)(message);
    pending.delete(message.id);
  }
});
const rpc = (method, params) =>
  new Promise((ok) => {
    const id = ++rpcId;
    pending.set(id, ok);
    app.send(JSON.stringify({ id, method, params }));
  }).then((m) => {
    assert.ok(!m.error, `${method}: ${JSON.stringify(m.error)}`);
    return m.result;
  });
await rpc("initialize", {
  clientInfo: { name: "smoke-app", version: "0" },
  capabilities: { experimentalApi: true },
});
app.send(JSON.stringify({ method: "initialized" }));

async function thread() {
  const id = (await rpc("thread/start", { cwd: scratch })).thread.id;
  // A first turn gives the thread its rollout, which `codex queue` looks up.
  await rpc("turn/start", {
    threadId: id,
    input: [{ type: "text", text: "hello" }],
  });
  await until("the first turn", () => completed(id) >= 1, 30_000);
  return id;
}
const completed = (id) =>
  turns.filter((t) => t.thread === id && t.event === "turn/completed").length;

function watch(threadId, extra) {
  return start(
    `watch ${threadId.slice(-4)}`,
    options.cli ? resolve(options.cli) : process.execPath,
    [
      ...(options.cli ? [] : [cliEntry]),
      "watch",
      "-p",
      "smoke",
      "--follow=codex",
      "--debounce",
      "1",
      "--interval",
      "1",
      "--any-actor",
    ],
    {
      PATH: process.env.PATH,
      HOME: short,
      XDG_CONFIG_HOME: join(scratch, "xdg"),
      XDG_STATE_HOME: join(scratch, "xdg"),
      TODOU_SERVER: base,
      TODOU_TOKEN: token,
      CODEX_HOME: codexHome,
      CODEX_THREAD_ID: threadId,
      TODOU_CODEX_BIN: queueBin,
      TODOU_CODEX_SOCKET: watchSocket,
      ...extra,
    },
  );
}
const requestWith = (marker) => modelRequests.find((r) => r.includes(marker));
const failures = [];
async function check(name, body) {
  try {
    await body();
    say(`ok   ${name}`);
  } catch (error) {
    failures.push(name);
    say(`FAIL ${name}: ${error.message}`);
  }
}

const idle = async (id) =>
  (await rpc("thread/read", { threadId: id })).thread.status.type === "idle";
const queued = await thread();
const queueWatch = watch(queued, {});
await until(
  "the watch subscription",
  () => queueWatch.output.stderr.includes("coalescing changes until idle"),
  30000,
);
await rpc("thread/unsubscribe", { threadId: queued });

if (options["idle-unload-check"]) {
  const control = await thread();
  await rpc("thread/unsubscribe", { threadId: control });
  const since = Date.now();
  say(
    "Waiting for an unsubscribed control thread to unload; observer reads do not subscribe",
  );
  while (true) {
    await sleep(10_000);
    assert.equal(
      queueWatch.exitCode,
      null,
      `watch stopped during idle interval: ${queueWatch.output.stderr}`,
    );
    assert.equal(
      queueWatch.signalCode,
      null,
      "watch terminated during idle interval",
    );
    const status = (await rpc("thread/read", { threadId: control })).thread
      .status.type;
    if (status === "notLoaded") break;
    assert.ok(
      Date.now() - since < 31 * 60_000,
      "control thread did not unload within 31 minutes",
    );
  }
  assert.equal(
    await idle(queued),
    true,
    "watcher's thread must remain loaded after the control unloads",
  );
  say(
    `Control unloaded after ${Math.round((Date.now() - since) / 1000)} seconds; watcher still subscribed`,
  );
}
const first = `SMOKE-QUEUE-${randomUUID()}`;
await comment(first);
await check(
  "sole watcher subscription starts and completes a queued turn",
  async () => {
    await until("the queued model request", () => requestWith(first), 45000);
    await until("the queued turn to finish", () => idle(queued), 30000);
  },
);
const second = `SMOKE-RANGE-${randomUUID()}`;
await comment(second);
await check("successive batches preserve the cursor range", async () => {
  const a = await until("first batch", () => requestWith(first), 1000);
  const b = await until("second batch", () => requestWith(second), 45000);
  const cursor = /cursor: ([^\\\s]+)/.exec(
    JSON.parse(a).content?.[0]?.text ?? a,
  )?.[1];
  const since = /since: ([^\\\s]+)/.exec(
    JSON.parse(b).content?.[0]?.text ?? b,
  )?.[1];
  assert.ok(cursor);
  assert.equal(since, cursor);
  await until("second completion", () => idle(queued), 30000);
});
queueWatch.kill("SIGTERM");
await until(
  "first watcher exit",
  () => queueWatch.exitCode !== null || queueWatch.signalCode !== null,
  10000,
);

const busy = await thread();
const other = (
  await api(
    "/api/projects/smoke/issues",
    { method: "POST", body: JSON.stringify({ title: "another card" }) },
    bearer,
  )
).json;
const busyWatch = watch(busy, {});
await until(
  "busy watch subscription",
  () => busyWatch.output.stderr.includes("coalescing changes until idle"),
  30000,
);
slowGate = new Promise((r) => {
  releaseSlow = r;
});
await rpc("turn/start", {
  threadId: busy,
  input: [{ type: "text", text: "SLOW work" }],
});
await until("slow request", () => requestWith("SLOW work"), 10000);
await rpc("thread/unsubscribe", { threadId: busy });
const markers = Array.from({ length: 3 }, () => `SMOKE-BUSY-${randomUUID()}`);
await check(
  "busy thread collects across debounce windows without queueing",
  async () => {
    for (let i = 0; i < markers.length; i++) {
      await comment(markers[i], i % 2 ? other.number : undefined);
      await sleep(1500);
      assert.equal(
        admitted(markers[i]),
        false,
        "a busy thread must not receive a queued batch",
      );
      assert.equal(requestWith(markers[i]), undefined);
    }
  },
);
releaseSlow();
await check("one idle batch includes every card and event", async () => {
  const request = await until(
    "combined model request",
    () => requestWith(markers[0]),
    45000,
  );
  for (const marker of markers) assert.ok(request.includes(marker));
  const batches = readFileSync(queueLog, "utf8")
    .trim()
    .split("\n")
    .filter((line) => line.includes(markers[0]));
  assert.equal(batches.length, 1);
  for (const marker of markers) assert.ok(batches[0].includes(marker));
  await until("combined turn completion", () => idle(busy), 30000);
  const history = await rpc("thread/turns/list", {
    threadId: busy,
    limit: 100,
    itemsView: "notLoaded",
  });
  assert.equal(history.data.length, 3, "initial, busy, and one combined turn");
});
slowGate = new Promise((r) => {
  releaseSlow = r;
});
// Resume the UI solely to initiate another turn, then detach again.
await rpc("thread/resume", { threadId: busy, excludeTurns: true });
await rpc("turn/start", {
  threadId: busy,
  input: [{ type: "text", text: "SLOW stop check" }],
});
await until("stop request", () => requestWith("SLOW stop check"), 10000);
await rpc("thread/unsubscribe", { threadId: busy });
const pendingMarker = `SMOKE-STOP-${randomUUID()}`;
await comment(pendingMarker);
await sleep(1500);
busyWatch.kill("SIGTERM");
await until(
  "busy watcher exit",
  () => busyWatch.exitCode !== null || busyWatch.signalCode !== null,
  10000,
);
await check(
  "stopping while busy prints pending changes without queueing",
  async () => {
    assert.equal(admitted(pendingMarker), false);
    assert.ok(busyWatch.output.stdout.includes(pendingMarker));
    assert.ok(busyWatch.output.stdout.includes("since:"));
  },
);
releaseSlow();
app.close();
say(
  failures.length
    ? `${failures.length} failed checks`
    : "All Codex follow checks passed",
);
process.exit(failures.length ? 1 : 0);
