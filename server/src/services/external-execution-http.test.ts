import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, rename, rm, symlink, writeFile } from "node:fs/promises";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { stripTypeScriptTypes } from "node:module";
import { dirname, join, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { runInNewContext } from "node:vm";
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { createExternalExecutionHttpPort } from "./external-execution-http.js";
import { createExternalExecutionLifecycle, type ExternalExecutionPort, type ExternalExecutionRun } from "./external-execution.js";

const run = { id: randomUUID(), companyId: randomUUID(), driverKind: "hive_runtime", status: "running" };
// Synthetic test credentials, never a live gateway descriptor.
const secret = "a".repeat(43);
const rotatedSecret = "b".repeat(43);
const fixturesRoot = resolve(fileURLToPath(new URL("../../../logs/p2-external-execution/http-port-tests/", import.meta.url)));
const servers: Server[] = [];
let fixture: string;
type Action = "recover" | "cancel" | "drain";
type RequestRecord = { method?: string; url?: string; authorization?: string; body: string };

beforeAll(async () => { await mkdir(fixturesRoot, { recursive: true }); });
beforeEach(async () => { fixture = await mkdtemp(join(fixturesRoot, "fixture-")); });
afterEach(async () => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
  await Promise.all(servers.splice(0).map(server => new Promise<void>((resolveClose, reject) => {
    server.close(error => error ? reject(error) : resolveClose());
    server.closeAllConnections();
  })));
  // Verify the absolute target before any recursive removal on Windows.
  expect(dirname(resolve(fixture))).toBe(fixturesRoot);
  await rm(fixture, { recursive: true, force: true });
});

function acceptance(action: Action = "recover") {
  return { accepted: true, companyId: run.companyId, runId: run.id, action, retained: true };
}

function json(response: ServerResponse, value: unknown, status = 202) {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(value));
}

async function gateway(respond?: (request: RequestRecord, response: ServerResponse, incoming: IncomingMessage) => void | Promise<void>) {
  const requests: RequestRecord[] = [];
  const server = createServer((incoming, response) => {
    void (async () => {
      const chunks: Buffer[] = [];
      for await (const chunk of incoming) chunks.push(Buffer.from(chunk));
      const request = {
        method: incoming.method, url: incoming.url, authorization: incoming.headers.authorization,
        body: Buffer.concat(chunks).toString("utf8"),
      };
      requests.push(request);
      if (respond) await respond(request, response, incoming);
      else json(response, acceptance(JSON.parse(request.body).action));
    })().catch(() => { if (!response.headersSent) json(response, { error: "test_gateway_error" }, 500); });
  });
  servers.push(server);
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => { server.removeListener("error", reject); resolveListen(); });
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Test gateway did not bind an IPv4 port");
  return { server, requests, baseUrl: `http://127.0.0.1:${address.port}` };
}

async function descriptor(baseUrl: string, patch: Record<string, unknown> = {}) {
  const path = join(fixture, "descriptor.json");
  await writeFile(path, JSON.stringify({ baseUrl, secret, ...patch }));
  return path;
}

function invoke(port: ExternalExecutionPort, action: Action, identity: ExternalExecutionRun = run) {
  return action === "recover" ? port.recover(identity) : port[action](identity, "core_reason_stays_in_database");
}

describe("external execution observer HTTP port", () => {
  it("keeps the explicit unavailable lifecycle when no descriptor is configured", async () => {
    vi.stubEnv("HIVE_PAPERCLIP_EXTERNAL_EXECUTION_DESCRIPTOR", "");
    expect(createExternalExecutionHttpPort()).toBeUndefined();
    const persistIntent = vi.fn(async () => {});
    const onUnavailable = vi.fn();
    const lifecycle = createExternalExecutionLifecycle({ persistIntent, onUnavailable, port: createExternalExecutionHttpPort() });
    expect(await lifecycle.cancel(run, "budget_pause")).toBe(true);
    expect(persistIntent).toHaveBeenCalledWith(run, { kind: "cancel", reason: "budget_pause" });
    expect(onUnavailable).toHaveBeenCalledWith(run, "cancel");
    expect(run.status).toBe("running");
  });

  it.each(["recover", "cancel", "drain"] as const)("sends only %s for the persisted company/run and retains the execution", async action => {
    const remote = await gateway();
    const path = await descriptor(remote.baseUrl);
    vi.stubEnv("HIVE_PAPERCLIP_EXTERNAL_EXECUTION_DESCRIPTOR", path);
    const port = createExternalExecutionHttpPort()!;
    const retained = {
      ...run, adapterType: "codex_local", processPid: process.pid,
      checkoutRunId: run.id, executionRunId: run.id,
      contextSnapshot: { hiveExternalExecutionControl: { kind: action, reason: "persisted_reason", cancellation: { errorCode: "budget_exceeded" } } },
    };
    const before = structuredClone(retained);
    await expect(invoke(port, action, retained)).resolves.toBeUndefined();
    expect(remote.requests).toEqual([{
      method: "POST", url: `/hive/external-execution/${run.companyId}/${run.id}`,
      authorization: `Bearer ${secret}`, body: JSON.stringify({ action }),
    }]);
    expect(retained).toEqual(before);
  });

  it("rereads an atomically rotated credential for each call on the same port", async () => {
    let currentSecret = secret;
    const remote = await gateway((request, response) => {
      json(response, acceptance(JSON.parse(request.body).action), request.authorization === `Bearer ${currentSecret}` ? 202 : 403);
    });
    const path = await descriptor(remote.baseUrl);
    const port = createExternalExecutionHttpPort({ descriptorPath: path })!;
    await port.recover(run);
    const replacement = join(fixture, "rotated.json");
    await writeFile(replacement, JSON.stringify({ baseUrl: remote.baseUrl, secret: rotatedSecret }));
    await rename(replacement, path);
    currentSecret = rotatedSecret;
    await port.cancel(run, "explicit_cancel");
    expect(remote.requests.map(request => request.authorization)).toEqual([`Bearer ${secret}`, `Bearer ${rotatedSecret}`]);
  });

  it.each(["", "/"])("accepts a strict loopback origin with suffix %j", async suffix => {
    const remote = await gateway();
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl + suffix) })!;
    await port.recover(run);
    expect(remote.requests).toHaveLength(1);
  });

  it.each([403, 503])("fails closed on HTTP %i without exposing the credential or response", async status => {
    const remote = await gateway((_request, response) => json(response, { error: secret, token: rotatedSecret }, status));
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl) })!;
    const error = await port.cancel(run, "budget_pause").catch(error => error);
    expect(error).toEqual(new Error("EXTERNAL_RUNTIME_UNAVAILABLE"));
    expect(error.cause).toBeUndefined();
    expect(String(error)).not.toContain(secret);
    expect(String(error)).not.toContain(rotatedSecret);
  });

  it("retains persisted cancellation and business locks when the HTTP gateway is unavailable", async () => {
    const retained = { ...run, checkoutRunId: run.id, executionRunId: run.id };
    let persisted = false;
    const remote = await gateway((_request, response) => {
      expect(persisted).toBe(true);
      json(response, { error: "unavailable" }, 503);
    });
    const persistIntent = vi.fn(async () => { persisted = true; });
    const onUnavailable = vi.fn();
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl) })!;
    const lifecycle = createExternalExecutionLifecycle({ port, persistIntent, onUnavailable });
    expect(await lifecycle.cancel(retained, "budget_pause", { errorCode: "budget_exceeded" })).toBe(true);
    expect(persistIntent).toHaveBeenCalledWith(retained, { kind: "cancel", reason: "budget_pause", cancellation: { errorCode: "budget_exceeded" } });
    expect(onUnavailable).toHaveBeenCalledWith(retained, "cancel");
    expect(retained).toEqual({ ...run, checkoutRunId: run.id, executionRunId: run.id });
  });

  it("rejects redirects without forwarding the credential to another loopback server", async () => {
    const target = await gateway();
    const remote = await gateway((_request, response) => {
      response.writeHead(307, { Location: `${target.baseUrl}/hive/redirect-target` });
      response.end();
    });
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl) })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(1);
    expect(target.requests).toHaveLength(0);
  });

  it.each(["headers", "body"])("bounds an HTTP timeout while waiting for %s", async phase => {
    const remote = await gateway((_request, response) => {
      if (phase === "body") {
        response.writeHead(202, { "Content-Type": "application/json" });
        response.write('{"accepted":true');
      }
    });
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl), requestTimeoutMs: 100 })!;
    const started = performance.now();
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(performance.now() - started).toBeLessThan(1500);
    expect(remote.requests).toHaveLength(1);
  });

  it.each([
    ["wrong company", { ...acceptance(), companyId: randomUUID() }],
    ["wrong run", { ...acceptance(), runId: randomUUID() }],
    ["wrong action", acceptance("cancel")],
    ["invalid company", { ...acceptance(), companyId: "company:forged" }],
    ["invalid run", { ...acceptance(), runId: "run:forged" }],
    ["false accepted", { ...acceptance(), accepted: false }],
    ["false retained", { ...acceptance(), retained: false }],
    ["missing retained", { accepted: true, companyId: run.companyId, runId: run.id, action: "recover" }],
    ["extra stopped claim", { ...acceptance(), stopped: true }],
    ["extra binding", { ...acceptance(), binding: { secret } }],
    ["array", [acceptance()]],
    ["null", null],
  ])("rejects a forged or invalid response: %s", async (_name, value) => {
    const remote = await gateway((_request, response) => json(response, value));
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl) })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
  });

  it.each(["empty", "malformed JSON", "invalid UTF-8", "wrong content type", "declared oversize", "streamed oversize"])("rejects %s response bodies", async kind => {
    const remote = await gateway((_request, response) => {
      const headers: Record<string, string> = { "Content-Type": kind === "wrong content type" ? "text/html" : "application/json" };
      if (kind === "declared oversize") headers["Content-Length"] = "2049";
      response.writeHead(202, headers);
      if (kind === "streamed oversize") {
        response.write(JSON.stringify(acceptance()));
        response.end(" ".repeat(2049));
      } else response.end(kind === "invalid UTF-8" ? Buffer.from([0xff, 0xfe]) : kind === "empty" ? "" : kind === "malformed JSON" ? "{" : JSON.stringify(acceptance()));
    });
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl), requestTimeoutMs: 500 })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
  });

  it.each([null, undefined, "process", "codex_local", "HIVE_RUNTIME"])("rejects a direct call for persisted driver %s", async driverKind => {
    const remote = await gateway();
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl) })!;
    await expect(port.recover({ ...run, driverKind })).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
  });

  it.each([
    { companyId: "company:forged" }, { id: "run:forged" }, { companyId: "../other" }, { id: "../cancel" },
  ])("rejects an invalid direct execution identity %j", async patch => {
    const remote = await gateway();
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl) })!;
    await expect(port.drain({ ...run, ...patch }, "shutdown")).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
  });

  it.each([0, 9, 10_001, Infinity, NaN, 12.5])("rejects an unbounded or invalid timeout %s", async requestTimeoutMs => {
    const remote = await gateway();
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl), requestTimeoutMs })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
  });

  it.each([
    ["HTTPS", (base: string) => base.replace("http:", "https:")],
    ["localhost", (base: string) => base.replace("127.0.0.1", "localhost")],
    ["other loopback IP", (base: string) => base.replace("127.0.0.1", "127.0.0.2")],
    ["IPv6 loopback", (base: string) => base.replace("127.0.0.1", "[::1]")],
    ["short IPv4", (base: string) => base.replace("127.0.0.1", "127.1")],
    ["integer IPv4", (base: string) => base.replace("127.0.0.1", "2130706433")],
    ["hex IPv4", (base: string) => base.replace("127.0.0.1", "0x7f000001")],
    ["username", (base: string) => base.replace("http://", "http://user@")],
    ["password", (base: string) => base.replace("http://", "http://user:password@")],
    ["path", (base: string) => `${base}/hive`],
    ["encoded path", (base: string) => `${base}/%2f`],
    ["query", (base: string) => `${base}/?token=forged`],
    ["fragment", (base: string) => `${base}/#forged`],
    ["empty query", (base: string) => `${base}?`],
    ["empty fragment", (base: string) => `${base}#`],
    ["whitespace", (base: string) => ` ${base}`],
    ["noncanonical port", (base: string) => base.replace(/:(\d+)$/, ":0$1")],
    ["missing port", () => "http://127.0.0.1"],
    ["default port", () => "http://127.0.0.1:80"],
  ] as const)("rejects descriptor authority with %s before sending", async (_name, transform) => {
    const remote = await gateway();
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(transform(remote.baseUrl)) })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
  });

  it.each([
    { secret: "a".repeat(42) }, { secret: "a".repeat(44) }, { secret: "+".repeat(43) },
    { secret: "/".repeat(43) }, { secret: " ".repeat(43) }, { secret: 43 },
    { credential: secret }, { baseUrl: null },
  ])("rejects malformed descriptor fields %j", async patch => {
    const remote = await gateway();
    const port = createExternalExecutionHttpPort({ descriptorPath: await descriptor(remote.baseUrl, patch) })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
  });

  it.each(["missing", "relative", "directory", "oversize", "invalid JSON", "invalid UTF-8"])("rejects a %s descriptor file", async kind => {
    const remote = await gateway();
    let path = join(fixture, "descriptor.json");
    if (kind === "relative") path = "relative-descriptor.json";
    else if (kind === "directory") await mkdir(path);
    else if (kind !== "missing") await writeFile(path, kind === "oversize" ? " ".repeat(4097) : kind === "invalid UTF-8" ? Buffer.from([0xff]) : "{");
    const port = createExternalExecutionHttpPort({ descriptorPath: path })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
  });

  it("rejects a descriptor symlink", async ({ skip }) => {
    const remote = await gateway();
    const target = await descriptor(remote.baseUrl);
    const alias = join(fixture, "descriptor-link.json");
    try { await symlink(target, alias, "file"); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EPERM") { skip(); return; }
      throw error;
    }
    const port = createExternalExecutionHttpPort({ descriptorPath: alias })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
  });

  it("rejects a descriptor reached through a parent symlink or Windows junction", async () => {
    const remote = await gateway();
    await descriptor(remote.baseUrl);
    const alias = join(fixture, "linked-parent");
    await symlink(fixture, alias, process.platform === "win32" ? "junction" : "dir");
    const port = createExternalExecutionHttpPort({ descriptorPath: join(alias, "descriptor.json") })!;
    await expect(port.recover(run)).rejects.toThrow("EXTERNAL_RUNTIME_UNAVAILABLE");
    expect(remote.requests).toHaveLength(0);
    // Remove the link itself before fixture cleanup; never traverse its target.
    await rm(alias);
  });
});

describe("core observer port assembly", () => {
  it.each([
    { heartbeatSchedulerEnabled: true, configured: true }, { heartbeatSchedulerEnabled: false, configured: true },
    { heartbeatSchedulerEnabled: true, configured: false }, { heartbeatSchedulerEnabled: false, configured: false },
  ])("uses one observer in schedulerEnabled=$heartbeatSchedulerEnabled, configured=$configured", async ({ heartbeatSchedulerEnabled, configured }) => {
    // Evaluate only the real assembly statements; importing index boots unrelated providers and the server.
    const text = await readFile(new URL("../index.ts", import.meta.url), "utf8");
    const source = stripTypeScriptTypes(text);
    const statements = new Map<string, string>();
    for (const name of ["externalExecution", "heartbeat", "environmentLeaseCleanupHeartbeat"]) {
      const matches = [...source.matchAll(new RegExp(`^\\s*const ${name}\\s*=[\\s\\S]*?;`, "gm"))];
      expect(matches).toHaveLength(1);
      statements.set(name, matches[0][0]);
    }
    expect(source).toMatch(/import\s*\{\s*createExternalExecutionHttpPort\s*\}\s*from\s*"\.\/services\/external-execution-http\.js"/);
    const port = { recover: vi.fn(), cancel: vi.fn(), drain: vi.fn() };
    const createPort = vi.fn(() => configured ? port : undefined);
    const heartbeatInstance = {};
    const heartbeatService = vi.fn(() => heartbeatInstance);
    const db = {};
    const pluginWorkerManager = {};
    const assembly = [
      statements.get("externalExecution"), statements.get("heartbeat"), statements.get("environmentLeaseCleanupHeartbeat"),
      "({ heartbeat, environmentLeaseCleanupHeartbeat });",
    ].join("\n");
    const result = runInNewContext(assembly, { config: { heartbeatSchedulerEnabled }, db, pluginWorkerManager, heartbeatService, createExternalExecutionHttpPort: createPort });
    expect(createPort).toHaveBeenCalledOnce();
    expect(heartbeatService).toHaveBeenCalledExactlyOnceWith(db, { pluginWorkerManager, externalExecution: configured ? port : undefined });
    expect(result.heartbeat).toBe(heartbeatSchedulerEnabled ? heartbeatInstance : null);
    expect(result.environmentLeaseCleanupHeartbeat).toBe(heartbeatInstance);
  });
});
