import { constants } from "node:fs";
import { lstat, open, realpath } from "node:fs/promises";
import { isAbsolute, resolve } from "node:path";
import { z } from "zod";
import { isExternalExecutionRun, type ExternalExecutionPort, type ExternalExecutionRun } from "./external-execution.js";

const MAX_DESCRIPTOR_BYTES = 4096;
const MAX_RESPONSE_BYTES = 2048;
const Descriptor = z.strictObject({
  baseUrl: z.string().min(1).max(2048),
  secret: z.string().regex(/^[A-Za-z0-9_-]{43}$/),
});
const Identity = z.object({ companyId: z.string().uuid(), id: z.string().uuid() });
const Accepted = z.strictObject({
  accepted: z.literal(true), companyId: z.string().uuid(), runId: z.string().uuid(),
  action: z.enum(["recover", "cancel", "drain"]), retained: z.literal(true),
});
const unavailable = () => new Error("EXTERNAL_RUNTIME_UNAVAILABLE");
const pathKey = (path: string) => process.platform === "win32" ? path.toLowerCase() : path;

async function readDescriptor(path: string) {
  if (!isAbsolute(path)) throw unavailable();
  const before = await lstat(path, { bigint: true });
  if (!before.isFile() || before.isSymbolicLink() || before.size > BigInt(MAX_DESCRIPTOR_BYTES) ||
      pathKey(await realpath(path)) !== pathKey(resolve(path))) throw unavailable();
  const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
  try {
    const opened = await file.stat({ bigint: true });
    if (!opened.isFile() || opened.dev !== before.dev || opened.ino !== before.ino || opened.size > BigInt(MAX_DESCRIPTOR_BYTES)) throw unavailable();
    const bytes = Buffer.alloc(MAX_DESCRIPTOR_BYTES + 1);
    let length = 0;
    while (length < bytes.length) {
      const { bytesRead } = await file.read(bytes, length, bytes.length - length, length);
      if (!bytesRead) break;
      length += bytesRead;
    }
    const after = await lstat(path, { bigint: true });
    if (length > MAX_DESCRIPTOR_BYTES || !after.isFile() || after.isSymbolicLink() ||
        after.dev !== opened.dev || after.ino !== opened.ino ||
        pathKey(await realpath(path)) !== pathKey(resolve(path))) throw unavailable();
    const descriptor = Descriptor.parse(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length))));
    const base = new URL(descriptor.baseUrl);
    if (base.protocol !== "http:" || base.hostname !== "127.0.0.1" || !base.port ||
        base.username || base.password || base.pathname !== "/" || base.search || base.hash ||
        ![base.origin, `${base.origin}/`].includes(descriptor.baseUrl)) throw unavailable();
    return { ...descriptor, base };
  } finally {
    await file.close();
  }
}

async function readAcceptance(response: Response) {
  if (!response.ok || !/^application\/json(?:;\s*charset=utf-8)?$/i.test(response.headers.get("content-type") ?? "") ||
      Number(response.headers.get("content-length")) > MAX_RESPONSE_BYTES) {
    await response.body?.cancel().catch(() => {});
    throw unavailable();
  }
  const reader = response.body?.getReader();
  if (!reader) throw unavailable();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.byteLength;
      if (length > MAX_RESPONSE_BYTES) throw unavailable();
      chunks.push(value);
    }
    return Accepted.parse(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(chunks, length))));
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

/** Acknowledgement retains execution; it proves neither observer attachment nor a stopped provider. */
export function createExternalExecutionHttpPort(options: {
  descriptorPath?: string;
  requestTimeoutMs?: number;
} = {}): ExternalExecutionPort | undefined {
  const path = options.descriptorPath ?? process.env.HIVE_PAPERCLIP_EXTERNAL_EXECUTION_DESCRIPTOR;
  if (!path) return undefined;
  const timeoutMs = options.requestTimeoutMs ?? 5000;
  const send = async (run: ExternalExecutionRun, action: "recover" | "cancel" | "drain") => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      if (!isExternalExecutionRun(run) || !Number.isInteger(timeoutMs) || timeoutMs < 10 || timeoutMs > 10_000) throw unavailable();
      const identity = Identity.parse(run);
      const deadline = new AbortController();
      const timeout = new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => { deadline.abort(); reject(unavailable()); }, timeoutMs);
      });
      const control = (async () => {
        const { base, secret } = await readDescriptor(path);
        // A descriptor read that outlives the deadline must never issue a later control request.
        deadline.signal.throwIfAborted();
        const response = await fetch(new URL(`/hive/external-execution/${identity.companyId}/${identity.id}`, base), {
          method: "POST", redirect: "error", signal: deadline.signal,
          headers: { Authorization: `Bearer ${secret}`, "Content-Type": "application/json" },
          body: JSON.stringify({ action }),
        });
        const accepted = await readAcceptance(response);
        if (accepted.companyId !== identity.companyId || accepted.runId !== identity.id || accepted.action !== action) throw unavailable();
      })();
      await Promise.race([control, timeout]);
    } catch {
      // Never attach descriptor contents or a remote response to an error.
      throw unavailable();
    } finally {
      clearTimeout(timer);
    }
  };
  return {
    recover: run => send(run, "recover"),
    cancel: run => send(run, "cancel"),
    drain: run => send(run, "drain"),
  };
}
