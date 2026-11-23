import http from "node:http";

const PORT = Number(process.env.PORT || 8787);
const OLLAMA_URL = (process.env.OLLAMA_URL || "http://127.0.0.1:11434").replace(/\/$/, "");
const OLLAMA_MODEL = process.env.OLLAMA_MODEL || "llama3.2";
const ALLOWED_ORIGINS = new Set(
  (process.env.ALLOWED_ORIGINS || "http://localhost:8080")
    .split(",")
    .map((origin) => origin.trim())
    .filter(Boolean),
);
const MAX_BODY_BYTES = 10 * 1024 * 1024;

const encodePacked = (text) => ({ p: Buffer.from(text, "utf8").toString("base64url") });
const frame = (text) => `data: ${JSON.stringify(encodePacked(text))}\n\n`;

function decodePacked(body) {
  if (!body || typeof body.p !== "string") throw Object.assign(new Error("Malformed request"), { status: 400 });
  try {
    return JSON.parse(Buffer.from(body.p, "base64url").toString("utf8"));
  } catch {
    throw Object.assign(new Error("Malformed request"), { status: 400 });
  }
}

function applyCors(req, res) {
  res.setHeader("Vary", "Origin");
  const origin = req.headers.origin;
  if (!origin || !ALLOWED_ORIGINS.has(origin)) return;
  res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Credentials", "true");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader(
    "Access-Control-Allow-Headers",
    req.headers["access-control-request-headers"] || "content-type",
  );
}

function sendJson(res, status, value) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(value));
}

async function readJson(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) throw Object.assign(new Error("Request too large"), { status: 413 });
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw Object.assign(new Error("Malformed request"), { status: 400 });
  }
}

async function handleChat(req, res) {
  const body = decodePacked(await readJson(req));
  const messages = [];
  if (typeof body.system_prompt === "string" && body.system_prompt.trim()) {
    messages.push({ role: "system", content: body.system_prompt.trim() });
  }
  messages.push(...(Array.isArray(body.messages) ? body.messages : []));

  const controller = new AbortController();
  res.on("close", () => controller.abort());

  let upstream;
  try {
    upstream = await fetch(`${OLLAMA_URL}/v1/chat/completions`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ model: OLLAMA_MODEL, messages, stream: true }),
      signal: controller.signal,
    });
  } catch {
    return sendJson(res, 502, { error: "The self-hosted model server is unreachable." });
  }
  if (!upstream.ok || !upstream.body) {
    const detail = (await upstream.text().catch(() => "")).slice(0, 200);
    return sendJson(res, 502, { error: `Model server error (${upstream.status}). ${detail}`.trim() });
  }

  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
  });

  const reader = upstream.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      let newline;
      while ((newline = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (!line.startsWith("data:")) continue;
        const data = line.slice(5).trim();
        if (!data) continue;
        if (data === "[DONE]") {
          res.end(frame("[DONE]"));
          return;
        }
        res.write(frame(data));
      }
    }
    res.end(frame("[DONE]"));
  } catch {
    if (!res.writableEnded) {
      res.end(frame(JSON.stringify({ error: "Generation was interrupted." })));
    }
  }
}

async function handleTitle(req, res) {
  const body = decodePacked(await readJson(req));
  const message = typeof body.message === "string" ? body.message : "";
  const title = message.replace(/\s+/g, " ").trim().slice(0, 60) || "New chat";
  sendJson(res, 200, encodePacked(JSON.stringify({ title })));
}

const server = http.createServer(async (req, res) => {
  applyCors(req, res);
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    return res.end();
  }
  try {
    if (req.method === "POST" && req.url === "/api/ai/chat") return await handleChat(req, res);
    if (req.method === "POST" && req.url === "/api/ai/title") return await handleTitle(req, res);
    sendJson(res, 404, { error: "Not found" });
  } catch (error) {
    if (res.headersSent) return res.end();
    sendJson(res, error.status || 500, { error: error.status ? error.message : "Internal error" });
  }
});

server.listen(PORT, () => {
  console.log(`AI adapter listening on :${PORT} -> ${OLLAMA_URL} (model ${OLLAMA_MODEL})`);
});
