// Edge streaming gateway.
//
// Forwards arbitrary HTTP/1.1 + HTTP/2 traffic to a configured upstream
// service while preserving full bidirectional streaming. Used as a thin
// CDN-fronted entry point for backend APIs that need durable long-lived
// request/response bodies (server-sent events, chunked uploads, RPC-style
// duplex traffic, etc).
//
// Configure the upstream by setting the TARGET_DOMAIN environment variable
// to a fully-qualified origin URL, e.g. "https://api.example.com".

import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 60,
};

const UPSTREAM = (process.env.TARGET_DOMAIN || "").replace(/\/+$/, "");

// RFC 7230 hop-by-hop headers plus a few platform-injected ones that should
// not survive the proxy boundary. Forwarding these would either confuse the
// upstream parser or leak edge-internal metadata to the origin.
const HOP_BY_HOP = [
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
];

function buildUpstreamHeaders(incoming) {
  const out = new Headers();
  let realIp = null;

  for (const name of Object.keys(incoming)) {
    const lower = name.toLowerCase();
    const value = incoming[name];

    if (HOP_BY_HOP.includes(lower)) continue;
    // Drop edge-internal headers; they're noisy and can disclose region info.
    if (lower.startsWith("x-vercel-")) continue;

    // Collapse the various IP-forwarding headers into a single canonical
    // x-forwarded-for so the origin sees one value, not a chain.
    if (lower === "x-real-ip") { realIp = value; continue; }
    if (lower === "x-forwarded-for") { realIp ??= value; continue; }

    out.set(lower, Array.isArray(value) ? value.join(", ") : String(value));
  }

  if (realIp) out.set("x-forwarded-for", realIp);
  return out;
}

function relayResponseHeaders(from, to) {
  for (const [name, value] of from) {
    // Node manages transfer-encoding itself based on the outgoing stream.
    if (name.toLowerCase() === "transfer-encoding") continue;
    try {
      to.setHeader(name, value);
    } catch {
      // Skip headers Node refuses to set (e.g. malformed names from upstream).
    }
  }
}

export default async function handler(req, res) {
  if (!UPSTREAM) {
    res.statusCode = 500;
    res.end("Misconfigured: TARGET_DOMAIN is not set");
    return;
  }

  const targetUrl = UPSTREAM + req.url;
  const method = req.method;
  const carriesBody = method !== "GET" && method !== "HEAD";

  const init = {
    method,
    headers: buildUpstreamHeaders(req.headers),
    redirect: "manual",
  };

  if (carriesBody) {
    // half-duplex lets the request body keep flowing while the response
    // body is already being read back — required for long-lived sessions.
    init.body = Readable.toWeb(req);
    init.duplex = "half";
  }

  let upstream;
  try {
    upstream = await fetch(targetUrl, init);
  } catch (err) {
    console.error("upstream fetch failed:", err);
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway: upstream unreachable");
    }
    return;
  }

  res.statusCode = upstream.status;
  relayResponseHeaders(upstream.headers, res);

  if (!upstream.body) {
    res.end();
    return;
  }

  try {
    await pipeline(Readable.fromWeb(upstream.body), res);
  } catch (err) {
    console.error("response pipeline failed:", err);
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway: upstream stream failed");
    }
  }
}
