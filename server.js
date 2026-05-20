/**
 * ============================================================
 *  TikTok → Roblox Live Bridge Server
 *  Stack: Node.js + Express + tiktok-live-connector
 * ============================================================
 *
 *  Install dependencies first:
 *    npm install tiktok-live-connector express cors dotenv
 *
 *  Environment variables (.env):
 *    TIKTOK_USERNAME=your_tiktok_username
 *    PORT=3000
 *    API_SECRET=your_secret_key_here   ← Roblox will send this header
 */

require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { WebcastPushConnection } = require("tiktok-live-connector");

const app = express();
app.use(cors());
app.use(express.json());

// ─── Configuration ────────────────────────────────────────────────────────────
const TIKTOK_USERNAME = process.env.TIKTOK_USERNAME || "YOUR_TIKTOK_USERNAME";
const API_SECRET      = process.env.API_SECRET      || "CHANGE_THIS_SECRET";
const PORT            = process.env.PORT            || 3000;

// How long (ms) the server keeps an event in the queue before auto-expiring it
const EVENT_TTL_MS = 60_000; // 60 seconds

// ─── In-Memory Event Queue ─────────────────────────────────────────────────────
/**
 * Each item in the queue:
 * {
 *   id:          string   – unique event ID
 *   type:        "chat" | "follow" | "gift"
 *   tiktokUser:  string   – TikTok display name
 *   robloxUser:  string   – Roblox username parsed from comment (or same as tiktokUser)
 *   giftName:    string?  – only present for "gift" events
 *   giftCount:   number?  – repeat count for gifts
 *   timestamp:   number   – unix ms
 * }
 */
let eventQueue = [];
let eventIdCounter = 0;

function nextId() {
  return `evt_${Date.now()}_${++eventIdCounter}`;
}

/** Remove events older than EVENT_TTL_MS */
function pruneOldEvents() {
  const cutoff = Date.now() - EVENT_TTL_MS;
  eventQueue = eventQueue.filter((e) => e.timestamp > cutoff);
}

/** Add an event, prune stale ones first */
function enqueue(event) {
  pruneOldEvents();
  eventQueue.push({ ...event, id: nextId(), timestamp: Date.now() });
  console.log(`[QUEUE] +${event.type} | roblox:"${event.robloxUser}" | queue_size:${eventQueue.length}`);
}

// ─── Roblox Username Extractor ─────────────────────────────────────────────────
/**
 * Roblox usernames: 3–20 chars, letters/numbers/underscores, no leading/trailing underscore.
 * We look for the FIRST valid-looking username token in a comment.
 * Viewers are instructed to comment exactly their Roblox username.
 */
const ROBLOX_USERNAME_REGEX = /^(?!.*__)[a-zA-Z0-9][a-zA-Z0-9_]{1,18}[a-zA-Z0-9]$|^[a-zA-Z0-9]{3,20}$/;

function extractRobloxUsername(comment) {
  if (!comment || typeof comment !== "string") return null;
  // Try the whole trimmed comment first (most common case)
  const trimmed = comment.trim();
  if (ROBLOX_USERNAME_REGEX.test(trimmed)) return trimmed;

  // Otherwise look for a word token that matches
  const tokens = trimmed.split(/\s+/);
  for (const token of tokens) {
    if (ROBLOX_USERNAME_REGEX.test(token)) return token;
  }
  return null;
}

// ─── TikTok Live Connection ────────────────────────────────────────────────────
let tiktokConnection = null;
let connectionStatus = "disconnected";

function connectToTikTok() {
  if (tiktokConnection) {
    tiktokConnection.disconnect();
  }

  console.log(`[TIKTOK] Connecting to @${TIKTOK_USERNAME} ...`);
  tiktokConnection = new WebcastPushConnection(TIKTOK_USERNAME, {
    processInitialData: false,   // skip backlog of old messages
    enableExtendedGiftInfo: true // get full gift metadata
  });

  // ── Chat Event ──────────────────────────────────────────────────────────────
  tiktokConnection.on("chat", (data) => {
    const comment     = data.comment   || "";
    const tiktokUser  = data.uniqueId  || data.nickname || "unknown";
    const robloxUser  = extractRobloxUsername(comment);

    if (!robloxUser) return; // comment doesn't look like a Roblox username

    enqueue({ type: "chat", tiktokUser, robloxUser });
  });

  // ── Follow Event ─────────────────────────────────────────────────────────────
  tiktokConnection.on("follow", (data) => {
    const tiktokUser = data.uniqueId || data.nickname || "unknown";
    // Followers don't always comment a username simultaneously.
    // We store the follow — if they ALSO comment a username, the Roblox side
    // merges them via the "pendingFollowers" set (handled in Luau).
    enqueue({ type: "follow", tiktokUser, robloxUser: tiktokUser });
  });

  // ── Gift Event ───────────────────────────────────────────────────────────────
  tiktokConnection.on("gift", (data) => {
    // Only fire once per gift streak (when the streak ends)
    if (data.giftType === 1 && !data.repeatEnd) return;

    const tiktokUser = data.uniqueId   || data.nickname   || "unknown";
    const giftName   = data.giftName   || data.describe   || "Gift";
    const giftCount  = data.repeatCount || 1;

    enqueue({ type: "gift", tiktokUser, robloxUser: tiktokUser, giftName, giftCount });
  });

  // ── Connection lifecycle ──────────────────────────────────────────────────────
  tiktokConnection.connect()
    .then((state) => {
      connectionStatus = "connected";
      console.log(`[TIKTOK] Connected! RoomID: ${state.roomId}`);
    })
    .catch((err) => {
      connectionStatus = "error";
      console.error("[TIKTOK] Connection failed:", err.message);
      console.log("[TIKTOK] Retrying in 30 seconds...");
      setTimeout(connectToTikTok, 30_000);
    });

  tiktokConnection.on("disconnected", () => {
    connectionStatus = "disconnected";
    console.warn("[TIKTOK] Disconnected. Reconnecting in 15 seconds...");
    setTimeout(connectToTikTok, 15_000);
  });

  tiktokConnection.on("error", (err) => {
    console.error("[TIKTOK] Error:", err);
  });
}

// ─── Middleware: Validate Roblox requests ──────────────────────────────────────
function requireSecret(req, res, next) {
  const secret = req.headers["x-api-secret"];
  if (secret !== API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// ─── REST API Endpoints ────────────────────────────────────────────────────────

/**
 * GET /events
 * Roblox polls this every few seconds to fetch pending events.
 * Returns up to `limit` events (default 10) and removes them from the queue
 * so they aren't processed twice.
 *
 * Query params:
 *   ?limit=10   – max events to return per poll
 */
app.get("/events", requireSecret, (req, res) => {
  pruneOldEvents();
  const limit  = Math.min(parseInt(req.query.limit) || 10, 50);
  const batch  = eventQueue.splice(0, limit); // dequeue atomically
  res.json({ events: batch, remaining: eventQueue.length });
});

/**
 * GET /status
 * Health-check — lets you confirm the server is running and TikTok is connected.
 */
app.get("/status", (req, res) => {
  res.json({
    status:       "ok",
    tiktok:       connectionStatus,
    queueSize:    eventQueue.length,
    uptime:       Math.floor(process.uptime()),
    tiktokUser:   TIKTOK_USERNAME,
  });
});

/**
 * POST /test-event  (development only — remove or protect in production)
 * Manually inject a test event so you can test Roblox without a live TikTok stream.
 *
 * Body: { "type": "chat"|"follow"|"gift", "robloxUser": "Builderman", "giftName": "Rose" }
 */
app.post("/test-event", requireSecret, (req, res) => {
  const { type = "chat", robloxUser = "Builderman", giftName = "TestGift", giftCount = 1 } = req.body;
  const tiktokUser = req.body.tiktokUser || robloxUser;
  enqueue({ type, tiktokUser, robloxUser, giftName, giftCount });
  res.json({ ok: true, queueSize: eventQueue.length });
});

// ─── Start ─────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n🚀  Bridge server listening on http://localhost:${PORT}`);
  console.log(`🔑  API Secret: ${API_SECRET}`);
  console.log(`🎮  Poll endpoint: GET /events  (used by Roblox)\n`);
  connectToTikTok();
});
