/**
 * ============================================================
 *  TikTok → Roblox Live Bridge Server  —  v2 (Rosa OBS update)
 *  Stack: Node.js + Express + tiktok-live-connector
 * ============================================================
 *
 *  WHAT'S NEW vs original:
 *    ✅ Emits a local "rosa_gift" event so obs-music-switcher.js
 *       can auto-change your OBS music when Rosa is gifted
 *    ✅ Everything else is identical to your working original
 *
 *  SETUP:
 *    1. Replace your old server.js with this file on Railway
 *    2. Run obs-music-switcher.js LOCALLY on your laptop (not Railway)
 *       because it needs to talk to OBS which is on your computer
 *
 *  .env variables (same as before — nothing new required):
 *    TIKTOK_USERNAME=your_tiktok_username
 *    PORT=3000
 *    API_SECRET=your_secret_key_here
 */

require("dotenv").config();
const express    = require("express");
const cors       = require("cors");
const { EventEmitter } = require("events");
const { WebcastPushConnection } = require("tiktok-live-connector");

const app    = express();
const events = new EventEmitter();   // ← internal event bus (Rosa trigger)

app.use(cors());
app.use(express.json());

// ─── Configuration ────────────────────────────────────────────────────────────
const TIKTOK_USERNAME = process.env.TIKTOK_USERNAME || "YOUR_TIKTOK_USERNAME";
const API_SECRET      = process.env.API_SECRET      || "CHANGE_THIS_SECRET";
const PORT            = process.env.PORT            || 3000;
const EVENT_TTL_MS    = 60_000;

// ─── In-Memory Event Queue ─────────────────────────────────────────────────────
let eventQueue    = [];
let eventIdCounter = 0;

function nextId() {
  return `evt_${Date.now()}_${++eventIdCounter}`;
}

function pruneOldEvents() {
  const cutoff = Date.now() - EVENT_TTL_MS;
  eventQueue   = eventQueue.filter((e) => e.timestamp > cutoff);
}

function enqueue(event) {
  pruneOldEvents();
  eventQueue.push({ ...event, id: nextId(), timestamp: Date.now() });
  console.log(`[QUEUE] +${event.type} | roblox:"${event.robloxUser}" | queue_size:${eventQueue.length}`);

  // ── Rosa gift → fire internal event so obs-music-switcher.js reacts ──────
  if (event.type === "gift" && event.giftName === "Rosa") {
    console.log(`[OBS] 🌸 Rosa gift detected from @${event.tiktokUser} — firing music change event`);
    events.emit("rosa_gift", {
      tiktokUser: event.tiktokUser,
      robloxUser: event.robloxUser,
      giftCount:  event.giftCount || 1,
    });
  }
}

// ─── Roblox Username Extractor ─────────────────────────────────────────────────
const ROBLOX_USERNAME_REGEX =
  /^(?!.*__)[a-zA-Z0-9][a-zA-Z0-9_]{1,18}[a-zA-Z0-9]$|^[a-zA-Z0-9]{3,20}$/;

function extractRobloxUsername(comment) {
  if (!comment || typeof comment !== "string") return null;
  const trimmed = comment.trim();
  if (ROBLOX_USERNAME_REGEX.test(trimmed)) return trimmed;
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
  if (tiktokConnection) tiktokConnection.disconnect();

  console.log(`[TIKTOK] Connecting to @${TIKTOK_USERNAME} ...`);
  tiktokConnection = new WebcastPushConnection(TIKTOK_USERNAME, {
    processInitialData:    false,
    enableExtendedGiftInfo: true,
  });

  tiktokConnection.on("chat", (data) => {
    const comment    = data.comment  || "";
    const tiktokUser = data.uniqueId || data.nickname || "unknown";
    const robloxUser = extractRobloxUsername(comment);
    if (!robloxUser) return;
    enqueue({ type: "chat", tiktokUser, robloxUser });
  });

  tiktokConnection.on("follow", (data) => {
    const tiktokUser = data.uniqueId || data.nickname || "unknown";
    enqueue({ type: "follow", tiktokUser, robloxUser: tiktokUser });
  });

  tiktokConnection.on("gift", (data) => {
    if (data.giftType === 1 && !data.repeatEnd) return;
    const tiktokUser = data.uniqueId    || data.nickname   || "unknown";
    const giftName   = data.giftName    || data.describe   || "Gift";
    const giftCount  = data.repeatCount || 1;
    enqueue({ type: "gift", tiktokUser, robloxUser: tiktokUser, giftName, giftCount });
  });

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

// ─── Middleware ────────────────────────────────────────────────────────────────
function requireSecret(req, res, next) {
  if (req.headers["x-api-secret"] !== API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get("/events", requireSecret, (req, res) => {
  pruneOldEvents();
  const limit = Math.min(parseInt(req.query.limit) || 10, 50);
  const batch = eventQueue.splice(0, limit);
  res.json({ events: batch, remaining: eventQueue.length });
});

app.get("/status", (req, res) => {
  res.json({
    status:     "ok",
    tiktok:     connectionStatus,
    queueSize:  eventQueue.length,
    uptime:     Math.floor(process.uptime()),
    tiktokUser: TIKTOK_USERNAME,
  });
});

app.post("/test-event", requireSecret, (req, res) => {
  const { type = "chat", robloxUser = "Builderman", giftName = "TestGift", giftCount = 1 } = req.body;
  const tiktokUser = req.body.tiktokUser || robloxUser;
  enqueue({ type, tiktokUser, robloxUser, giftName, giftCount });
  res.json({ ok: true, queueSize: eventQueue.length });
});

// ─── Rosa SSE endpoint (obs-music-switcher.js connects here) ──────────────────
// SSE = Server-Sent Events. The switcher keeps this connection open and
// receives a message the moment Rosa is gifted.
app.get("/rosa-stream", requireSecret, (req, res) => {
  res.setHeader("Content-Type",  "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection",    "keep-alive");
  res.flushHeaders();

  console.log("[OBS] obs-music-switcher connected to /rosa-stream ✅");

const onRosa = (data) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  events.on("rosa_gift", onRosa);

  // Heartbeat every 20 seconds — stops Railway from killing the connection
  const heartbeat = setInterval(() => {
    res.write(`: heartbeat\n\n`);
  }, 20_000);

  req.on("close", () => {
    clearInterval(heartbeat);
    events.off("rosa_gift", onRosa);
    console.log("[OBS] obs-music-switcher disconnected from /rosa-stream");
  });

// ─── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n🚀  Bridge server listening on http://localhost:${PORT}`);
  console.log(`🔑  API Secret: ${API_SECRET}`);
  console.log(`🎮  Poll endpoint: GET /events  (used by Roblox)`);
  console.log(`🌸  Rosa SSE:      GET /rosa-stream  (used by obs-music-switcher.js)\n`);
  connectToTikTok();
});

// Export event emitter so obs-music-switcher can import it when running locally
module.exports = { events };
