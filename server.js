require("dotenv").config();
const express    = require("express");
const cors       = require("cors");
const { EventEmitter } = require("events");
const { WebcastPushConnection } = require("tiktok-live-connector");

const app    = express();
const events = new EventEmitter();

app.use(cors());
app.use(express.json());

const TIKTOK_USERNAME = process.env.TIKTOK_USERNAME || "YOUR_TIKTOK_USERNAME";
const API_SECRET      = process.env.API_SECRET      || "CHANGE_THIS_SECRET";
const PORT            = process.env.PORT            || 3000;
const EVENT_TTL_MS    = 60_000;

let eventQueue     = [];
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

  if (event.type === "gift" && event.giftName === "Rosa") {
    console.log(`[OBS] Rosa gift from @${event.tiktokUser} — firing music change`);
    events.emit("rosa_gift", {
      tiktokUser: event.tiktokUser,
      robloxUser: event.robloxUser,
      giftCount:  event.giftCount || 1,
    });
  }
}

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

let tiktokConnection = null;
let connectionStatus = "disconnected";

function connectToTikTok() {
  if (tiktokConnection) tiktokConnection.disconnect();

  console.log(`[TIKTOK] Connecting to @${TIKTOK_USERNAME} ...`);
  tiktokConnection = new WebcastPushConnection(TIKTOK_USERNAME, {
    processInitialData:     false,
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

function requireSecret(req, res, next) {
  if (req.headers["x-api-secret"] !== API_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

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

app.get("/rosa-stream", requireSecret, (req, res) => {
  res.setHeader("Content-Type",  "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection",    "keep-alive");
  res.flushHeaders();

  console.log("[OBS] Music switcher connected to /rosa-stream");

  const onRosa = (data) => {
    res.write(`data: ${JSON.stringify(data)}\n\n`);
  };

  events.on("rosa_gift", onRosa);

  const heartbeat = setInterval(() => {
    res.write(`: heartbeat\n\n`);
  }, 20_000);

  req.on("close", () => {
    clearInterval(heartbeat);
    events.off("rosa_gift", onRosa);
    console.log("[OBS] Music switcher disconnected from /rosa-stream");
  });
});

app.listen(PORT, () => {
  console.log(`\n🚀  Bridge server listening on port ${PORT}`);
  console.log(`🌸  Rosa SSE: GET /rosa-stream\n`);
  connectToTikTok();
});
