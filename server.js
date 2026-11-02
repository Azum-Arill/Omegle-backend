const express = require('express');
const http = require('http');
const cors = require('cors');
const { Server } = require('socket.io');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

// Client IP: first x-forwarded-for value, falling back to the socket address.
function getIp(socket) {
  const forwarded = socket.handshake.headers['x-forwarded-for'];
  const first = typeof forwarded === 'string' ? forwarded.split(',')[0].trim() : '';
  return first || socket.handshake.address;
}

const BLOCKED_IPS = new Set(
  (process.env.BLOCKED_IPS || '')
    .split(',')
    .map((ip) => ip.trim())
    .filter(Boolean)
);

const RATE_WINDOW_MS = 60 * 1000;
const RATE_MAX_CONNECTIONS = 30;
const RATE_BAN_MS = 60 * 1000;

// After two people part ways they will not be matched with each other again for this long.
// Reported pairs are kept apart for much longer. Both can be overridden with env vars.
const REMATCH_COOLDOWN_MS = Number(process.env.REMATCH_COOLDOWN_MS) || 30 * 1000;
const REPORT_COOLDOWN_MS = Number(process.env.REPORT_COOLDOWN_MS) || 10 * 60 * 1000;

// ip -> { times: connection timestamps within the window, bannedUntil }
const connectionLog = new Map();

function isRateLimited(ip, now) {
  let entry = connectionLog.get(ip);
  if (!entry) {
    entry = { times: [], bannedUntil: 0 };
    connectionLog.set(ip, entry);
  }
  if (entry.bannedUntil > now) return true;

  entry.times = entry.times.filter((t) => now - t < RATE_WINDOW_MS);
  entry.times.push(now);
  if (entry.times.length > RATE_MAX_CONNECTIONS) {
    entry.bannedUntil = now + RATE_BAN_MS;
    entry.times = [];
    return true;
  }
  return false;
}

// Drop entries with no recent connections and no active ban so memory stays bounded.
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of connectionLog) {
    const idle = entry.times.every((t) => now - t >= RATE_WINDOW_MS);
    if (idle && entry.bannedUntil <= now) connectionLog.delete(ip);
  }
}, RATE_WINDOW_MS).unref();

io.use((socket, next) => {
  const ip = getIp(socket);
  if (BLOCKED_IPS.has(ip)) {
    console.log(`refused: reason=blocklist ip=${ip}`);
    return next(new Error('blocked'));
  }
  if (isRateLimited(ip, Date.now())) {
    console.log(`refused: reason=rate limit ip=${ip}`);
    return next(new Error('rate limited'));
  }
  next();
});

// Socket ids waiting for a partner, oldest first.
let queue = [];

// Map of socket.id -> partner socket.id for everyone currently paired.
const partners = new Map();

// Pair key -> number of ice-candidates relayed so far (cleared when the pair ends).
const iceCounts = new Map();

// socket.id -> { id, until }: the last partner this socket must not be re-matched with yet.
const noRematch = new Map();

function pairKey(x, y) {
  return x < y ? `${x}|${y}` : `${y}|${x}`;
}

function blockRematch(aId, bId, ms) {
  const until = Date.now() + ms;
  noRematch.set(aId, { id: bId, until });
  noRematch.set(bId, { id: aId, until });
}

function canPair(aId, bId, now) {
  const x = noRematch.get(aId);
  if (x && x.id === bId && x.until > now) return false;
  const y = noRematch.get(bId);
  if (y && y.id === aId && y.until > now) return false;
  return true;
}

function pair(a, b) {
  const room = `${a.id}#${b.id}`;
  a.join(room);
  b.join(room);

  partners.set(a.id, b.id);
  partners.set(b.id, a.id);

  // The person who has waited longest is the initiator (creates the WebRTC offer).
  console.log(`paired: ${a.id} (initiator) <-> ${b.id}`);
  a.emit('matched', { room, initiator: true });
  b.emit('matched', { room, initiator: false });
}

// Pair up everyone in the queue who can be paired. Safe to call at any time.
function tryMatch() {
  const now = Date.now();

  // Forget sockets that have gone away or are already paired.
  queue = queue.filter((id) => {
    const s = io.sockets.sockets.get(id);
    return s && s.connected && !partners.has(id);
  });

  for (let i = 0; i < queue.length; i++) {
    for (let j = i + 1; j < queue.length; j++) {
      if (!canPair(queue[i], queue[j], now)) continue;
      const a = io.sockets.sockets.get(queue[i]);
      const b = io.sockets.sockets.get(queue[j]);
      queue.splice(j, 1);
      queue.splice(i, 1);
      pair(a, b);
      i--; // the next waiting socket has moved into slot i
      break;
    }
  }
}

function enqueue(socket) {
  // Ignore sockets that are already queued or already paired.
  if (partners.has(socket.id) || queue.includes(socket.id)) return;
  queue.push(socket.id);
  console.log(`queued: ${socket.id} (waiting=${queue.length})`);
  tryMatch();
}

function dequeue(id) {
  queue = queue.filter((x) => x !== id);
}

// Break the pairing of `socket`. The remaining partner is told and goes back in the queue.
function leavePartner(socket, reason) {
  const partnerId = partners.get(socket.id);
  if (!partnerId) return;

  partners.delete(socket.id);
  partners.delete(partnerId);
  iceCounts.delete(pairKey(socket.id, partnerId));
  blockRematch(socket.id, partnerId, reason === 'reported' ? REPORT_COOLDOWN_MS : REMATCH_COOLDOWN_MS);
  console.log(`partner-left: ${partnerId} told by ${socket.id} reason=${reason}`);

  const partner = io.sockets.sockets.get(partnerId);
  if (partner) {
    partner.emit('partner-left', { reason });
    // They still want someone to talk to, so they keep waiting.
    enqueue(partner);
  }
}

// Simple health check, with a few live numbers.
app.get('/', (req, res) => {
  res.json({
    status: 'running',
    online: io.engine.clientsCount,
    waiting: queue.length,
    inCalls: partners.size,
  });
});

function broadcastOnlineCount() {
  io.emit('online-count', io.engine.clientsCount);
}

// Re-check the queue now and then so cooldowns that expire lead to matches.
setInterval(tryMatch, 2000).unref();

io.on('connection', (socket) => {
  console.log(`connected: ${socket.id}`);

  // Every connection counts as online, but only joins the queue on 'join'.
  broadcastOnlineCount();

  // Client pressed Start: enter the queue (ignored if already queued or paired).
  socket.on('join', () => {
    enqueue(socket);
  });

  // Client pressed Stop: leave the pairing and the queue, keep the connection.
  socket.on('leave', () => {
    leavePartner(socket, 'leave');
    dequeue(socket.id);
  });

  // Log the report, drop the pairing, and requeue the reporter.
  socket.on('report', (payload) => {
    const partnerId = partners.get(socket.id);
    if (!partnerId) return;
    const reason = typeof payload?.reason === 'string' ? payload.reason.slice(0, 50) : 'Other';
    const partnerSocket = io.sockets.sockets.get(partnerId);
    const partnerIp = partnerSocket ? getIp(partnerSocket) : 'unknown';
    console.log(`report: ${socket.id} reported ${partnerId} ip=${partnerIp} (${reason})`);
    leavePartner(socket, 'reported');
    enqueue(socket);
  });

  // Relay WebRTC signalling verbatim to the current partner.
  const forward = (event) => (payload) => {
    const partnerId = partners.get(socket.id);
    if (!partnerId) return;
    const partner = io.sockets.sockets.get(partnerId);
    if (!partner) return;
    if (event === 'ice-candidate') {
      const key = pairKey(socket.id, partnerId);
      const count = (iceCounts.get(key) || 0) + 1;
      iceCounts.set(key, count);
      console.log(`relay: ice-candidate from=${socket.id} to=${partnerId} count=${count}`);
    } else {
      console.log(`relay: ${event} from=${socket.id} to=${partnerId}`);
    }
    partner.emit(event, payload);
  };

  socket.on('offer', forward('offer'));
  socket.on('answer', forward('answer'));
  socket.on('ice-candidate', forward('ice-candidate'));

  // User wants a new partner: drop the current one, then requeue.
  socket.on('next', () => {
    leavePartner(socket, 'next');
    enqueue(socket);
  });

  socket.on('disconnect', () => {
    console.log(`disconnected: ${socket.id}`);
    leavePartner(socket, 'disconnect');
    dequeue(socket.id);
    noRematch.delete(socket.id);
    broadcastOnlineCount();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Signalling server listening on port ${PORT}`);
});
