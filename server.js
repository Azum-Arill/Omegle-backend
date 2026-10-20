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

// Simple health check.
app.get('/', (req, res) => {
  res.json({ status: 'running', online: io.engine.clientsCount });
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
const RATE_MAX_CONNECTIONS = 10;
const RATE_BAN_MS = 5 * 60 * 1000;

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
    return next(new Error('blocked'));
  }
  if (isRateLimited(ip, Date.now())) {
    return next(new Error('rate limited'));
  }
  next();
});

// Socket id of a user currently waiting for a partner, or null if nobody waits.
let waiting = null;

// Map of socket.id -> partner socket.id for everyone currently paired.
const partners = new Map();

function pair(a, b) {
  const room = `${a.id}#${b.id}`;
  a.join(room);
  b.join(room);

  partners.set(a.id, b.id);
  partners.set(b.id, a.id);

  // One side is the designated initiator (creates the WebRTC offer).
  a.emit('matched', { room, initiator: true });
  b.emit('matched', { room, initiator: false });
}

function leavePartner(socket, reason) {
  const partnerId = partners.get(socket.id);
  if (!partnerId) return;

  // Tear down the pairing on both sides.
  partners.delete(socket.id);
  partners.delete(partnerId);

  const partner = io.sockets.sockets.get(partnerId);
  if (partner) {
    partner.emit('partner-left', { reason });
  }
}

function enqueue(socket) {
  // Don't let a socket queue twice.
  if (waiting === socket.id) return;

  if (waiting && waiting !== socket.id) {
    const other = io.sockets.sockets.get(waiting);
    waiting = null;

    // The waiting socket may have vanished; if so, this socket waits instead.
    if (other && other.connected) {
      pair(other, socket);
      return;
    }
  }

  waiting = socket.id;
}

function broadcastOnlineCount() {
  io.emit('online-count', io.engine.clientsCount);
}

io.on('connection', (socket) => {
  console.log(`connected: ${socket.id}`);

  // Every connection counts as online, but only joins the queue on 'join'.
  broadcastOnlineCount();

  // Client pressed Start: enter the queue (ignored if already queued or paired).
  socket.on('join', () => {
    if (partners.has(socket.id)) return;
    enqueue(socket);
  });

  // Client pressed Stop: leave the pairing and the queue, keep the connection.
  socket.on('leave', () => {
    leavePartner(socket, 'leave');
    if (waiting === socket.id) waiting = null;
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
    if (partner) partner.emit(event, payload);
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

    // Clear the queue slot if this socket was the one waiting.
    if (waiting === socket.id) waiting = null;
    broadcastOnlineCount();
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Signalling server listening on port ${PORT}`);
});
