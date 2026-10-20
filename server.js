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
  res.send('Signalling server is running');
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

io.on('connection', (socket) => {
  console.log(`connected: ${socket.id}`);

  // New arrival joins the queue (and is paired immediately if someone waits).
  enqueue(socket);

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
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Signalling server listening on port ${PORT}`);
});
