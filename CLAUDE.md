# omegle-backend (signalling server)

## The product
A random video and text chat for students (starting with the BITS Design School network), like Omegle but with
accountability: users are anonymous to each other, verified to the platform, and only reveal contact details when
both press Connect. Tone: productive but fun. Not a professional-networking site at launch.
The make-or-break problem is density: random chat only works when many people are online at the same time,
so every feature must be judged by whether it helps people meet quickly. Launch plan: scheduled "Open Hour"
events inside closed communities.

## How the system fits together
- Frontend: Next.js on Vercel (separate repo, omegle-frontend). Video, audio and chat go browser to browser
  (WebRTC, with Metered TURN as a fallback). This server never touches video.
- This repo: a single server.js (Express + Socket.io) on Railway. It only does matching and signalling.
- Railway deploys automatically on every push to main. Pushing therefore changes the live site within about 2 minutes.
- All state (queue, pairs) is in memory. A restart or deploy clears the queue. Run a single replica only.

## Socket contract (do not change names without updating the frontend repo)
Client to server: join, leave, next, report {reason}, offer, answer, ice-candidate.
Server to client: matched {room, initiator}, partner-left {reason}, online-count (number), offer, answer, ice-candidate.
- A socket is only queued on 'join'. Connecting alone just counts as online.
- When a partner leaves (next, leave, disconnect, report) the remaining user is told and put back in the queue
  automatically. The client must not emit 'join' again.
- Two people who just parted are not re-matched for 30 seconds (10 minutes after a report).
- GET / returns {status, online, waiting, inCalls}.
- Connections are refused for blocked IPs (env BLOCKED_IPS) and for IPs opening more than 30 connections a minute.

## Roadmap (one phase at a time)
1. Experience (frontend work). 2. Verified students via college email code (Supabase Auth, token checked on connect).
3. Moderation: store reports in Supabase, account bans, terms and privacy pages, NSFW blur on the client.
4. Matching quality: interest tags as a soft preference, only when about 30 or more people are online.
5. Saved connections and direct messages. 6. Sponsored events and premium.

## Working rules
- The owner is a product designer with limited coding experience. Before changing files, show a short plan in
  plain English and wait for approval. After changing, explain what changed and how to test it.
- Never commit secrets (.env files, passwords, tokens). Read secrets from environment variables.
- Keep logs useful but never log message content or video details. IPs may be logged only for abuse handling.
- Test locally with `PORT=3456 node server.js` (PowerShell: `$env:PORT=3456; node server.js`) before suggesting a push.
- Do not commit or push unless asked. Use British English in any user-facing text.
