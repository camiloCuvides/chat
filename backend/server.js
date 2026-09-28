'use strict';
const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

// ---- Configuración (variables de entorno y límites) ----
const PORT = Number(process.env.PORT) || 3000;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',').map((s) => s.trim().replace(/\/+$/, '')).filter(Boolean);
const NAME_MIN = 2, NAME_MAX = 20;
const MSG_MAX = 500;
const RATE_MAX = 5, RATE_WINDOW_MS = 5000;   // 5 mensajes cada 5 s
const TYPING_MAX = 20;                        // eventos typing cada 5 s
const MAX_STRIKES = 5;                        // avisos de ritmo antes de cortar
const HISTORY_MAX = 50;
const PING_MS = 30000;
const JOIN_TIMEOUT_MS = 15000;
const MAX_PAYLOAD = 4096;

const log = (tag, ...a) => console.log(`[${tag}]`, new Date().toISOString(), ...a);
if (!ALLOWED_ORIGINS.length) log('aviso', 'ALLOWED_ORIGINS vacío: se rechazarán todas las conexiones');

// ---- Estado ----
const clients = new Map();   // ws -> { name, joined, stamps, typingStamps, strikes }
const history = [];

// ---- HTTP (solo health check) ----
const server = http.createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  res.writeHead(404); res.end();
});

const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_PAYLOAD });

const getIp = (req) =>
  (req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket.remoteAddress;

// ---- Handshake: aquí se valida el Origin ----
server.on('upgrade', (req, socket, head) => {
  const origin = (req.headers.origin || '').replace(/\/+$/, '');
  const ip = getIp(req);
  log('upgrade', `ip=${ip} origin=${origin || '(ninguno)'} url=${req.url}`);
  if (!origin || !ALLOWED_ORIGINS.includes(origin)) {
    log('upgrade', `RECHAZADO origin no permitido: ${origin || '(ninguno)'}`);
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n');
    return socket.destroy();
  }
  wss.handleUpgrade(req, socket, head, (ws) => {
    ws.meta = { ip, origin };
    wss.emit('connection', ws, req);
  });
});

// ---- Utilidades ----
function send(ws, obj) {
  if (ws.readyState !== WebSocket.OPEN) return;
  try { ws.send(JSON.stringify(obj)); } catch (e) { log('error', 'send falló', e.message); }
}
function broadcast(obj, except) {
  for (const [ws, c] of clients) if (c.joined && ws !== except) send(ws, obj);
}
const userList = () => [...clients.values()].filter((c) => c.joined).map((c) => c.name);
const sendError = (ws, code, message) => send(ws, { type: 'error', code, message });

function cleanName(v) {
  if (typeof v !== 'string') return '';
  return v.replace(/[\u0000-\u001f\u007f]/g, '').replace(/\s+/g, ' ').trim();
}
function allowed(list, max) {
  const now = Date.now();
  while (list.length && now - list[0] > RATE_WINDOW_MS) list.shift();
  if (list.length >= max) return false;
  list.push(now);
  return true;
}

// ---- Conexiones ----
wss.on('connection', (ws) => {
  const state = { name: null, joined: false, stamps: [], typingStamps: [], strikes: 0 };
  clients.set(ws, state);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  log('conexion', `abierta ip=${ws.meta.ip} total=${clients.size}`);

  const joinTimer = setTimeout(() => {
    if (!state.joined) ws.close(4001, 'join_timeout');
  }, JOIN_TIMEOUT_MS);

  ws.on('message', (data, isBinary) => {
    if (isBinary) return sendError(ws, 'bad_format', 'Solo se admite texto');
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return sendError(ws, 'bad_json', 'JSON inválido'); }
    if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') {
      return sendError(ws, 'bad_format', 'Formato inválido');
    }

    if (msg.type === 'join') {
      if (state.joined) return sendError(ws, 'already_joined', 'Ya estás dentro');
      const name = cleanName(msg.name);
      if (name.length < NAME_MIN || name.length > NAME_MAX) {
        return sendError(ws, 'invalid_name', `El nombre debe tener entre ${NAME_MIN} y ${NAME_MAX} caracteres`);
      }
      const taken = userList().some((n) => n.toLowerCase() === name.toLowerCase());
      if (taken) return sendError(ws, 'name_taken', 'Ese nombre ya está en uso');
      state.name = name;
      state.joined = true;
      clearTimeout(joinTimer);
      send(ws, { type: 'joined', name });
      send(ws, { type: 'history', messages: history });
      broadcast({ type: 'system', text: `${name} entró al chat`, ts: Date.now() }, ws);
      broadcast({ type: 'users', users: userList() });
      send(ws, { type: 'users', users: userList() });
      log('join', `${name} ip=${ws.meta.ip}`);
      return;
    }

    if (!state.joined) return sendError(ws, 'not_joined', 'Primero debes enviar join');

    if (msg.type === 'message') {
      if (typeof msg.text !== 'string') return sendError(ws, 'bad_format', 'text debe ser texto');
      const text = msg.text.trim();
      if (!text) return sendError(ws, 'empty', 'El mensaje está vacío');
      if (text.length > MSG_MAX) return sendError(ws, 'too_long', `Máximo ${MSG_MAX} caracteres`);
      if (!allowed(state.stamps, RATE_MAX)) {
        state.strikes += 1;
        if (state.strikes >= MAX_STRIKES) return ws.close(1008, 'rate_limit');
        return sendError(ws, 'rate_limit', 'Vas muy rápido, espera unos segundos');
      }
      const out = { type: 'message', name: state.name, text, ts: Date.now() };
      history.push(out);
      if (history.length > HISTORY_MAX) history.shift();
      broadcast(out);
      return;
    }

    if (msg.type === 'typing') {
      if (!allowed(state.typingStamps, TYPING_MAX)) return; // se ignora en silencio
      broadcast({ type: 'typing', name: state.name, isTyping: msg.isTyping === true }, ws);
      return;
    }

    sendError(ws, 'unknown_type', 'Tipo de mensaje desconocido');
  });

  ws.on('error', (e) => log('error', `ip=${ws.meta.ip} ${e.message}`));

  ws.on('close', (code, reason) => {
    clearTimeout(joinTimer);
    clients.delete(ws);
    log('cierre', `ip=${ws.meta.ip} origin=${ws.meta.origin} nombre=${state.name || '-'} code=${code} motivo=${reason.toString() || '-'} total=${clients.size}`);
    if (state.joined) {
      broadcast({ type: 'typing', name: state.name, isTyping: false });
      broadcast({ type: 'system', text: `${state.name} salió del chat`, ts: Date.now() });
      broadcast({ type: 'users', users: userList() });
    }
  });
});

// ---- Ping/pong: corta a quien no responde ----
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { log('ping', `sin pong, terminando ip=${ws.meta && ws.meta.ip}`); ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) { ws.terminate(); }
  }
}, PING_MS);

// ---- Robustez ----
process.on('uncaughtException', (e) => log('error', 'uncaughtException', e.stack || e));
process.on('unhandledRejection', (e) => log('error', 'unhandledRejection', e));
function shutdown() {
  log('sistema', 'apagando');
  clearInterval(heartbeat);
  for (const ws of wss.clients) ws.close(1001, 'server_shutdown');
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(PORT, '0.0.0.0', () => {
  log('sistema', `escuchando en ${PORT} origins=${ALLOWED_ORIGINS.join(',') || '(ninguno)'}`);
});