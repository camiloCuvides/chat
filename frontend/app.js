'use strict';
const WS_URL = window.CHAT_CONFIG.WS_URL;
const $ = (id) => document.getElementById(id);
const el = { status: $('status'), login: $('login'), chat: $('chat'), nameInput: $('nameInput'),
  joinBtn: $('joinBtn'), loginError: $('loginError'), users: $('users'), userCount: $('userCount'),
  messages: $('messages'), typing: $('typing'), msgInput: $('msgInput'), sendBtn: $('sendBtn') };

let ws = null, myName = null, pendingName = null, joined = false, everJoined = false;
let attempts = 0, reconnectTimer = null, countdownTimer = null, nextRetryAt = 0;
let iAmTyping = false, typingTimer = null;
const typers = new Map(); // nombre -> timeout

// ---- Estado de la conexión, leído de readyState ----
const STATES = ['CONNECTING (0)', 'OPEN (1)', 'CLOSING (2)', 'CLOSED (3)'];
const CLASSES = ['s-connecting', 's-open', 's-closing', 's-closed'];
function renderStatus() {
  let text, cls;
  if (!ws) { text = 'Sin conexión'; cls = 's-closed'; }
  else { text = STATES[ws.readyState]; cls = CLASSES[ws.readyState]; }
  if (reconnectTimer && nextRetryAt) {
    text += ` · reintentando en ${Math.max(0, Math.ceil((nextRetryAt - Date.now()) / 1000))} s`;
  }
  el.status.textContent = text;
  el.status.className = 'status ' + cls;
  const open = !!ws && ws.readyState === WebSocket.OPEN;
  el.sendBtn.disabled = !(open && joined);
  el.joinBtn.disabled = !!ws && ws.readyState === WebSocket.CONNECTING;
}
setInterval(renderStatus, 500);

// ---- Conexión y reconexión con backoff ----
function connect() {
  clearTimeout(reconnectTimer); reconnectTimer = null; nextRetryAt = 0;
  try { ws = new WebSocket(WS_URL); } catch (e) { scheduleReconnect(); return; }
  const mine = ws;
  renderStatus();
  mine.onopen = () => { renderStatus(); send({ type: 'join', name: myName || pendingName }); };
  mine.onmessage = (ev) => { if (ws === mine) handle(ev.data); };
  mine.onerror = () => renderStatus();
  mine.onclose = () => {
    if (ws !== mine) return;
    joined = false; clearTypers(); renderStatus();
    if (everJoined || pendingName) scheduleReconnect();
  };
}
function scheduleReconnect() {
  if (reconnectTimer) return;
  const delay = Math.min(1000 * 2 ** attempts, 30000) + Math.random() * 500;
  attempts += 1;
  nextRetryAt = Date.now() + delay;
  reconnectTimer = setTimeout(connect, delay);
  renderStatus();
}
function send(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
}

// ---- Mensajes del servidor ----
function handle(raw) {
  let m; try { m = JSON.parse(raw); } catch { return; }
  switch (m.type) {
    case 'joined':
      joined = true; everJoined = true; myName = m.name; attempts = 0;
      el.login.classList.add('hidden'); el.chat.classList.remove('hidden');
      el.msgInput.focus(); renderStatus(); break;
    case 'history':
      el.messages.replaceChildren();
      (m.messages || []).forEach(addMessage); break;
    case 'users': renderUsers(m.users || []); break;
    case 'message': removeTyper(m.name); addMessage(m); break;
    case 'system': addSystem(m.text, false); break;
    case 'typing': m.isTyping ? addTyper(m.name) : removeTyper(m.name); break;
    case 'error': handleError(m); break;
  }
}
function handleError(m) {
  if (!joined) {
    if (everJoined) { ws.close(); return; }         // reconexión: se reintenta con backoff
    el.loginError.textContent = m.message;          // primer intento: se muestra en el login
    pendingName = null; return;
  }
  addSystem(m.message, true);
}

// ---- Pintar en pantalla (siempre con textContent: sin XSS) ----
function fmtTime(ts) { return new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }); }
function scrollDown(force) {
  const b = el.messages;
  if (force || b.scrollHeight - b.scrollTop - b.clientHeight < 80) b.scrollTop = b.scrollHeight;
}
function addMessage(m) {
  const div = document.createElement('div');
  div.className = 'msg' + (m.name === myName ? ' me' : '');
  const n = document.createElement('span'); n.className = 'n'; n.textContent = m.name;
  const t = document.createElement('span'); t.textContent = m.text;
  const h = document.createElement('span'); h.className = 't'; h.textContent = fmtTime(m.ts);
  div.append(n, t, h); el.messages.append(div); scrollDown(m.name === myName);
}
function addSystem(text, isErr) {
  const div = document.createElement('div');
  div.className = 'sys' + (isErr ? ' err' : ''); div.textContent = text;
  el.messages.append(div); scrollDown(true);
}
function renderUsers(list) {
  el.users.replaceChildren();
  list.forEach((u) => { const li = document.createElement('li'); li.textContent = u; el.users.append(li); });
  el.userCount.textContent = list.length;
}

// ---- «Está escribiendo…» (recibido) ----
function renderTypers() {
  const names = [...typers.keys()];
  el.typing.textContent = !names.length ? '' : names.length === 1 ? `${names[0]} está escribiendo…`
    : names.length === 2 ? `${names[0]} y ${names[1]} están escribiendo…` : 'Varias personas están escribiendo…';
}
function addTyper(name) {
  clearTimeout(typers.get(name));
  typers.set(name, setTimeout(() => removeTyper(name), 4000)); // caduca solo
  renderTypers();
}
function removeTyper(name) { clearTimeout(typers.get(name)); typers.delete(name); renderTypers(); }
function clearTypers() { typers.forEach((t) => clearTimeout(t)); typers.clear(); renderTypers(); }

// ---- «Está escribiendo…» (enviado) ----
function stopTyping() {
  clearTimeout(typingTimer);
  if (iAmTyping) { iAmTyping = false; send({ type: 'typing', isTyping: false }); }
}
el.msgInput.addEventListener('input', () => {
  if (!el.msgInput.value) return stopTyping();
  if (!iAmTyping) { iAmTyping = true; send({ type: 'typing', isTyping: true }); }
  clearTimeout(typingTimer); typingTimer = setTimeout(stopTyping, 2000);
});

// ---- Acciones del usuario ----
function doJoin() {
  const name = el.nameInput.value.trim();
  el.loginError.textContent = '';
  if (name.length < 2 || name.length > 20) { el.loginError.textContent = 'El nombre debe tener entre 2 y 20 caracteres'; return; }
  pendingName = name;
  if (ws && ws.readyState === WebSocket.OPEN) send({ type: 'join', name });
  else if (!ws || ws.readyState === WebSocket.CLOSED) { attempts = 0; connect(); }
}
function doSend() {
  const text = el.msgInput.value.trim();
  if (!text || !joined) return;
  if (text.length > 500) { addSystem('Máximo 500 caracteres', true); return; }
  stopTyping(); send({ type: 'message', text }); el.msgInput.value = ''; el.msgInput.focus();
}
el.joinBtn.addEventListener('click', doJoin);
el.nameInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doJoin(); });
el.sendBtn.addEventListener('click', doSend);
el.msgInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') doSend(); });
renderStatus();