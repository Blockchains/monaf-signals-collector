/**
 * Box collector for the 15m fade re-test. One QuickNode WSS socket (newHeads + all Swap/Mint/Burn logs, no address filter),
 * getLogs backfill on reconnect, Binance 1m klines, router-pool liquidity. Writes data/YYYY-MM-DD/*.jsonl (UTC day rotation)
 * and data/heartbeat.json. The endpoint is read from the repo defaults file and is never printed or written.
 */
import fs from 'node:fs';
import path from 'node:path';
import https from 'node:https';
import pools from './net/signalPools.json';
import { ALL_TOPICS, DESK_TOKENS, KNOWN, ROUTER_POOLS, WMON_ADDR, backfillRanges, decodeLog, flowLegs, tokenKey, usdOf, type PoolInfo, type RawLog } from './net/signalCore.ts';

const ROOT = process.env.SIGNALS_DIR || '/workspace/monaf/signals-tape';
const DATA = ROOT + '/data';
fs.mkdirSync(DATA, { recursive: true }); fs.mkdirSync(ROOT + '/run', { recursive: true });
// optional QuickNode fallback (box only): env, else the mondex defaults file if present. Public Monad RPC needs no secrets.
let src = ''; try { src = fs.readFileSync(process.env.QN_DEFAULTS_FILE || '/workspace/mondex/repo/src/net/quicknode.defaults.server.ts', 'utf8'); } catch { /* none (e.g. GitHub Actions) */ }
const WSS = process.env.QUICKNODE_MONAD_WSS_URL || /wssUrl:\s*"([^"]+)"/.exec(src)?.[1] || '';
const RPC = process.env.QUICKNODE_MONAD_RPC_URL || /rpcUrl:\s*"([^"]+)"/.exec(src)?.[1] || '';
const red = (s: string) => s.replace(/(https?|wss?):\/\/\S+/g, '<redacted-url>').replace(/[A-Za-z0-9_]{24,}/g, '<redacted>');
const log = (...a: unknown[]) => console.log(new Date().toISOString(), red(a.map(String).join(' ')));

const T = (pools as unknown as { t: string[] }).t; const P = (pools as unknown as { p: Record<string, [number, number]> }).p;
const dyn = new Map<string, PoolInfo | null>();
const lookup = (k: string) => { const r = P[k]; return r ? { t0: T[r[0]], t1: T[r[1]] } : dyn.get(k); };
const USDC_MON = '0x63e48b725540a3db24acf6682a29f877808c53f2';
const ROUTER = [...new Set(Object.values(ROUTER_POOLS).flat())];
const px = new Map<string, number>();
const price = (t: string) => { const k = KNOWN[t]; if (!k) return null; if (k.tier === 0) return 1; if (k.tier === 1) return px.get(WMON_ADDR) ?? null; return px.get(tokenKey(t)) ?? null; };

const day = (ms = Date.now()) => new Date(ms).toISOString().slice(0, 10);
const streams = new Map<string, fs.WriteStream>();
function out(name: string, row: unknown) {
  const d = day(); const key = `${d}/${name}`;
  let s = streams.get(key);
  if (!s) {
    for (const [k, v] of streams) if (!k.startsWith(d)) { v.end(); streams.delete(k); }
    fs.mkdirSync(path.join(DATA, d), { recursive: true });
    s = fs.createWriteStream(path.join(DATA, d, name + '.jsonl'), { flags: 'a' }); streams.set(key, s);
  }
  s.write(JSON.stringify(row) + '\n');
}


const st = { startedAt: Date.now(), connected: false, head: 0, headAt: 0, lastSeen: 0, reconnects: 0, events: 0, backfilled: 0, unmapped: 0, lags: [] as number[], klinesAt: 0, wsHeadAt: 0, polledTo: 0, httpHeads: 0 };
const headArrive = new Map<number, number>(); const blockTs = new Map<number, number>();
const seen = new Set<string>(); const seenQ: string[] = [];

function ingest(l: RawLog, bf: boolean) {
  if (!l || l.removed || !Array.isArray(l.topics)) return;
  const key = `${l.transactionHash}:${l.logIndex}`; if (seen.has(key)) return;
  seen.add(key); seenQ.push(key); if (seenQ.length > 100_000) seen.delete(seenQ.shift()!);
  const e = decodeLog(l, lookup);
  const b = Number.parseInt(String(l.blockNumber), 16);
  if (b > st.lastSeen) st.lastSeen = b;
  const recv = Date.now();
  const lag = !bf && headArrive.has(b) ? recv - headArrive.get(b)! : null;
  if (lag != null) { st.lags.push(lag); if (st.lags.length > 5000) st.lags.shift(); }
  if (!e) {
    st.unmapped++; out('unmapped', { b, a: l.address.toLowerCase(), t0: l.topics[0], p1: l.topics[1] ?? null, d: l.data, tx: l.transactionHash, i: l.logIndex });
    lookupPool(l); return;
  }
  if (e.kind === 'swap' && e.pool === USDC_MON && e.a0 !== 0n && e.a1 !== 0n) {
    const p = (Number(e.a1 < 0n ? -e.a1 : e.a1) / 1e6) / (Number(e.a0 < 0n ? -e.a0 : e.a0) / 1e18); if (p > 0 && Number.isFinite(p)) px.set(WMON_ADDR, p);
  }
  const usd = usdOf(e, price);
  const legs = e.kind === 'swap' && usd != null ? flowLegs(e, usd).map(f => [KNOWN[f.token]?.sym ?? f.token, f.side]) : [];
  st.events++; if (bf) st.backfilled++;
  out('events', { ts: blockTs.get(b) ?? Math.floor(recv / 1000), recv, b, i: e.idx, tx: e.tx, k: e.kind, v: e.venue, p: e.pool, t0: e.t0, t1: e.t1, a0: e.a0.toString(), a1: e.a1.toString(), liq: e.liq.toString(), usd: usd == null ? null : +usd.toFixed(2), legs, router: ROUTER.includes(e.pool) || undefined, bf: bf || undefined, lag });
}
const pending = new Set<string>();
function lookupPool(l: RawLog) {
  const a = l.address.toLowerCase();
  if (a === '0x188d586ddcf52439676ca21a244753fa19f9ea8e' || dyn.has(a) || pending.has(a) || pending.size >= 4) return;
  pending.add(a);
  void Promise.all(['0x0dfe1681', '0xd21220a7'].map(d => rpc<string>('eth_call', [{ to: a, data: d }, 'latest']).catch(() => null))).then(([x, y]) => {
    dyn.set(a, x && y && x.length >= 66 && y.length >= 66 ? { t0: '0x' + x.slice(26, 66), t1: '0x' + y.slice(26, 66) } : null); pending.delete(a);
  });
}

async function backfill(from: number, head: number, kind = 'backfill') {
  for (const [a, b] of backfillRanges(from, head, 20_000, 100)) {
    try {
      const [logs, ba, bb] = await Promise.all([
        rpc<RawLog[]>('eth_getLogs', [{ fromBlock: '0x' + a.toString(16), toBlock: '0x' + b.toString(16), topics: [ALL_TOPICS] }], 4000),
        rpc<{ timestamp: string }>('eth_getBlockByNumber', ['0x' + a.toString(16), false]),
        rpc<{ timestamp: string }>('eth_getBlockByNumber', ['0x' + b.toString(16), false]),
      ]);
      const ta = Number.parseInt(ba.timestamp, 16), tb = Number.parseInt(bb.timestamp, 16);
      for (const l of logs) { const n = Number.parseInt(String(l.blockNumber), 16); const had = blockTs.has(n); if (!had) blockTs.set(n, Math.round(ta + ((n - a) * (tb - ta)) / Math.max(1, b - a))); ingest(l, true); if (!had) blockTs.delete(n); }
    } catch (err) { log('backfill chunk failed', a, b, (err as Error).message); }
    await new Promise(r => setTimeout(r, 60));
  }
  if (kind === 'backfill') out('health', { t: Date.now(), kind, from, head });
}

// ===== network layer: public Monad RPC first (ranked by latency+freshness), QuickNode last-resort fallback =====
// keep-alive agents, 2s timeouts, hedged requests, per-endpoint circuit breakers, jittered backoff,
// two redundant WS subscriptions (deduped heads/logs) + always-on hot-standby HTTP polling.
type Ep = { name: string; url: string; qn: boolean; agent: https.Agent; ewma: number; samples: number[]; fails: number; trips: number; openUntil: number; head: number; headAt: number; lastProbe: number; ok: number; err: number; behind: number };
const mkAgent = () => new https.Agent({ keepAlive: true, keepAliveMsecs: 10_000, maxSockets: 32, maxFreeSockets: 8, timeout: 30_000 });
const PUB_HTTP = ['rpc', 'rpc1', 'rpc2', 'rpc3'];
const PUB_WS = ['rpc', 'rpc1', 'rpc3', 'rpc2'];
const HTTP: Ep[] = PUB_HTTP.map(n => ({ name: n, url: `https://${n}.monad.xyz`, qn: false, agent: mkAgent(), ewma: 300, samples: [], fails: 0, trips: 0, openUntil: 0, head: 0, headAt: 0, lastProbe: 0, ok: 0, err: 0, behind: 0 }));
if (RPC) HTTP.push({ name: 'quicknode', url: RPC, qn: true, agent: mkAgent(), ewma: 1e4, samples: [], fails: 0, trips: 0, openUntil: 0, head: 0, headAt: 0, lastProbe: Date.now(), ok: 0, err: 0, behind: 0 });
const WSEPS = PUB_WS.map(n => ({ name: n, url: `wss://${n}.monad.xyz`, qn: false, fails: 0, openUntil: 0 }));
if (WSS) WSEPS.push({ name: 'quicknode', url: WSS, qn: true, fails: 0, openUntil: 0 });
const QN_PROBE_MS = 15 * 60_000;
const jitter = (ms: number) => Math.round(ms * (0.5 + Math.random()));
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
class RpcErr extends Error { constructor(m: string, public endpoint: boolean) { super(m); } }

let blocked = new Set<string>();
setInterval(() => { try { blocked = new Set(fs.readFileSync(ROOT + '/run/fault-block', 'utf8').split(/\s+/).filter(Boolean)); } catch { blocked = new Set(); } }, 1000);
let rpcId = 0;
function post(e: Ep, body: string, ms: number): Promise<any> {
  if (blocked.has(e.name)) return new Promise((_, reject) => setTimeout(() => reject(new RpcErr('timeout ' + ms + 'ms (fault-block)', true)), ms));
  return new Promise((resolve, reject) => {
    const t0 = performance.now(); let done = false;
    const fin = (err: Error | null, v?: unknown) => { if (done) return; done = true; clearTimeout(to); if (err) reject(err); else { sample(e, performance.now() - t0); resolve(v); } };
    const req = https.request(e.url, { method: 'POST', agent: e.agent, headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) } }, res => {
      const ch: Buffer[] = []; res.on('data', c => ch.push(c)); res.on('error', er => fin(new RpcErr(er.message, true)));
      res.on('end', () => {
        if ((res.statusCode ?? 0) >= 400) return fin(new RpcErr('http ' + res.statusCode, true));
        let j: any; try { j = JSON.parse(Buffer.concat(ch).toString()); } catch { return fin(new RpcErr('bad json', true)); }
        if (j.error) { const m = String(j.error.message || 'rpc error'); return fin(new RpcErr(m, /quota|credit|rate|limit exceeded|too many|restricted|capacity|unavailable/i.test(m) && !/range/i.test(m))); }
        fin(null, j.result);
      });
    });
    const to = setTimeout(() => { req.destroy(); fin(new RpcErr('timeout ' + ms + 'ms', true)); }, ms);
    req.on('error', er => fin(new RpcErr(er.message, true)));
    req.end(body);
  });
}
function sample(e: Ep, ms: number) { e.ewma = e.samples.length ? e.ewma * 0.7 + ms * 0.3 : ms; e.samples.push(ms); if (e.samples.length > 200) e.samples.shift(); }
function good(e: Ep) { e.fails = 0; e.trips = 0; e.ok++; }
function bad(e: Ep, why: string) {
  e.err++; e.fails++;
  if (e.fails >= (e.qn ? 1 : 3)) {
    e.trips++; const base = e.qn ? QN_PROBE_MS : Math.min(300_000, 10_000 * 2 ** (e.trips - 1));
    e.openUntil = Date.now() + jitter(base); e.fails = 0;
    log('breaker open', e.name, Math.round((e.openUntil - Date.now()) / 1000) + 's', why.slice(0, 60)); out('health', { t: Date.now(), kind: 'breaker', ep: e.name, until: e.openUntil });
  }
}
const maxHead = () => Math.max(0, ...HTTP.map(e => e.head));
const lat = (e: Ep) => { if (!e.samples.length) return e.ewma; const a = e.samples.slice(-30).sort((x, y) => x - y); return 0.5 * a[a.length >> 1] + 0.5 * e.ewma; };
function score(e: Ep) { return lat(e) + Math.max(0, e.behind - 1) * 150 + (e.qn ? 1e6 : 0); }
function ranked(): Ep[] {
  const now = Date.now(); const up = HTTP.filter(e => e.openUntil <= now).sort((a, b) => score(a) - score(b));
  return up.length ? up : [...HTTP].sort((a, b) => a.openUntil - b.openUntil); // all open: try the soonest-closing anyway
}
let lastPrimary = '';
async function rpc<T>(method: string, params: unknown[], ms = 2000): Promise<T> {
  const body = JSON.stringify({ jsonrpc: '2.0', id: ++rpcId, method, params });
  const cands = ranked(); let lastErr: Error = new Error('no endpoint');
  if (cands[0] && cands[0].name !== lastPrimary) { lastPrimary = cands[0].name; log('primary rpc ->', cands[0].name); out('health', { t: Date.now(), kind: 'endpoint', rpc: cands[0].name }); }
  // hedge: fire #1, and #2 if #1 hasn't answered within ~2x its typical latency
  for (let i = 0; i < cands.length; i += 2) {
    const a = cands[i], b = cands[i + 1];
    const hedgeAfter = Math.min(600, Math.max(120, 2.5 * a.ewma));
    try {
      return await new Promise<T>((resolve, reject) => {
        let pending = 0, settled = false, fired = 0;
        const fire = (e: Ep) => { pending++; fired++; post(e, body, ms).then(v => { good(e); if (!settled) { settled = true; resolve(v as T); } }, (err: RpcErr) => { pending--; if (err.endpoint) bad(e, err.message); else good(e); lastErr = err; if (!err.endpoint && !settled) { settled = true; reject(err); return; } if (!settled && pending === 0 && (fired === 2 || !b)) { settled = true; reject(err); } else if (!settled && pending === 0 && b && fired === 1) fire(b); }); };
        fire(a);
        if (b && !(b.qn && !a.qn)) setTimeout(() => { if (!settled && fired === 1) fire(b); }, hedgeAfter);
      });
    } catch (err) { if (!(err as RpcErr).endpoint) throw err; }
  }
  throw lastErr;
}
// re-rank: probe every endpoint (QuickNode only every 15 min to spare quota)
async function probeAll() {
  const now = Date.now();
  const act = HTTP.filter(e => !(e.qn && now - e.lastProbe < QN_PROBE_MS) && !(!e.qn && e.openUntil > now && now - e.lastProbe < 30_000));
  // freshness: one simultaneous blockNumber per endpoint, compared against the best answer
  const hs = await Promise.all(act.map(e => post(e, JSON.stringify({ jsonrpc: '2.0', id: ++rpcId, method: 'eth_blockNumber', params: [] }), 2000).then(v => Number.parseInt(v as string, 16), () => 0)));
  const top = Math.max(0, ...hs); act.forEach((e, i) => { if (hs[i]) { e.head = hs[i]; e.headAt = Date.now(); e.behind = e.behind * 0.5 + (top - hs[i]) * 0.5; } });
  await Promise.all(act.map(async e => {
    e.lastProbe = now;
    for (let k = 0; k < 4; k++) {
      try { const n = Number.parseInt(await post(e, JSON.stringify({ jsonrpc: '2.0', id: ++rpcId, method: 'eth_blockNumber', params: [] }), 2000), 16); if (n > e.head) { e.head = n; e.headAt = Date.now(); } good(e); if (e.openUntil > Date.now()) { e.openUntil = 0; log('breaker closed', e.name); } }
      catch (err) { bad(e, (err as Error).message); break; }
    }
  }));
  // move a WS slot to a faster endpoint if one is clearly better (only while the other slot is live)
  for (const s of slots) {
    const best = pickWs(s); const o = slots.find(x => x !== s)!;
    const hl = (n?: string) => { const h = HTTP.find(x => x.name === n); return h ? lat(h) : 1e9; };
    if (s.ws && s.ep && best && best.name !== s.ep.name && hl(best.name) + 30 < hl(s.ep.name) && o.ws && Date.now() - o.headAt < 2000) { log('ws', s.id, 're-rank', s.ep.name, '->', best.name); const w = s.ws; s.ws = null; w.onclose = null; try { w.close(); } catch { /* */ } connect(s); break; }
  }
  const r = ranked(); out('health', { t: Date.now(), kind: 'rank', r: r.map(e => [e.name, Math.round(e.ewma), +e.behind.toFixed(2), e.openUntil > Date.now() ? 'open' : 'ok']) });
}

// ---- WebSocket: two redundant subscriptions on different endpoints ----
type Slot = { id: number; ws: WebSocket | null; ep: (typeof WSEPS)[number] | null; headAt: number; openedAt: number; backoff: number; heads: number };
const slots: Slot[] = [0, 1].map(id => ({ id, ws: null, ep: null, headAt: 0, openedAt: 0, backoff: 1000, heads: 0 }));
let lastAnyWsHead = 0;
function pickWs(s: Slot) {
  const now = Date.now(); const other = slots.find(o => o !== s)?.ep?.name;
  const up = WSEPS.filter(e => e.openUntil <= now && e.name !== other);
  const hl = (n: string) => { const h = HTTP.find(x => x.name === n); return h ? lat(h) : 1e9; };
  const pub = up.filter(e => !e.qn).sort((a, b) => hl(a.name) - hl(b.name)); return pub[0] ?? up[0] ?? WSEPS.filter(e => e.name !== other).sort((a, b) => a.openUntil - b.openUntil)[0];
}
function connect(s: Slot) {
  const e = pickWs(s); s.ep = e; s.openedAt = Date.now(); s.headAt = 0;
  log('ws', s.id, 'connect ->', e.name);
  let ws: WebSocket; try { ws = new WebSocket(e.url); } catch { return wsFail(s, 'ctor'); }
  s.ws = ws; const ids = new Map<string, string>(); let first = true;
  ws.onopen = () => { ws.send(JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'eth_subscribe', params: ['newHeads'] })); ws.send(JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'eth_subscribe', params: ['logs', { topics: [ALL_TOPICS] }] })); };
  ws.onmessage = (m) => {
    if (blocked.has('ws:' + e.name)) return;
    let msg: any; try { msg = JSON.parse(String(m.data)); } catch { return; }
    if (msg.id === 1 && msg.result) ids.set(msg.result, 'h'); if (msg.id === 2 && msg.result) ids.set(msg.result, 'l');
    if (msg.method !== 'eth_subscription') return;
    const r = msg.params?.result; if (!r) return;
    if (ids.get(msg.params.subscription) === 'h' || 'parentHash' in r) {
      const n = Number.parseInt(r.number, 16), ts = Number.parseInt(r.timestamp, 16), now = Date.now();
      s.headAt = now; s.heads++; s.backoff = 1000; e.fails = 0;
      const wasStale = now - lastAnyWsHead > 3000; lastAnyWsHead = now; st.wsHeadAt = now; st.connected = true;
      if (!headArrive.has(n)) { // dedupe across both sockets: first arrival wins
        headArrive.set(n, now); blockTs.set(n, ts); if (headArrive.size > 600) { const k = headArrive.keys().next().value!; headArrive.delete(k); blockTs.delete(k); }
        if (n > st.head) { st.head = n; st.headAt = now; }
        out('heads', { b: n, ts, recv: now, gas: Number.parseInt(r.gasUsed ?? '0x0', 16), ws: e.name });
      }
      if (first) { first = false; if (wasStale) { const from = Math.max(st.lastSeen, st.polledTo); if (from) void backfill(from, n); } }
    } else ingest(r, false);
  };
  ws.onerror = () => undefined;
  ws.onclose = () => { if (s.ws === ws) { s.ws = null; out('health', { t: Date.now(), kind: 'disconnect', slot: s.id, ep: e.name }); wsFail(s, 'close'); } };
  const ka = setInterval(() => { if (ws.readyState === 1) try { ws.send(JSON.stringify({ jsonrpc: '2.0', id: 9, method: 'eth_chainId', params: [] })); } catch { /* */ } }, 15_000);
  const wd = setInterval(() => {
    const now = Date.now();
    const dead = ws.readyState === 1 ? now - Math.max(s.headAt, s.openedAt) > 5_000 : ws.readyState === 0 && now - s.openedAt > 5_000;
    if (dead && s.ws === ws) { log('ws', s.id, 'watchdog: no heads 5s on', e.name); out('health', { t: now, kind: 'watchdog', slot: s.id, ep: e.name }); s.ws = null; ws.onclose = null; try { ws.close(); } catch { /* */ } clearInterval(wd); clearInterval(ka); wsFail(s, 'watchdog'); }
  }, 1000);
  ws.addEventListener('close', () => { clearInterval(wd); clearInterval(ka); });
}
function wsFail(s: Slot, why: string) {
  const e = s.ep; st.reconnects++;
  if (e && (s.headAt < s.openedAt || !s.headAt)) { e.fails++; if (e.fails >= (e.qn ? 1 : 2)) { e.openUntil = Date.now() + jitter(e.qn ? QN_PROBE_MS : Math.min(300_000, 15_000 * e.fails)); } }
  if (!slots.some(o => o.ws && o.headAt)) st.connected = false;
  const w = jitter(s.backoff); s.backoff = Math.min(15_000, s.backoff * 2); setTimeout(() => connect(s), why === 'ctor' ? w : Math.min(w, 2000));
}

// ---- hot-standby HTTP: always polls the head; pulls logs itself whenever WS heads are >2s stale ----
let polling = false;
async function poll() {
  if (polling) return; polling = true;
  try {
    const n = Number.parseInt(await rpc<string>('eth_blockNumber', [], 1500), 16);
    const now = Date.now(); const wsLive = now - lastAnyWsHead < 2_000;
    if (wsLive) { st.polledTo = Math.max(st.polledTo, Math.min(n, st.head)); }
    else if (n > st.head) {
      const from = Math.max(st.polledTo, st.lastSeen, st.head) + 1;
      st.head = n; st.headAt = now; st.httpHeads++;
      out('heads', { b: n, recv: now, src: 'http' });
      if (from > 1 && n >= from) await backfill(Math.max(from, n - 2000), n, 'poll');
      st.polledTo = n;
    }
  } catch (err) { log('poll', (err as Error).message); }
  polling = false;
}
function netStatus() {
  const now = Date.now();
  return { rpc: ranked().slice(0, 5).map(e => ({ n: e.name, ms: Math.round(lat(e)), behind: +e.behind.toFixed(2), open: e.openUntil > now, ok: e.ok, err: e.err })), ws: slots.map(s => ({ ep: s.ep?.name ?? null, live: !!s.ws && now - s.headAt < 3000, heads: s.heads })) };
}

const SYMS: Record<string, string> = { ETHUSDT: DESK_TOKENS.WETH, BTCUSDT: DESK_TOKENS.WBTC, SOLUSDT: DESK_TOKENS.SOL };
const lastK = new Map<string, number>();
async function klines() {
  for (const s of Object.keys(SYMS)) {
    try {
      const r = await fetch(`https://data-api.binance.vision/api/v3/klines?symbol=${s}&interval=1m&limit=5`, { signal: AbortSignal.timeout(6000) });
      const rows = await r.json() as unknown[][];
      const now = Date.now();
      for (const k of rows) {
        const open = Number(k[0]), close = Number(k[6]);
        if (close > now) { px.set(SYMS[s], Number(k[4])); if (s === 'BTCUSDT') px.set(DESK_TOKENS.cbBTC, Number(k[4])); continue; }
        if (open <= (lastK.get(s) ?? 0)) continue;
        lastK.set(s, open);
        out('binance1m', { s, t: open, o: +k[1], h: +k[2], l: +k[3], c: +k[4], v: +k[5], qv: +k[7], n: k[8], tbq: +k[10] });
        px.set(SYMS[s], Number(k[4])); if (s === 'BTCUSDT') px.set(DESK_TOKENS.cbBTC, Number(k[4]));
      }
      st.klinesAt = now;
    } catch (err) { log('klines', s, (err as Error).message); }
  }
}
async function liquidity() {
  const t = Date.now();
  const row: Record<string, number | null> = {};
  await Promise.all(ROUTER.map(async p => { const r = await rpc<string>('eth_call', [{ to: p, data: '0x1a686502' }, 'latest']).catch(() => null); row[p] = r && r.length > 2 ? Number(BigInt(r)) : null; }));
  out('liquidity', { t, l: row });
}
async function monBootstrap() {
  const r = await rpc<string>('eth_call', [{ to: USDC_MON, data: '0x3850c7bd' }, 'latest']).catch(() => null);
  if (r && r.length >= 66) { const sq = Number(BigInt(r.slice(0, 66))) / 2 ** 96; px.set(WMON_ADDR, sq * sq * 1e12); }
}
function heartbeat() {
  const l = [...st.lags].sort((a, b) => a - b); const q = (p: number) => l.length ? l[Math.min(l.length - 1, Math.ceil(p * l.length) - 1)] : null;
  const hb = { t: Date.now(), pid: process.pid, startedAt: st.startedAt, connected: st.connected, head: st.head, headAgeMs: st.headAt ? Date.now() - st.headAt : null, lastSeen: st.lastSeen, reconnects: st.reconnects, events: st.events, backfilled: st.backfilled, unmapped: st.unmapped, lagP50: q(0.5), lagP90: q(0.9), klinesAgeMs: st.klinesAt ? Date.now() - st.klinesAt : null, monUsd: px.get(WMON_ADDR) ?? null, httpHeads: st.httpHeads, wsHeadAgeMs: st.wsHeadAt ? Date.now() - st.wsHeadAt : null, net: netStatus() };
  fs.writeFileSync(DATA + '/heartbeat.json.tmp', JSON.stringify(hb)); fs.renameSync(DATA + '/heartbeat.json.tmp', DATA + '/heartbeat.json');
  return hb;
}

// resume across restarts: start backfill from the last block in the previous heartbeat
try { const hb = JSON.parse(fs.readFileSync(DATA + '/heartbeat.json', 'utf8')); if (hb.lastSeen > 0) st.lastSeen = hb.lastSeen; } catch { /* first run */ }
// single instance: refuse to start if another live collector holds the lock
const LOCK = ROOT + '/run/collector.lock';
if (Number(process.env.RUN_FOR_S) > 0) setTimeout(() => { log('RUN_FOR_S reached, exiting'); heartbeat(); for (const v of streams.values()) v.end(); setTimeout(() => process.exit(0), 1500); }, Number(process.env.RUN_FOR_S) * 1000);
try { const o = Number(fs.readFileSync(LOCK, 'utf8')); if (o && o !== process.pid) { try { process.kill(o, 0); console.log(new Date().toISOString(), 'another collector running', o, '- exiting'); process.exit(3); } catch { /* stale */ } } } catch { /* none */ }
fs.writeFileSync(LOCK, String(process.pid));
process.on('unhandledRejection', e => log('unhandled', String(e)));
log('collector start pid', process.pid, 'resume', st.lastSeen);
heartbeat(); setInterval(() => heartbeat(), 2_000);
let tick = Date.now();
setInterval(() => { const now = Date.now(); if (now - tick > 10_000) { log('timer drift', now - tick, 'ms (process/box was paused)'); out('health', { t: now, kind: 'pause', ms: now - tick }); for (const sl of slots) if (sl.ws) try { sl.ws.close(); } catch { /* */ } } tick = now; }, 1000);
void probeAll().finally(() => { connect(slots[0]); setTimeout(() => connect(slots[1]), 300); });
setInterval(() => void probeAll(), 60_000);
setInterval(() => void poll(), 500);
void monBootstrap(); void klines();
setInterval(() => void klines(), 20_000);
setInterval(() => void liquidity(), 30_000);
setInterval(() => { const hb = heartbeat(); out('health', { kind: 'minute', ...hb }); }, 60_000);
