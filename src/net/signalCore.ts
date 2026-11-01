/**
 * All-pool Monad signal core (pure, no I/O). Decodes v2 / v3 / PancakeSwap v3 / Uniswap v4 Swap, Mint and Burn logs,
 * prices each event in USD, keeps per-token 1m / 5m / 15m taker-flow windows and on-chain VWAP moves, and tracks
 * liquidity on the pools the router trades through. Flow is context: the backtest showed no directional edge at 1m/5m.
 */
export const TOPIC = {
  v2Swap: '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
  v3Swap: '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
  pcsSwap: '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83',
  v4Swap: '0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f',
  v3Mint: '0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde',
  v3Burn: '0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c',
  v2Mint: '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f',
  v2Burn: '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
  v4Modify: '0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec',
} as const;
export const ALL_TOPICS: string[] = Object.values(TOPIC);
export const V4_POOL_MANAGER = '0x188d586ddcf52439676ca21a244753fa19f9ea8e';
export const NATIVE = '0x0000000000000000000000000000000000000000';
export const WMON_ADDR = '0x3bd359c1119da7da1d913d1c4d2b7c461115433a';

/** Tokens with known decimals and a USD source. tier 0 = USD stable, 1 = MON (priced on-chain), 2 = CEX reference. */
export const KNOWN: Record<string, { sym: string; dec: number; tier: 0 | 1 | 2 }> = {
  '0x754704bc059f8c67012fed69bc8a327a5aafb603': { sym: 'USDC', dec: 6, tier: 0 },
  '0x00000000efe302beaa2b3e6e1b18d08d69a9012a': { sym: 'AUSD', dec: 6, tier: 0 },
  '0xe7cd86e13ac4309349f30b3435a9d337750fc82d': { sym: 'USDT0', dec: 6, tier: 0 },
  [WMON_ADDR]: { sym: 'MON', dec: 18, tier: 1 },
  [NATIVE]: { sym: 'MON', dec: 18, tier: 1 },
  '0xee8c0e9f1bffb4eb878d8f15f368a02a35481242': { sym: 'WETH', dec: 18, tier: 2 },
  '0x0555e30da8f98308edb960aa94c0db47230d2b9c': { sym: 'WBTC', dec: 8, tier: 2 },
  '0xd18b7ec58cdf4876f6afebd3ed1730e4ce10414b': { sym: 'cbBTC', dec: 8, tier: 2 },
  '0xea17e5a9efebf1477db45082d67010e2245217f1': { sym: 'SOL', dec: 9, tier: 2 },
};
/** Native MON and WMON are one token for flow. */
export const tokenKey = (t: string) => (t.toLowerCase() === NATIVE ? WMON_ADDR : t.toLowerCase());
export const isStable = (t: string) => KNOWN[t.toLowerCase()]?.tier === 0;

export type RawLog = { address: string; topics: string[]; data: string; blockNumber: string | number; logIndex?: string | number; transactionHash?: string; removed?: boolean };
export type PoolInfo = { t0: string; t1: string };
export type Venue = 'v2' | 'v3' | 'v4';
/** a0/a1 are signed token deltas from the pool's side (+ = into the pool). liq = liquidity units for v3/v4 mint/burn. */
export type Decoded = { kind: 'swap' | 'mint' | 'burn'; venue: Venue; pool: string; t0: string; t1: string; a0: bigint; a1: bigint; liq: bigint; block: number; idx: number; tx: string };

const words = (d: string) => { const h = d.startsWith('0x') ? d.slice(2) : d; const w: string[] = []; for (let i = 0; i + 64 <= h.length; i += 64) w.push(h.slice(i, i + 64)); return w; };
const u = (h: string) => BigInt('0x' + (h || '0'));
const s256 = (h: string) => { const v = u(h); return v >= 1n << 255n ? v - (1n << 256n) : v; };
const num = (v: string | number | undefined) => (typeof v === 'number' ? v : v == null ? 0 : Number.parseInt(v, 16));

export type PoolLookup = (key: string) => PoolInfo | null | undefined;

/** null = not a log we decode, or an unmapped pool (the caller counts those). */
export function decodeLog(l: RawLog, pools: PoolLookup): Decoded | null {
  const t0 = (l.topics[0] || '').toLowerCase();
  const addr = l.address.toLowerCase();
  const block = num(l.blockNumber); const idx = num(l.logIndex); const tx = l.transactionHash ?? '';
  const w = words(l.data);
  const base = (venue: Venue, key: string) => { const m = pools(key); return m ? { venue, pool: key, t0: m.t0.toLowerCase(), t1: m.t1.toLowerCase(), block, idx, tx } : null; };
  if (t0 === TOPIC.v4Swap || t0 === TOPIC.v4Modify) {
    if (addr !== V4_POOL_MANAGER || !l.topics[1]) return null;
    const b = base('v4', l.topics[1].toLowerCase()); if (!b) return null;
    if (t0 === TOPIC.v4Swap) {
      if (w.length < 2) return null;
      // v4 reports the swapper's delta: flip to the pool's side. int128 values arrive sign-extended to 256 bits.
      return { ...b, kind: 'swap', a0: -s256(w[0]), a1: -s256(w[1]), liq: 0n };
    }
    if (w.length < 3) return null;
    const d = s256(w[2]);
    if (d === 0n) return null;
    return { ...b, kind: d > 0n ? 'mint' : 'burn', a0: 0n, a1: 0n, liq: d > 0n ? d : -d };
  }
  if (t0 === TOPIC.v3Swap || t0 === TOPIC.pcsSwap) {
    const b = base('v3', addr); if (!b || w.length < 2) return null;
    return { ...b, kind: 'swap', a0: s256(w[0]), a1: s256(w[1]), liq: 0n };
  }
  if (t0 === TOPIC.v2Swap) {
    const b = base('v2', addr); if (!b || w.length < 4) return null;
    return { ...b, kind: 'swap', a0: u(w[0]) - u(w[2]), a1: u(w[1]) - u(w[3]), liq: 0n };
  }
  if (t0 === TOPIC.v3Mint) { const b = base('v3', addr); if (!b || w.length < 4) return null; return { ...b, kind: 'mint', a0: u(w[2]), a1: u(w[3]), liq: u(w[1]) }; }
  if (t0 === TOPIC.v3Burn) { const b = base('v3', addr); if (!b || w.length < 3) return null; return { ...b, kind: 'burn', a0: -u(w[1]), a1: -u(w[2]), liq: u(w[0]) }; }
  if (t0 === TOPIC.v2Mint) { const b = base('v2', addr); if (!b || w.length < 2) return null; return { ...b, kind: 'mint', a0: u(w[0]), a1: u(w[1]), liq: 0n }; }
  if (t0 === TOPIC.v2Burn) { const b = base('v2', addr); if (!b || w.length < 2) return null; return { ...b, kind: 'burn', a0: -u(w[0]), a1: -u(w[1]), liq: 0n }; }
  return null;
}

export type PriceFn = (token: string) => number | null;
const whole = (raw: bigint, dec: number) => Number(raw < 0n ? -raw : raw) / 10 ** dec;

/** USD size of an event from its best-priced side (stable, then MON, then CEX reference). */
export function usdOf(e: Pick<Decoded, 't0' | 't1' | 'a0' | 'a1'>, price: PriceFn): number | null {
  const sides = [{ t: e.t0, a: e.a0 }, { t: e.t1, a: e.a1 }]
    .map(s => ({ ...s, k: KNOWN[s.t] }))
    .filter(s => s.k && s.a !== 0n)
    .sort((x, y) => x.k!.tier - y.k!.tier);
  for (const s of sides) { const px = price(s.t); if (px != null && px > 0) return whole(s.a, s.k!.dec) * px; }
  return null;
}

/** One flow leg per non-stable token in a swap. A token was bought when the pool's balance of it fell. */
export function flowLegs(e: Decoded, usd: number): { token: string; side: 'buy' | 'sell'; usd: number; qty: number | null }[] {
  if (e.kind !== 'swap' || !(usd > 0)) return [];
  const out: { token: string; side: 'buy' | 'sell'; usd: number; qty: number | null }[] = [];
  for (const [t, a] of [[e.t0, e.a0], [e.t1, e.a1]] as [string, bigint][]) {
    if (a === 0n || isStable(t)) continue;
    const k = KNOWN[t];
    out.push({ token: tokenKey(t), side: a < 0n ? 'buy' : 'sell', usd, qty: k ? whole(a, k.dec) : null });
  }
  return out;
}

export type Win = { buyUsd: number; sellUsd: number; imb: number | null; n: number };
type Bucket = { b: number; s: number; n: number; pu: number; pq: number };

/** Per-token minute buckets: taker flow plus a USD VWAP for tokens with known decimals. Keeps ~70 minutes. */
export class FlowBook {
  private m = new Map<string, Map<number, Bucket>>();
  add(token: string, side: 'buy' | 'sell', usd: number, qty: number | null, tSec: number) {
    const mn = Math.floor(tSec / 60);
    let byMin = this.m.get(token); if (!byMin) { byMin = new Map(); this.m.set(token, byMin); }
    const b = byMin.get(mn) ?? { b: 0, s: 0, n: 0, pu: 0, pq: 0 };
    if (side === 'buy') b.b += usd; else b.s += usd;
    b.n += 1;
    if (qty != null && qty > 0) { b.pu += usd; b.pq += qty; }
    byMin.set(mn, b);
  }
  prune(nowSec: number, keepMin = 70) {
    const cut = Math.floor(nowSec / 60) - keepMin;
    for (const [t, byMin] of this.m) { for (const k of byMin.keys()) if (k < cut) byMin.delete(k); if (!byMin.size) this.m.delete(t); }
  }
  tokens(): string[] { return [...this.m.keys()]; }
  /** The last `mins` minutes including the forming one. */
  window(token: string, mins: number, nowSec: number): Win {
    const byMin = this.m.get(token); const end = Math.floor(nowSec / 60);
    let b = 0, s = 0, n = 0;
    if (byMin) for (let k = end - mins + 1; k <= end; k++) { const x = byMin.get(k); if (x) { b += x.b; s += x.s; n += x.n; } }
    return { buyUsd: b, sellUsd: s, imb: b + s > 0 ? (b - s) / (b + s) : null, n };
  }
  /** VWAP of the latest priced minute at or before `minute` (looking back up to `look` minutes). */
  vwapAt(token: string, minute: number, look = 3): number | null {
    const byMin = this.m.get(token); if (!byMin) return null;
    for (let k = minute; k > minute - look; k--) { const x = byMin.get(k); if (x && x.pq > 0) return x.pu / x.pq; }
    return null;
  }
  /** % change of the on-chain VWAP over `mins` minutes. null when either end has no priced swap. */
  move(token: string, mins: number, nowSec: number): number | null {
    const end = Math.floor(nowSec / 60);
    const now = this.vwapAt(token, end); const then = this.vwapAt(token, end - mins);
    return now != null && then != null && then > 0 ? (now / then - 1) * 100 : null;
  }
}

export type ShockLevel = 'none' | 'warn' | 'severe';
export type Shock = { pool: string; level: ShockLevel; dropPct: number; burnUsd5m: number; mintUsd5m: number; at: number };
export const SHOCK_WARN_PCT = 20;
export const SHOCK_SEVERE_PCT = 40;

/** Liquidity samples (pool liquidity() reads) and Mint/Burn USD per router pool. A drop vs the 15-minute high is a shock. */
export class LiquidityWatch {
  private s = new Map<string, { t: number; liq: number }[]>();
  private ev = new Map<string, { t: number; usd: number; burn: boolean }[]>();
  sample(pool: string, liq: number, tSec: number) {
    const a = this.s.get(pool) ?? []; a.push({ t: tSec, liq }); while (a.length && tSec - a[0].t > 900) a.shift(); this.s.set(pool, a);
  }
  event(pool: string, burn: boolean, usd: number, tSec: number) {
    const a = this.ev.get(pool) ?? []; a.push({ t: tSec, usd, burn }); while (a.length && tSec - a[0].t > 900) a.shift(); this.ev.set(pool, a);
  }
  shock(pool: string, nowSec: number): Shock {
    const a = (this.s.get(pool) ?? []).filter(x => nowSec - x.t <= 900);
    const cur = a[a.length - 1]?.liq ?? null; const hi = a.reduce((m, x) => Math.max(m, x.liq), 0);
    const dropPct = cur != null && hi > 0 ? Math.max(0, (1 - cur / hi) * 100) : 0;
    const ev = (this.ev.get(pool) ?? []).filter(x => nowSec - x.t <= 300);
    const burnUsd5m = ev.filter(x => x.burn).reduce((m, x) => m + x.usd, 0);
    const mintUsd5m = ev.filter(x => !x.burn).reduce((m, x) => m + x.usd, 0);
    const level: ShockLevel = dropPct >= SHOCK_SEVERE_PCT ? 'severe' : dropPct >= SHOCK_WARN_PCT ? 'warn' : 'none';
    return { pool, level, dropPct: +dropPct.toFixed(2), burnUsd5m: +burnUsd5m.toFixed(2), mintUsd5m: +mintUsd5m.toFixed(2), at: nowSec };
  }
}

/** Percentile of a numeric list (nearest rank). */
export function pctl(xs: number[], p: number): number | null {
  if (!xs.length) return null;
  const a = [...xs].sort((x, y) => x - y);
  return a[Math.min(a.length - 1, Math.max(0, Math.ceil((p / 100) * a.length) - 1))];
}

/** Block ranges to backfill after a gap: at most `maxBlocks` back, in chunks of ≤ `chunk` blocks (QuickNode caps getLogs at 1,000). */
export function backfillRanges(lastSeen: number, head: number, maxBlocks = 3000, chunk = 1000): [number, number][] {
  if (!(head > lastSeen)) return [];
  const from = Math.max(lastSeen + 1, head - maxBlocks + 1);
  const out: [number, number][] = [];
  for (let a = from; a <= head; a += chunk) out.push([a, Math.min(head, a + chunk - 1)]);
  return out;
}

/** Wire format pushed to clients. */
export type SignalSnapshot = {
  at: number;
  health: { source: 'quicknode' | 'fallback'; connected: boolean; head: number | null; headAgeMs: number | null; logLagP50Ms: number | null; logLagP90Ms: number | null; reconnects: number; backfilledLogs: number; decoded: number; unmapped: number; startedAt: number };
  flows: { token: string; sym: string; w1: Win; w5: Win; w15: Win }[];
  moves: Record<string, { m5: number | null; m15: number | null; h1: number | null }>;
  shocks: Shock[];
};

/** Pools the desk router trades through (asset/WMON on PancakeSwap v3) plus the USDC reference pools its gates read. */
export const ROUTER_POOLS: Record<string, string[]> = {
  WETH: ['0xb02793fe655c1169a8699b4ee462f8ac9c75e402', '0x25ef1a210ff55bcee9f8fee979aaff6bd1be5bf1'],
  WBTC: ['0x0944526d2727b532653e6ca6c4d980461e170a09', '0x9b60e561e3ab15782fbb23ea0a766dd8d91ff8ac'],
  cbBTC: ['0x614b85502b89540bb79be98d5429ec032a78a284', '0xebc92c45c652e9aae8a886fa49fb2135796d1be1'],
  SOL: ['0xbab9acc9d872c6cadb6dcf4f44bd0b54080a18ea'],
  USDCm: ['0x63e48b725540a3db24acf6682a29f877808c53f2'],
};
/** Desk symbol → token address, for the on-chain moves that replace the single GeckoTerminal pool. */
export const DESK_TOKENS: Record<string, string> = {
  WETH: '0xee8c0e9f1bffb4eb878d8f15f368a02a35481242',
  WBTC: '0x0555e30da8f98308edb960aa94c0db47230d2b9c',
  cbBTC: '0xd18b7ec58cdf4876f6afebd3ed1730e4ce10414b',
  SOL: '0xea17e5a9efebf1477db45082d67010e2245217f1',
};

export type Guard = { level: ShockLevel; block: boolean; tighten: boolean; text: string; dropPct: number };
/** Protective only: warn and tighten on a sharp drop, refuse a new entry on a severe one. Stale or missing data never blocks. */
export function liquidityGuardFrom(symbol: string, shocks: Shock[] | null | undefined, ageSec: number): Guard {
  const none: Guard = { level: 'none', block: false, tighten: false, text: '', dropPct: 0 };
  if (!shocks || !(ageSec <= 30)) return none;
  const mine = new Set(ROUTER_POOLS[symbol] ?? []);
  const worst = shocks.filter(s => mine.has(s.pool)).sort((a, b) => b.dropPct - a.dropPct)[0];
  if (!worst || worst.level === 'none') return none;
  const pct = Math.round(worst.dropPct);
  if (worst.level === 'severe') return { level: 'severe', block: true, tighten: true, dropPct: worst.dropPct, text: `Liquidity on the ${symbol} pool fell ${pct}% in the last 15 minutes. New entries are paused until it recovers. Nothing was sent.` };
  return { level: 'warn', block: false, tighten: true, dropPct: worst.dropPct, text: `Liquidity on the ${symbol} pool fell ${pct}% in the last 15 minutes. Using a live quote and a tighter minimum.` };
}
