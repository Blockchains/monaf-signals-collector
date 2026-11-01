#!/usr/bin/env python3
"""Hourly signals tape. Usage: tape.py SITE_DIR HOUR_START_EPOCH [END_EPOCH].
Writes SITE_DIR/YYYY-MM-DD/HHMM-UTC.html (+ -p2, -p3… when over budget), updates index.html,
prints every relative path for the hour (primary first), one per line."""
import json, os, sys, glob, math, html, statistics, time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from zoneinfo import ZoneInfo

D = os.environ.get('SIGNALS_DATA', '/workspace/monaf/signals-tape/data')
BASE = 'https://blockchains.github.io/monaf-tape/'
PAGE_BUDGET = 180_000  # bytes per HTML page before splitting
site, h0 = sys.argv[1], int(sys.argv[2]); h1 = int(sys.argv[3]) if len(sys.argv) > 3 else h0 + 3600
now = time.time()
LON = lambda t: datetime.fromtimestamp(t, timezone.utc).astimezone(ZoneInfo('Europe/London'))
UTC = lambda t: datetime.fromtimestamp(t, timezone.utc)
SYM = {'WETH': 'ETH', 'WBTC': 'BTC', 'cbBTC': 'BTC'}
ROUTER = {'0xb02793fe655c1169a8699b4ee462f8ac9c75e402': 'WETH/WMON', '0x25ef1a210ff55bcee9f8fee979aaff6bd1be5bf1': 'WETH/USDC ref', '0x0944526d2727b532653e6ca6c4d980461e170a09': 'WBTC/WMON',
          '0x9b60e561e3ab15782fbb23ea0a766dd8d91ff8ac': 'WBTC/USDC ref', '0x614b85502b89540bb79be98d5429ec032a78a284': 'cbBTC/WMON', '0xebc92c45c652e9aae8a886fa49fb2135796d1be1': 'cbBTC/USDC ref',
          '0xbab9acc9d872c6cadb6dcf4f44bd0b54080a18ea': 'SOL/WMON', '0x63e48b725540a3db24acf6682a29f877808c53f2': 'WMON/USDC'}

def rows(name, days=None):
    for f in sorted(glob.glob(f'{D}/*/{name}.jsonl')):
        if days and os.path.basename(os.path.dirname(f)) not in days: continue
        with open(f) as fh:
            for line in fh:
                try: yield json.loads(line)
                except Exception: pass

days_all = sorted(os.path.basename(p) for p in glob.glob(f'{D}/20*') if os.path.isdir(p))
def daysfor(a, b):
    out, t = set(), a - 86400
    while t <= b + 86400: out.add(UTC(t).strftime('%Y-%m-%d')); t += 3600
    return out

# ---- all events (research needs full history) ----
minute = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0]))  # asset -> minute -> [buy, sell]
hour_flow = defaultdict(lambda: [0.0, 0.0, 0])
lags, liq_ev, nev, nbf, first_ts = [], [], 0, 0, None
for e in rows('events'):
    ts = e['ts']; first_ts = ts if first_ts is None else min(first_ts, ts)
    usd = e.get('usd') or 0
    for tok, side in e.get('legs', []):
        a = SYM.get(tok)
        if a: minute[a][ts // 60][0 if side == 'buy' else 1] += usd
        if h0 <= ts < h1:
            f = hour_flow[tok]; f[0 if side == 'buy' else 1] += usd; f[2] += 1
    if h0 <= ts < h1:
        nev += 1; nbf += 1 if e.get('bf') else 0
        if e.get('lag') is not None: lags.append(e['lag'])
        if e.get('router') and e['k'] in ('mint', 'burn'): liq_ev.append(e)

px = defaultdict(dict)  # asset -> minute -> close
for k in rows('binance1m'):
    a = {'ETHUSDT': 'ETH', 'BTCUSDT': 'BTC', 'SOLUSDT': 'SOL'}.get(k['s'])
    if a: px[a][k['t'] // 60000] = k['c']

# ---- 15m fade research ----
TH, VOL, HOLD = 0.3, 5000, 15
trig = []
for a in ('ETH', 'BTC'):
    mins = minute[a]
    if not mins: continue
    lo, hi = min(mins), max(mins); nxt = -1
    for m in range(lo + 4, hi + 1):
        if m < nxt: continue
        b = sum(mins[x][0] for x in range(m - 4, m + 1) if x in mins); s = sum(mins[x][1] for x in range(m - 4, m + 1) if x in mins)
        if b + s < VOL: continue
        I = (b - s) / (b + s)
        if abs(I) < TH: continue
        p0 = px[a].get(m); p1 = px[a].get(m + HOLD)
        ret = None if p0 is None or p1 is None else (p1 / p0 - 1) * 1e4 * (-1 if I > 0 else 1)
        if p0 is None: continue
        trig.append({'a': a, 'm': m, 'I': I, 'vol': b + s, 'fade': 'short' if I > 0 else 'long', 'bps': ret})
        nxt = m + HOLD
def summ(xs):
    xs = [x for x in xs if x is not None]
    if not xs: return {'n': 0}
    sd = statistics.pstdev(xs) if len(xs) > 1 else 0
    return {'n': len(xs), 'hit': sum(1 for x in xs if x > 0) / len(xs), 'mean': statistics.mean(xs), 'med': statistics.median(xs), 't': (statistics.mean(xs) / (sd / math.sqrt(len(xs)))) if sd > 0 else None}

# ---- health ----
hl = [h for h in rows('health', daysfor(h0, h1)) if h.get('kind') == 'minute' and h0 * 1000 <= h['t'] < h1 * 1000]
heads = [h for h in rows('heads', daysfor(h0, h1)) if h0 <= h['recv'] / 1000 < h1]
_end = min(h1, int(time.time())) * 1000
_r = [h0 * 1000] + [h['recv'] for h in heads] + [_end]  # include window edges: silence at start/end of the hour is a gap too
gaps = [(_r[i] - _r[i - 1]) for i in range(1, len(_r))] if _end > h0 * 1000 else []
# box/VM suspends: collector logs kind=pause {t, ms} after the wall clock jumps; sum overlap with this window
pauses = [(h['t'] - h['ms'], h['t']) for h in rows('health', daysfor(h0 - 86400, h1 + 86400)) if h.get('kind') == 'pause']
paused_s = sum(max(0, min(b, h1 * 1000) - max(a, h0 * 1000)) for a, b in pauses) / 1000
disc = [h for h in rows('health', daysfor(h0, h1)) if h.get('kind') == 'disconnect' and h0 * 1000 <= h['t'] < h1 * 1000]
bfs = [h for h in rows('health', daysfor(h0, h1)) if h.get('kind') == 'backfill' and h0 * 1000 <= h['t'] < h1 * 1000]
try: hb = json.load(open(f'{D}/heartbeat.json'))
except Exception: hb = {}
try: restarts = [int(x) for x in open(os.environ.get('SIGNALS_RESTARTS', '/workspace/monaf/signals-tape/run/restarts')).read().split()]
except Exception: restarts = []
span = max(1, min(h1, now) - h0)
covered = len(hl)
q = lambda xs, p: sorted(xs)[min(len(xs) - 1, max(0, math.ceil(p * len(xs)) - 1))] if xs else None

# ---- liquidity shocks ----
liq = defaultdict(list)
for r in rows('liquidity', daysfor(h0 - 900, h1)):
    for p, v in r['l'].items():
        if v: liq[p].append((r['t'] / 1000, v))
shocks = []
for p, xs in liq.items():
    xs.sort(); j = 0; last = None
    for i, (t, v) in enumerate(xs):
        while xs[j][0] < t - 900: j += 1
        hi = max(x[1] for x in xs[j:i + 1]); d = (1 - v / hi) * 100 if hi else 0
        if h0 <= t < h1 and d >= 20 and (last is None or t - last > 300): shocks.append((t, p, d)); last = t

# ---- render helpers ----
E = html.escape
day = UTC(h0).strftime('%Y-%m-%d')
hhmm = UTC(h0).strftime('%H%M')
primary_rel = f"{day}/{hhmm}-UTC.html"
fmt = lambda x, d=0: '–' if x is None else f'{x:,.{d}f}'
pc = lambda x: '–' if x is None else f'{x*100:.0f}%'
def table(head, body):
    if not body: return '<p class="m">None in this window.</p>'
    return '<table><tr>' + ''.join(f'<th>{E(h)}</th>' for h in head) + '</tr>' + ''.join('<tr>' + ''.join(f'<td>{c}</td>' for c in r) + '</tr>' for r in body) + '</table>'
symname = lambda t: t if not t.startswith('0x') else f'<code>{t[:10]}…</code>'
flow_rows = sorted(hour_flow.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))[:25]
partial = min(h1, now) < h1
hr_trig = [t for t in trig if h0 - 3 * 3600 <= t['m'] * 60 < h1]
S_all, S_e, S_b = summ([t['bps'] for t in trig]), summ([t['bps'] for t in trig if t['a'] == 'ETH']), summ([t['bps'] for t in trig if t['a'] == 'BTC'])
data_days = len(days_all); data_span_d = (now - first_ts) / 86400 if first_ts else 0
def srow(name, s): return [name, s['n'], pc(s.get('hit')), fmt(s.get('mean'), 1), fmt(s.get('med'), 1), fmt(s.get('t'), 2)]
up = (now - hb['startedAt'] / 1000) / 3600 if hb.get('startedAt') else None

# ---- deterministic analysis / thinking (no LLM) ----
def analysis_prose():
    lines = []
    lines.append(f"Window {UTC(h0).strftime('%Y-%m-%d %H:%M')}–{UTC(h1).strftime('%H:%M')} UTC covered {nev} decoded swap/liquidity events"
                 f"{' (partial hour)' if partial else ''}.")
    if lags:
        lines.append(f"Log lag after head was p50 {fmt(q(lags,.5))} ms / p90 {fmt(q(lags,.9))} ms"
                     f"{'; reconnects or disconnects in-window would bias late prints.' if disc or bfs else '; no disconnects in this hour.'}")
    else:
        lines.append("No lag samples in this hour — either quiet flow or the collector was offline for the window.")
    total_flow = sum(v[0] + v[1] for v in hour_flow.values())
    if flow_rows:
        top_tok, top_v = flow_rows[0]
        imb = (top_v[0] - top_v[1]) / (top_v[0] + top_v[1]) if top_v[0] + top_v[1] else 0
        lines.append(f"Largest token by notional was {top_tok} with ${top_v[0]+top_v[1]:,.0f} across {top_v[2]} legs "
                     f"(buy/sell imbalance {imb:+.2f}). Aggregate window notional across mapped tokens ≈ ${total_flow:,.0f}.")
    else:
        lines.append("No mapped token flow in this window.")
    resolved = [t for t in hr_trig if t['bps'] is not None]
    pending = [t for t in hr_trig if t['bps'] is None]
    if hr_trig:
        lines.append(f"Fade rule (|I|≥{TH} on ≥${VOL:,} over 5m, hold {HOLD}m, non-overlapping): "
                     f"{len(hr_trig)} triggers in the last 3h ending this window "
                     f"({len(resolved)} resolved, {len(pending)} still pending Binance close).")
        if resolved:
            hit = sum(1 for t in resolved if t['bps'] > 0) / len(resolved)
            mean = statistics.mean(t['bps'] for t in resolved)
            lines.append(f"Resolved 3h sample hit rate {hit*100:.0f}% with mean {mean:+.1f} bps "
                         f"(sign = fade direction). Treat as descriptive, not a live edge.")
    else:
        lines.append(f"No fade triggers met the |I|≥{TH} / ${VOL:,} bar in the trailing 3h.")
    if shocks:
        lines.append(f"{len(shocks)} liquidity shock(s) ≥20% vs 15m high on router pools; "
                     f"largest drop {max(d for _,_,d in shocks):.0f}%.")
    else:
        lines.append("No ≥20% liquidity shocks on router pools this hour.")
    if S_all.get('n'):
        lines.append(f"All-history fade research (non-overlapping): n={S_all['n']}, hit {pc(S_all.get('hit'))}, "
                     f"mean {fmt(S_all.get('mean'),1)} bps, median {fmt(S_all.get('med'),1)} bps"
                     f"{', t='+fmt(S_all.get('t'),2) if S_all.get('t') is not None else ''}. "
                     f"Data span {data_span_d:.2f} days across {data_days} day folder(s)"
                     f"{'; below the 7-day bar, so treat as unproven.' if data_span_d < 7 else '; 7+ days collected.'}")
    else:
        lines.append("Fade research has zero resolved triggers so far — waiting for more imbalance samples.")
    lines.append(f"Collector heartbeat: connected={hb.get('connected')}, headAgeMs={hb.get('headAgeMs')}, "
                 f"process up {fmt(up,2)} h (wall clock, includes suspends), box paused {fmt(paused_s/60,1)} min in window, restarts in window {sum(1 for r in restarts if h0 <= r < h1)} "
                 f"(total recorded {len(restarts)}). Minute heartbeats present {covered}/{round(span/60)}.")
    lines.append("Interpretation: this page is a research tape of observed Monad pool flow against Binance closes. "
                 "It does not execute, size, or recommend trades. Past hit rates do not predict future results.")
    return ''.join(f'<p>{E(x)}</p>' for x in lines)

CSS = """body{font:14px/1.45 ui-sans-serif,system-ui,sans-serif;background:#0b0b0f;color:#e4e4e7;max-width:1000px;margin:0 auto;padding:16px}h1{font-size:18px;word-break:break-all}h1 a{color:#a78bfa}h2{font-size:15px;margin-top:24px;border-bottom:1px solid #27272a;padding-bottom:4px}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}td,th{border-bottom:1px solid #1f1f23;padding:3px 6px;text-align:right}td:first-child,th:first-child{text-align:left}.m{color:#a1a1aa}code{color:#a1a1aa}.pos{color:#6ee7b7}.neg{color:#fda4af}.nav{margin:12px 0;display:flex;gap:12px;flex-wrap:wrap}.nav a{color:#a78bfa}"""

intro = f"""<p class="m">Window {UTC(h0).strftime('%Y-%m-%d %H:%M')}–{UTC(h1).strftime('%H:%M')} UTC ({LON(h0).strftime('%H:%M')}–{LON(h1).strftime('%H:%M %Z')} Europe/London){' · partial hour, data up to ' + UTC(now).strftime('%H:%M') + ' UTC' if partial else ''}. Generated {UTC(now).strftime('%Y-%m-%d %H:%M:%S')} UTC. Source: Monad RPC logs (public endpoints; box + GitHub Actions collectors merged) over all mapped Monad v2/v3/v4 pools + Binance 1m klines. Research data only, not trading advice; past results do not predict future results. <a href="../index.html">All tapes</a></p>"""

sections = [
    ('Feed health and latency', table(['Metric', 'Value'], [
        ['Decoded events in window', fmt(nev)], ['of which backfilled after a reconnect', fmt(nbf)],
        ['newHeads received', fmt(len(heads))], ['Max gap between heads (s)', fmt(max(gaps) / 1000 if gaps else None, 1)],
        ['Log lag after head p50 / p90 (ms)', f"{fmt(q(lags, .5))} / {fmt(q(lags, .9))}"],
        ['Disconnects / backfills', f"{len(disc)} / {len(bfs)}"], ['Unmapped pool logs (collector total)', fmt(hb.get('unmapped'))],
        ['Current: connected / head age (ms)', f"{hb.get('connected')} / {fmt(hb.get('headAgeMs'))}"]])),
    ('Collector uptime', table(['Metric', 'Value'], [
        ['Minutes with a heartbeat in window', f"{covered} of {round(span/60)}"],
        ['Box/VM paused in window (min)', fmt(paused_s / 60, 1)],
        ['Collector process up (h)', fmt(up, 2)],
        ['Restarts in window / total', f"{sum(1 for r in restarts if h0 <= r < h1)} / {len(restarts)}"],
        ['Data days collected', f"{data_days} day folder(s), {data_span_d:.2f} days of events (target ≥7)"]])),
    ('Flow imbalance per token (all pools, this window)',
     table(['Token', 'Buy $', 'Sell $', 'Imbalance', 'Legs'],
           [[symname(k), fmt(v[0]), fmt(v[1]), fmt((v[0]-v[1])/(v[0]+v[1]) if v[0]+v[1] else None, 2), v[2]] for k, v in flow_rows])),
    ('Fade triggers (last 3 hours, with later outcomes)',
     '<p class="m">Rule under test: all-pool 5m imbalance ≥ ' + str(TH) + ' in absolute value on ≥ $' + f'{VOL:,}' +
     ' volume → fade it; outcome = Binance close ' + str(HOLD) + 'm later, in bps, signed in the fade direction. Non-overlapping per asset (no new trigger for ' + str(HOLD) + 'm).</p>' +
     table(['Time (UTC)', 'Asset', '5m imbalance', '5m $', 'Fade', 'Outcome bps'],
           [[UTC(t['m']*60).strftime('%m-%d %H:%M'), t['a'], fmt(t['I'], 2), fmt(t['vol']), t['fade'],
             'pending' if t['bps'] is None else f"<span class={'pos' if t['bps']>0 else 'neg'}>{t['bps']:+.1f}</span>"] for t in hr_trig])),
    ('Liquidity shocks (router pools)',
     table(['Time (UTC)', 'Pool', 'Drop vs 15m high'], [[UTC(t).strftime('%H:%M:%S'), ROUTER.get(p, p), f'{d:.0f}%'] for t, p, d in shocks]) +
     table(['Time (UTC)', 'Pool', 'Event', 'USD'], [[UTC(e['ts']).strftime('%H:%M:%S'), ROUTER.get(e['p'], e['p']), e['k'], fmt(e.get('usd'))] for e in liq_ev[-30:]])),
    ('Running fade research (all data so far)',
     table(['Set', 'n', 'Hit rate', 'Mean bps', 'Median bps', 't'], [srow('All', S_all), srow('ETH', S_e), srow('BTC', S_b)]) +
     f'<p class="m">n counts resolved, non-overlapping triggers. {"Fewer than 7 days collected, so treat this as unproven." if data_span_d < 7 else "7+ days collected."} t is not corrected for testing several rules.</p>'),
    ('Analysis / thinking', analysis_prose()),
]

def wrap(rel, page_i, page_n, body_html, part_rels):
    url = BASE + rel
    nav = ''
    if page_n > 1:
        links = []
        if page_i > 0:
            links.append(f'<a href="{E(os.path.basename(part_rels[page_i-1]))}">← prev</a>')
        links.append(f'<span class="m">part {page_i+1} of {page_n}</span>')
        if page_i + 1 < page_n:
            links.append(f'<a href="{E(os.path.basename(part_rels[page_i+1]))}">next →</a>')
        nav = '<div class="nav">' + ' · '.join(links) + '</div>'
    title_bit = f" · part {page_i+1}/{page_n}" if page_n > 1 else ''
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>monaf signals tape {UTC(h0).strftime('%Y-%m-%d %H:%M')} UTC{title_bit}</title><style>{CSS}</style></head><body>
<h1><a href="{E(url)}">{E(url)}</a></h1>
{intro}{nav}
{body_html}
{nav}
</body></html>"""

# Pack sections into pages under PAGE_BUDGET (header ~2k + intro reserved)
# Force-split oversized single sections by row chunks if needed.
def section_html(title, body):
    return f'<h2>{E(title)}</h2>\n{body}'

packed = []  # list of list of section html strings
cur, cur_sz = [], 2500  # reserve for chrome
for title, body in sections:
    chunk = section_html(title, body)
    sz = len(chunk.encode('utf-8'))
    if cur and cur_sz + sz > PAGE_BUDGET:
        packed.append(cur); cur, cur_sz = [], 2500
    if sz > PAGE_BUDGET - 2500:
        # Split this section's tables roughly by half repeatedly until it fits, else dump alone
        packed.append(cur); cur = []
        packed.append([chunk]); cur, cur_sz = [], 2500
        continue
    cur.append(chunk); cur_sz += sz
if cur: packed.append(cur)
if not packed: packed = [[]]

n = len(packed)
part_rels = []
for i in range(n):
    if i == 0:
        part_rels.append(primary_rel)
    else:
        part_rels.append(f"{day}/{hhmm}-UTC-p{i+1}.html")

os.makedirs(os.path.join(site, day), exist_ok=True)
# Quality guard: never replace an existing page built from more data (e.g. box page vs Actions page, or vice versa)
import re as _re
_q_new = len(heads) * 10 + nev if 'nev' in dir() else len(heads) * 10
try: _m = _re.search(r'<!-- tape-quality (\d+) -->', open(os.path.join(site, part_rels[0])).read())
except Exception: _m = None
if _m and int(_m.group(1)) > _q_new and os.environ.get('TAPE_FORCE') != '1':
    sys.stderr.write(f"existing page quality {_m.group(1)} > new {_q_new}; keeping existing\n")
    for _p in sorted(glob.glob(os.path.join(site, day, f'{hhmm}-UTC*.html'))): print(os.path.relpath(_p, site))
    sys.exit(0)
# Remove stale continuation pages for this hour so a shorter rebuild does not leave orphans
for stale in glob.glob(os.path.join(site, day, f'{hhmm}-UTC*.html')):
    os.remove(stale)

for i, chunks in enumerate(packed):
    html_out = wrap(part_rels[i], i, n, '\n'.join(chunks), part_rels)
    if i == 0: html_out += f'\n<!-- tape-quality {_q_new} -->\n'
    open(os.path.join(site, part_rels[i]), 'w').write(html_out)

# Index: all hour parts + every other tape page, newest first
pages = sorted(glob.glob(f'{site}/20*/*-UTC*.html'), reverse=True)
items = ''.join(f'<li><a href="{E(os.path.relpath(p, site))}">{E(os.path.relpath(p, site)[:-5])}</a></li>' for p in pages)
open(os.path.join(site, 'index.html'), 'w').write(
    f'<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>monaf signals tape</title>'
    f'<style>body{{font:14px ui-sans-serif,system-ui;background:#0b0b0f;color:#e4e4e7;max-width:800px;margin:0 auto;padding:16px}}a{{color:#a78bfa}}</style></head>'
    f'<body><h1><a href="{BASE}">{BASE}</a></h1><p>Hourly tape of Monad all-pool signal activity and the 15m fade re-test. Research data only, not trading advice. '
    f'Hours that exceed ~{PAGE_BUDGET//1000}KB split into -p2, -p3… continuation pages listed here.</p><ul>{items}</ul></body></html>')

for rel in part_rels:
    print(rel)
