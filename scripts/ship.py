#!/usr/bin/env python3
"""Ship new bytes of DATA/<day>/<type>.jsonl as gzipped, uniquely-named shards into a checkout of the `data` branch:
   <repo>/data/<day>/<type>/<source>-<unix_ms>.jsonl.gz  (append-only; never rewrites a file => no merge conflicts).
Usage: ship.py DATA_DIR REPO_DIR SOURCE STATE_FILE. Offsets advance only for complete lines. Prints shard count."""
import gzip, json, os, sys, glob, time
data, repo, source, state_f = sys.argv[1:5]
TYPES = ('events', 'heads', 'health', 'binance1m', 'liquidity', 'unmapped')
try: st = json.load(open(state_f))
except Exception: st = {}
n = 0; ts = int(time.time() * 1000)
for f in sorted(glob.glob(f'{data}/20*/*.jsonl')):
    day, typ = os.path.basename(os.path.dirname(f)), os.path.basename(f)[:-6]
    if typ not in TYPES: continue
    size = os.path.getsize(f); off = st.get(f, 0)
    if off > size: off = 0  # file was replaced
    if size <= off: continue
    with open(f, 'rb') as fh: fh.seek(off); buf = fh.read(size - off)
    cut = buf.rfind(b'\n') + 1
    if cut <= 0: continue
    out = f'{repo}/data/{day}/{typ}/{source}-{ts}.jsonl.gz'
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with gzip.open(out, 'wb', compresslevel=6) as g: g.write(buf[:cut])
    st[f] = off + cut; n += 1
tmp = state_f + '.tmp'; json.dump(st, open(tmp, 'w')); os.replace(tmp, state_f)
print(n)
