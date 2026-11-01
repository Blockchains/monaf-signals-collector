#!/usr/bin/env python3
"""Merge all shards of a `data` branch checkout into plain DATA_OUT/<day>/<type>.jsonl, deduped across sources:
events/unmapped by (tx hash, log index); heads by block (earliest arrival wins); others by exact row.
Usage: materialize.py REPO_DIR DATA_OUT [DAYS_BACK]"""
import gzip, json, os, sys, glob, time
repo, out = sys.argv[1], sys.argv[2]; back = int(sys.argv[3]) if len(sys.argv) > 3 else 0
days = sorted(os.path.basename(p) for p in glob.glob(f'{repo}/data/20*'))
if back: days = days[-back:]
KEY = {'events': lambda r: (r.get('tx'), str(r.get('i'))), 'unmapped': lambda r: (r.get('tx'), str(r.get('i'))), 'heads': lambda r: r.get('b')}
SORT = {'events': lambda r: (r.get('b', 0), int(str(r.get('i', 0)), 0) if isinstance(r.get('i'), str) else r.get('i', 0)), 'unmapped': lambda r: (r.get('b', 0),), 'heads': lambda r: r.get('recv', 0)}
tot = {}
for d in days:
    for td in sorted(glob.glob(f'{repo}/data/{d}/*/')):
        typ = os.path.basename(td.rstrip('/')); seen = {}
        for f in sorted(glob.glob(td + '*.jsonl.gz')):
            try:
                with gzip.open(f, 'rt') as g:
                    for line in g:
                        line = line.strip()
                        if not line: continue
                        try: r = json.loads(line)
                        except Exception: continue
                        k = KEY[typ](r) if typ in KEY else line
                        if k in seen:
                            if typ == 'heads' and r.get('recv', 9e18) < seen[k][0].get('recv', 9e18): seen[k] = (r, line)
                            continue
                        seen[k] = (r, line)
            except (OSError, EOFError): continue
        rows = list(seen.values())
        rows.sort(key=lambda x: SORT[typ](x[0]) if typ in SORT else (x[0].get('t', 0) if isinstance(x[0], dict) else 0))
        os.makedirs(f'{out}/{d}', exist_ok=True)
        with open(f'{out}/{d}/{typ}.jsonl', 'w') as fh:
            for _, line in rows: fh.write(line + '\n')
        tot[typ] = tot.get(typ, 0) + len(rows)
print(json.dumps(tot))
