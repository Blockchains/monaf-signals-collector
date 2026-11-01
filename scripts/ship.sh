#!/usr/bin/env bash
# ship.sh DATA_DIR WORKDIR SOURCE  -- shard new data into the `data` branch and push (gitleaks first; retry on races).
# WORKDIR is a clone of this repo (created on first use). Remote auth comes from the caller's git credentials.
set -uo pipefail
DATA=$1; W=$2; SRC=$3; REMOTE=${SHIP_REMOTE:-https://github.com/Blockchains/monaf-signals-collector.git}
GITLEAKS=${GITLEAKS:-gitleaks}; HERE=$(cd "$(dirname "$0")" && pwd)
if [ ! -d "$W/.git" ]; then
  git clone -q --single-branch --branch data --depth 1 "$REMOTE" "$W" 2>/dev/null || { git init -q -b data "$W"; git -C "$W" remote add origin "$REMOTE"; }
fi
cd "$W"; git config user.name "monaf collector ($SRC)"; git config user.email "collector@users.noreply.github.com"
n=$(python3 "$HERE/ship.py" "$DATA" "$W" "$SRC" "$W/.ship-state-$SRC.json") || exit 1
grep -qx ".ship-state-*" .git/info/exclude 2>/dev/null || echo ".ship-state-*" >> .git/info/exclude
if [ "$n" -gt 0 ]; then git add -A data && git commit -qm "shards $SRC $(date -u +%FT%TZ) ($n)" || true; fi
git rev-parse -q --verify HEAD >/dev/null || exit 0
"$GITLEAKS" detect --source . --redact --no-banner --log-opts="origin/data..HEAD" >/dev/null 2>&1 || \
  "$GITLEAKS" detect --source . --redact --no-banner --log-opts="-20" >/dev/null 2>&1 || { echo "gitleaks found something - NOT pushing"; exit 2; }
for i in 1 2 3 4 5 6; do
  git push -q origin HEAD:data 2>/dev/null && { echo "pushed $n shards"; exit 0; }
  git fetch -q --depth 50 origin data 2>/dev/null && git rebase -q origin/data 2>/dev/null || git rebase --abort 2>/dev/null
  sleep $(( i * 2 + RANDOM % 3 ))
done
echo "push failed (will retry next round)"; exit 1
