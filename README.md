# monaf-signals-collector
Redundant Monad signals collector (public Monad RPC only, no secrets). Runs on the box and (optionally) in GitHub Actions.
Both write append-only gzipped 5-min shards to the `data` branch (`data/<day>/<type>/<source>-<ms>.jsonl.gz`);
`scripts/materialize.py` merges them deduped (events by tx+log index, heads by block). The hourly tape workflow
(`deploy/monaf-tape-workflow.yml`, installed in Blockchains/monaf-tape) builds pages from the merged data.
Always-on Actions mode is gated by repo variable `ALWAYS_ON=true` (see cost note in collector.yml).
