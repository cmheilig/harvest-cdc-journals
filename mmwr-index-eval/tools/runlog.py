"""Append one subagent's row to a run log (columns as run_log.tsv).

    python3 tools/runlog.py LOG BATCH ROLE TOKENS TOOL_USES DURATION_MS REFUSALS NOTES
                            [--ids id1,id2,...] [--outdir DIR] [--model MODEL]

TOKENS, TOOL_USES and DURATION_MS are the figures in the subagent's completion notice.
With --ids and --outdir, status is "ok k/n" for the ids whose DIR/<id>.json exists, and the
model is read from those records' reader.model unless --model is given.
"""
import argparse, json, os

p = argparse.ArgumentParser()
for a in ('log', 'batch', 'role', 'tokens', 'tool_uses', 'duration_ms', 'refusals', 'notes'):
    p.add_argument(a)
p.add_argument('--ids', default='')
p.add_argument('--outdir')
p.add_argument('--model')
a = p.parse_args()

ids = [i for i in a.ids.split(',') if i]
status, models = 'ok', set()
if ids and a.outdir:
    done = [i for i in ids if os.path.exists(f'{a.outdir}/{i}.json')]
    status = f'ok {len(done)}/{len(ids)}'
    models = {json.load(open(f'{a.outdir}/{i}.json')).get('reader', {}).get('model') for i in done}
model = a.model or ','.join(sorted(m for m in models if m))
row = [a.batch, a.role, model, status, a.tokens, a.tool_uses, f'{int(a.duration_ms) / 1000:.1f}', a.refusals, a.notes]
new = not os.path.exists(a.log)
with open(a.log, 'a') as o:
    if new:
        o.write('batch\trole\tmodel\tstatus\tsubagent_tokens\ttool_uses\tduration_s\trefusals\tnotes\n')
    o.write('\t'.join(row) + '\n')
print('\t'.join(row))
