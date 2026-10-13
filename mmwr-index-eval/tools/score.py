"""Score both readers against adjudication, by arm and stratum.

    python tools/score.py     # -> reports/scores.tsv, reports/brief_issues.md; summary printed

An agreed field counts as correct for both readers unless listed in agreed_errors; a disputed field counts as
correct for the labels the adjudicator ruled correct. Articles not adjudicated are skipped.
"""
import collections, csv, glob, json, sys
sys.path.insert(0, 'tools')
from compare import norm

mapping = json.load(open('adj_mapping.json'))
sample = {r['id']: r for r in csv.DictReader(open('sample.tsv'), delimiter='\t')}
err = collections.defaultdict(collections.Counter); n = collections.defaultdict(collections.Counter)
issues = []
for f in sorted(glob.glob('adjudication/*.json')):
    a = json.load(open(f)); i = a['id']; lab = mapping[i]; arm = sample[i]['arm']; st = sample[i]['stratum']
    paths = {'structural': f'out/haiku/{i}.json', 'judgment': f'out/opus/{i}.json' if arm == 'A' else f'out/merged/{i}.json'}
    rec = {k: norm(json.load(open(p))) for k, p in paths.items()}
    ruled = {d['field']: d for d in a.get('fields', [])}; agreed = {e['field'] for e in a.get('agreed_errors', [])}
    for fld in rec['structural']:
        same = rec['structural'][fld] == rec['judgment'][fld]
        for role in rec:
            if same:
                wrong = fld in agreed
            elif fld in ruled:
                wrong = role not in {lab[l] for l in ruled[fld].get('correct', []) if l in lab}
            else:
                continue
            for key in ((arm, 'all', fld), (arm, st, fld), (arm, 'all', '*'), (arm, st, '*')):
                n[key][role] += 1; err[key][role] += wrong
    if a.get('brief_issues'):
        issues.append(f'- {i}: {a["brief_issues"]}')
with open('reports/scores.tsv', 'w') as o:
    o.write('arm\tstratum\tfield\tstructural_err\tjudgment_err\tn\n')
    for key in sorted(n):
        o.write('\t'.join(key) + '\t' + '\t'.join(f"{err[key][r] / n[key][r]:.3f}" for r in ('structural', 'judgment')) + f"\t{n[key]['structural']}\n")
open('reports/brief_issues.md', 'w').write('\n'.join(issues) + '\n')
for key in sorted(k for k in n if k[2] == '*'):
    print(key, {r: f"{err[key][r] / n[key][r]:.3f}" for r in ('structural', 'judgment')})
