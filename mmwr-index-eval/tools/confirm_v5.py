"""Brief v5 confirmation run: one v5 read per article, scored against pilot 2's reference values.

    python3 tools/confirm_v5.py sample     # -> cv5_sample.tsv, cv5_batches.json (articles cited in BRIEF_V5_DECISIONS.md)
    python3 tools/confirm_v5.py compare    # out/v5/ vs pilot-2 reference -> adj_packet_cv5/, cv5_mapping.json, reports/cv5_match.tsv
    python3 tools/confirm_v5.py score      # adjudication_cv5/ -> reports/cv5_scores.tsv, reports/cv5_brief_issues.md

Reference value per method or equity field, from pilot 2: the adjudicated verdict where the two reads disagreed, the agreed
value otherwise (or the adjudicator's correction when listed in agreed_errors). Every field of the sampled articles has one,
because pilot 2 adjudicated every article with a method or equity disagreement. A v5 value that differs from the reference
goes to a v5 adjudicator, blinded as A/B, who rules under brief v5: the reference may be wrong under v5 by design.
See RUN_CONFIRM_V5.md.
"""
import collections, csv, glob, json, os, random, re, sys
sys.path.insert(0, 'tools')
from compare import norm, show
from pilot2 import group_of, routed, V5_ONLY

SEED = 20261016
BATCH = 4


def meta():
    return {r['id']: r for r in csv.DictReader(open('p2_sample.tsv'), delimiter='\t')}


def sample():
    ids = sorted(set(re.findall(r'mm\d{4}[ae]\d', open('BRIEF_V5_DECISIONS.md').read())))
    m = meta(); out = []
    for seg in ('1', '0'):
        part = [i for i in ids if m[i]['priority'] == seg]
        random.Random(f'{SEED}-{seg}').shuffle(part)
        k = -(-len(part) // BATCH)
        for n, i in enumerate(part):
            out.append({'id': i, 'stratum': m[i]['stratum'], 'article_type': m[i]['article_type'], 'priority': seg,
                        'adjudicated_p2': int(os.path.exists(f'adjudication_p2/{i}.json')),
                        'batch': f"V5{'P' if seg == '1' else 'O'}{n % k + 1:02d}"})
    out.sort(key=lambda r: (r['batch'], r['id']))
    with open('cv5_sample.tsv', 'w', newline='') as o:
        w = csv.DictWriter(o, list(out[0]), delimiter='\t', lineterminator='\n'); w.writeheader(); w.writerows(out)
    b = collections.defaultdict(list)
    for r in out:
        b[r['batch']].append(r['id'])
    json.dump(dict(sorted(b.items())), open('cv5_batches.json', 'w'), indent=1)
    print(len(out), 'articles in', len(b), 'batches;', sum(r['priority'] == '1' for r in out), 'priority;',
          sum(r['adjudicated_p2'] for r in out), 'adjudicated in pilot 2')


def same(a, b):
    """Compare a shown value with an adjudicator's verdict (lists of strings, or scalars written as JSON or text)."""
    if isinstance(a, list) or isinstance(b, list):
        return sorted(map(str, a if isinstance(a, list) else [a])) == sorted(map(str, b if isinstance(b, list) else [b]))
    if a is None or b is None:  # null is not the string "none" (e.g. nonindependence_approach)
        return a in (None, 'null') and b in (None, 'null')
    return str(a).lower() == str(b).lower()


P2_MAP = json.load(open('p2_mapping.json'))


def reference(i):
    """Pilot-2 reference per scored field: (acceptable values, source). Where the adjudicator marked several readers right
    (an ambiguity), each of their values is acceptable, besides the verdict."""
    lo = {k: v for k, v in norm(json.load(open(f'out/p2_low/{i}.json'))).items() if k not in V5_ONLY}
    me = {k: v for k, v in norm(json.load(open(f'out/p2_medium/{i}.json'))).items() if k not in V5_ONLY}
    adj = json.load(open(f'adjudication_p2/{i}.json')) if os.path.exists(f'adjudication_p2/{i}.json') else {'fields': []}
    ruled = {d['field']: d for d in adj.get('fields', [])}
    arm = {'low': lo, 'medium': me}
    fixed = {e['field']: e['verdict'] for e in adj.get('agreed_errors') or []}
    ref = {}
    for f in lo:
        if group_of(f) == 'best effort':
            continue
        if lo[f] == me[f]:
            ref[f] = ([fixed[f]], 'agreed_error') if f in fixed else ([show(lo[f])], 'agreed')
        elif f in ruled:
            ok = [show(arm[P2_MAP[i][l]][f]) for l in ruled[f].get('correct', []) if l in P2_MAP[i]]
            ref[f] = ([ruled[f]['verdict']] + ok, 'adjudicated')
    return ref


def compare():
    m = {r['id']: r for r in csv.DictReader(open('cv5_sample.tsv'), delimiter='\t')}
    rows, mapping, missing = [], {}, []
    for i in m:
        p = f'out/v5/{i}.json'
        if not os.path.exists(p):
            missing.append(i); continue
        rec = json.load(open(p)); v5 = norm(rec); ref = reference(i)
        mism = [f for f, (rv, src) in ref.items() if not any(same(show(v5[f]), x) for x in rv)]
        for f, (rv, src) in ref.items():
            rows.append({'id': i, 'priority': m[i]['priority'], 'group': group_of(f), 'field': f, 'source': src,
                         'match': int(f not in mism)})
        labs = ['A', 'B']; who = ['v5', 'reference']
        random.Random(f'{SEED}-{i}').shuffle(who)
        mapping[i] = {labs[k]: who[k] for k in range(2)}; lab = {who[k]: labs[k] for k in range(2)}
        d = f'adj_packet_cv5/{i}'; os.makedirs(d, exist_ok=True)
        json.dump({**rec, 'reader': {'label': lab['v5']}}, open(f'{d}/record_{lab["v5"]}.json', 'w'), ensure_ascii=False, indent=1)
        json.dump([{'field': f, lab['v5']: show(v5[f]), lab['reference']: ref[f][0][0]} for f in mism],
                  open(f'{d}/disagreements.json', 'w'), ensure_ascii=False, indent=1)
    json.dump(mapping, open('cv5_mapping.json', 'w'), indent=1)
    os.makedirs('reports', exist_ok=True)
    with open('reports/cv5_match.tsv', 'w', newline='') as o:
        w = csv.DictWriter(o, ['id', 'priority', 'group', 'field', 'source', 'match'], delimiter='\t', lineterminator='\n',
                           extrasaction='ignore')
        w.writeheader(); w.writerows(rows)
    n = len(rows); k = sum(r['match'] for r in rows)
    print(len(mapping), 'articles compared;', n - k, 'of', n, 'scored fields differ from the pilot-2 reference;',
          sum(1 for i in mapping if json.load(open(f'adj_packet_cv5/{i}/disagreements.json'))), 'articles to adjudicate;',
          'missing:', ' '.join(missing) or 'none')


def score():
    mapping = json.load(open('cv5_mapping.json')); m = meta()
    match = list(csv.DictReader(open('reports/cv5_match.tsv'), delimiter='\t'))
    ruled = {}
    issues = []
    for f in sorted(glob.glob('adjudication_cv5/*.json')):
        a = json.load(open(f)); i = a['id']; lab = mapping[i]
        for d in a.get('fields', []):
            ok = {lab[l] for l in d.get('correct', []) if l in lab}
            ruled[(i, d['field'])] = ('v5' in ok, 'reference' in ok, d.get('category'))
        if a.get('brief_issues'):
            issues.append(f'- {i}: {a["brief_issues"]}')
    n = collections.Counter(); err = collections.Counter(); cat = collections.Counter(); unruled = 0
    for r in match:
        for key in ((r['priority'], r['group']), ('all', r['group']), ('all', '*')):
            n[key] += 1
            if r['match'] == '1':
                continue
            res = ruled.get((r['id'], r['field']))
            if res is None:
                unruled += key == ('all', '*'); continue
            v5_ok, ref_ok, c = res
            err[key] += not v5_ok
            if key == ('all', '*'):
                cat[('v5 right, reference wrong' if v5_ok and not ref_ok else 'both defensible' if v5_ok
                     else 'v5 wrong') + f' / {c}'] += 1
    with open('reports/cv5_scores.tsv', 'w') as o:
        o.write('segment\tgroup\tv5_err\tn\n')
        for key in sorted(n):
            o.write(f"{key[0]}\t{key[1]}\t{err[key] / n[key]:.3f}\t{n[key]}\n")
        o.write('\noutcome of differing fields / adjudicator category\tcount\n')
        for k, v in sorted(cat.items()):
            o.write(f'{k}\t{v}\n')
    with open('reports/cv5_scores.tsv', 'a') as o:  # V17 routing on the v5 reads
        o.write('\nsegment\tarticles\trouted (V17)\n')
        for seg in ('1', '0'):
            ids = [i for i in mapping if m[i]['priority'] == seg]
            k = sum(routed(json.load(open(f'out/v5/{i}.json')), seg == '1') for i in ids)
            o.write(f"{'priority' if seg == '1' else 'other'}\t{len(ids)}\t{k}\n")
    open('reports/cv5_brief_issues.md', 'w').write('\n'.join(issues) + '\n')
    print(open('reports/cv5_scores.tsv').read(), 'differing fields not yet adjudicated:', unruled)


if __name__ == '__main__':
    {'sample': sample, 'compare': compare, 'score': score}[sys.argv[1]]()
