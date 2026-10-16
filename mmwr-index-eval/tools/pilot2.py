"""Pilot 2 (brief v4): two Opus reads per article at different effort levels, adjudicated; tests effort and routing.

    python tools/pilot2.py sample [all|priority]  # -> p2_sample.tsv, p2_batches.json (deterministic; default all)
    python tools/pilot2.py compare                # out/p2_low + out/p2_medium -> adj_packet_p2/, p2_mapping.json,
                                                  #    reports/p2_agreement.tsv
    python tools/pilot2.py score                  # adjudication_p2/ -> reports/p2_scores.tsv, reports/p2_routing.tsv,
                                                  #    reports/p2_brief_issues.md

Priority segment (BRIEF_V4_DECISIONS D18): full reports published 2016-2024. Writes only p2-specific paths.
See RUN_PILOT2.md.
"""
import collections, csv, glob, json, os, random, sys
sys.path.insert(0, 'tools')
from compare import norm, show

SEED = 20261014
BATCH = 4
ARMS = ('low', 'medium')
METHOD = {'data_focus', 'outcome_types', 'analytic_depth', 'has_regression', 'regression_families', 'procedure_classes',
          'has_test', 'has_standardization', 'has_weighting', 'comparison_framework', 'comparison_scale', 'inference',
          'study_design', 'groupwise_any', 'groupwise_assessments', 'ci_overlap'}
EQUITY = {'equity_level', 'equity_flag', 'equity_dimensions', 'equity_action', 'has_determinants', 'groupwise_dims'}
# best effort (D10, D11, D14, D19): everything else, e.g. data_source_type, setting, vaccine_outcome_type


def group_of(field):
    return 'method' if field in METHOD or field.startswith('rf_') else 'equity' if field in EQUITY else 'best effort'


def priority(row):
    return row['article_type'] == 'full_report' and '2016' <= row['date'][:4] <= '2024'


def sample(which='all'):
    rows = [r for r in csv.DictReader(open('sample.tsv'), delimiter='\t') if which == 'all' or priority(r)]
    out = []
    for st in sorted({r['stratum'] for r in rows}):
        part = sorted((r for r in rows if r['stratum'] == st), key=lambda r: r['id'])
        random.Random(f'{SEED}-{st}').shuffle(part)
        k = -(-len(part) // BATCH)
        for n, r in enumerate(part):
            out.append({'id': r['id'], 'stratum': st, 'article_type': r['article_type'], 'date': r['date'],
                        'priority': int(priority(r)), 'batch': f"P2{'R' if st == '2023-24' else 'O'}{n % k + 1:02d}"})
    out.sort(key=lambda r: (r['batch'], r['id']))
    with open('p2_sample.tsv', 'w', newline='') as o:
        w = csv.DictWriter(o, list(out[0]), delimiter='\t'); w.writeheader(); w.writerows(out)
    b = collections.defaultdict(list)
    for r in out:
        b[r['batch']].append(r['id'])
    json.dump(dict(sorted(b.items())), open('p2_batches.json', 'w'), indent=1)
    print(len(out), 'articles in', len(b), 'batches;', sum(r['priority'] for r in out), 'in the priority segment')


def rows():
    return list(csv.DictReader(open('p2_sample.tsv'), delimiter='\t'))


def compare():
    os.makedirs('reports', exist_ok=True)
    tally = collections.defaultdict(collections.Counter)
    mapping, n_any, missing = {}, 0, []
    for s in rows():
        i = s['id']; paths = {a: f'out/p2_{a}/{i}.json' for a in ARMS}
        if not all(os.path.exists(p) for p in paths.values()):
            missing.append(i); continue
        rec = {a: json.load(open(p)) for a, p in paths.items()}
        n = {a: norm(r) for a, r in rec.items()}
        dis = [f for f in n['low'] if n['low'][f] != n['medium'][f]]
        n_any += bool(dis)
        for f in n['low']:
            tally[(s['stratum'], group_of(f), f)]['disagree' if f in dis else 'agree'] += 1
        labs = ['A', 'B']; who = list(ARMS)
        random.Random(f'{SEED}-{i}').shuffle(who)
        mapping[i] = {labs[k]: who[k] for k in range(2)}
        lab_of = {who[k]: labs[k] for k in range(2)}
        d = f'adj_packet_p2/{i}'; os.makedirs(d, exist_ok=True)
        for k, a in enumerate(who):
            json.dump({**rec[a], 'reader': {'label': labs[k]}}, open(f'{d}/reader_{labs[k]}.json', 'w'), ensure_ascii=False, indent=1)
        json.dump([{'field': f, lab_of['low']: show(n['low'][f]), lab_of['medium']: show(n['medium'][f])}
                   for f in dis if group_of(f) != 'best effort'],
                  open(f'{d}/disagreements.json', 'w'), ensure_ascii=False, indent=1)
    json.dump(mapping, open('p2_mapping.json', 'w'), indent=1)
    with open('reports/p2_agreement.tsv', 'w') as o:
        o.write('stratum\tgroup\tfield\tagree\tdisagree\n')
        for (st, g, f), c in sorted(tally.items()):
            o.write(f"{st}\t{g}\t{f}\t{c['agree']}\t{c['disagree']}\n")
    print(len(mapping), 'articles compared;', n_any, 'with any disagreement (best-effort fields are not sent to adjudication);',
          'missing:', ' '.join(missing) or 'none')


def triggers(rec):
    """D18 routing signals in one record."""
    t = list(rec.get('uncertain_fields', []))
    if rec['equity'].get('level_confidence') in ('medium', 'low'):
        t.append(f"level_confidence:{rec['equity']['level_confidence']}")
    if any(p.get('alternative_reading') for p in rec['procedures']):
        t.append('alternative_reading')
    return t


def routed(rec, is_priority):
    """Owner's choice (D18 b): any signal routes a priority-segment report; others need a strong signal."""
    t = triggers(rec)
    if is_priority:
        return bool(t)
    return rec['equity'].get('level_confidence') == 'low' or len(rec.get('uncertain_fields', [])) >= 2


def score():
    mapping = json.load(open('p2_mapping.json')); meta = {r['id']: r for r in rows()}
    err = collections.defaultdict(collections.Counter); n = collections.defaultdict(collections.Counter)
    errors_by = collections.defaultdict(lambda: collections.Counter())   # (arm, id) -> method/equity error count
    issues, done = [], set()
    for f in sorted(glob.glob('adjudication_p2/*.json')):
        a = json.load(open(f)); i = a['id']; lab = mapping[i]; done.add(i); m = meta[i]
        seg = 'priority' if m['priority'] == '1' else 'other'
        rec = {arm: norm(json.load(open(f'out/p2_{arm}/{i}.json'))) for arm in ARMS}
        ruled = {d['field']: d for d in a.get('fields', [])}; agreed = {e['field'] for e in a.get('agreed_errors') or []}
        for fld in rec['low']:
            g = group_of(fld)
            if g == 'best effort':
                continue
            same = rec['low'][fld] == rec['medium'][fld]
            for arm in ARMS:
                if same:
                    wrong = fld in agreed
                elif fld in ruled:
                    wrong = arm not in {lab[l] for l in ruled[fld].get('correct', []) if l in lab}
                else:
                    continue
                errors_by[(arm, i)][g] += wrong
                for key in ((seg, m['stratum'], g), (seg, 'all', g), ('all', 'all', g), ('all', 'all', '*')):
                    n[key][arm] += 1; err[key][arm] += wrong
        if a.get('brief_issues'):
            issues.append(f'- {i}: {a["brief_issues"]}')
    with open('reports/p2_scores.tsv', 'w') as o:
        o.write('segment\tstratum\tgroup\tlow_err\tmedium_err\tn\n')
        for key in sorted(n):
            o.write('\t'.join(key) + '\t' + '\t'.join(f"{err[key][a] / n[key][a]:.3f}" for a in ARMS) + f"\t{n[key]['low']}\n")
    # routing: if each arm were the single primary read, how many of its method/equity errors sit in routed articles?
    with open('reports/p2_routing.tsv', 'w') as o:
        o.write('primary_arm\tsegment\tarticles\trouted\terrors\terrors_in_routed\n')
        for arm in ARMS:
            for seg in ('priority', 'other', 'all'):
                ids = [i for i in meta if seg == 'all' or (meta[i]['priority'] == '1') == (seg == 'priority')]
                ids = [i for i in ids if os.path.exists(f'out/p2_{arm}/{i}.json')]
                r = {i: routed(json.load(open(f'out/p2_{arm}/{i}.json')), meta[i]['priority'] == '1') for i in ids}
                e = {i: sum(errors_by[(arm, i)].values()) for i in ids}
                o.write(f"{arm}\t{seg}\t{len(ids)}\t{sum(r.values())}\t{sum(e.values())}\t{sum(e[i] for i in ids if r[i])}\n")
    open('reports/p2_brief_issues.md', 'w').write('\n'.join(issues) + '\n')
    print('adjudicated', len(done), 'of', len(mapping))
    for key in sorted(k for k in n if k[0] == 'all' or k[1] == 'all'):
        print(key, {a: f"{err[key][a] / n[key][a]:.3f}" for a in ARMS}, 'n =', n[key]['low'])
    print(open('reports/p2_routing.tsv').read())


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else ''
    if cmd == 'sample':
        sample(sys.argv[2] if len(sys.argv) > 2 else 'all')
    else:
        {'compare': compare, 'score': score}[cmd]()
