"""Compare the two readers per article, build blinded adjudication packets, and write the between-reader table.

    python tools/compare.py
      -> reports/agreement.tsv          per field: agree / disagree, by arm
      -> reports/between_readers.tsv    per article: both readers' key values side by side (for candidate selection)
      -> adj_packet/<id>/               reader_A.json, reader_B.json, disagreements.json (labels shuffled per article)
      -> adj_mapping.json               label -> reader (not to be opened by adjudicators)

Arm A pairs out/haiku with out/opus; arm B pairs out/haiku with out/merged (see tools/merge_review.py).
"""
import collections, csv, json, os, random

SEED = 20261012


def norm(r):
    rf = r.get('regression_fields') or {}
    eq = r['equity']; procs = r['procedures']
    f = {
        'data_focus': (r.get('data_focus') or {}).get('v'),
        'outcome_types': frozenset(x['v'] for x in r['outcome_types']),
        'analytic_depth': frozenset(x['v'] for x in r['analytic_depth']),
        'has_regression': any(p['class'] == 'regression' for p in procs),
        'regression_families': frozenset(p['regression_family'] for p in procs if p['class'] == 'regression'),
        'procedure_classes': frozenset(p['class'] for p in procs),
        'has_test': any(p['class'] == 'test' for p in procs),
        'has_standardization': any(p['class'] == 'standardization' for p in procs),
        'has_weighting': any(p['class'] == 'weighting' for p in procs),
        'comparison_framework': r['comparison']['framework'],
        'comparison_scale': r['comparison']['scale'],
        'inference': r['inference']['v'],
        'study_design': frozenset(x['v'] for x in r['study_design']),
        'condition_group': frozenset(x['v'] for x in r['condition_group']),
        'data_source_type': frozenset(x['v'] for x in r['data_source_type']),
        'setting': (r['setting'] or {}).get('v'),
        'vaccine_outcome_type': (r['vaccine_outcome_type'] or {}).get('v') if r['vaccine_outcome_type'] else None,
        'equity_level': eq['level'],
        'equity_flag': eq['level'] >= 3,
        'equity_dimensions': frozenset(d['v'] for d in eq['dimensions']),
        'equity_action': eq['equity_action']['v'],
        'has_determinants': bool((eq['determinants_of_health'] or {}).get('v')),
    }
    for k in ('nonindependence_approach', 'unadjusted_results', 'adjusted_results', 'interaction_modeled', 'sensitivity_analysis'):
        f['rf_' + k] = (rf.get(k) or {}).get('v') if rf else 'n/a'
    return f


def show(v):
    return sorted(map(str, v)) if isinstance(v, frozenset) else v


def pairs():
    """(id, arm, structural_path, judgment_path) for every article whose two records exist."""
    for s in csv.DictReader(open('sample.tsv'), delimiter='\t'):
        i, arm = s['id'], s['arm']
        a = f'out/haiku/{i}.json'
        b = f'out/opus/{i}.json' if arm == 'A' else f'out/merged/{i}.json'
        if os.path.exists(a) and os.path.exists(b):
            yield i, arm, s, a, b


def main():
    os.makedirs('reports', exist_ok=True)
    tally = collections.defaultdict(collections.Counter)
    mapping = {}
    rows = []
    for i, arm, s, pa, pb in pairs():
        ra, rb = json.load(open(pa)), json.load(open(pb))
        na, nb = norm(ra), norm(rb)
        dis = [{'field': f, 'X': show(na[f]), 'Y': show(nb[f])} for f in na if na[f] != nb[f]]
        for f in na:
            tally[(arm, f)]['agree' if na[f] == nb[f] else 'disagree'] += 1
        # blinded packet: X/Y -> A/B shuffled per article
        labs = ['A', 'B']; who = [('structural', pa, ra), ('judgment', pb, rb)]
        random.Random(f'{SEED}-{i}').shuffle(who)
        mapping[i] = {labs[k]: who[k][0] for k in range(2)}
        d = f'adj_packet/{i}'; os.makedirs(d, exist_ok=True)
        for k, (role, _, rec) in enumerate(who):
            json.dump({**rec, 'reader': {'label': labs[k]}}, open(f'{d}/reader_{labs[k]}.json', 'w'), ensure_ascii=False, indent=1)
        lab_of = {who[k][0]: labs[k] for k in range(2)}
        json.dump([{'field': x['field'], lab_of['structural']: x['X'], lab_of['judgment']: x['Y']} for x in dis],
                  open(f'{d}/disagreements.json', 'w'), ensure_ascii=False, indent=1)
        eb = rb['equity']
        rows.append({'id': i, 'arm': arm, 'stratum': s['stratum'], 'article_type': s['article_type'],
                     'data_focus_S': na['data_focus'], 'data_focus_J': nb['data_focus'],
                     'equity_level_S': na['equity_level'], 'equity_level_J': nb['equity_level'],
                     'J_confidence': eb.get('level_confidence'), 'J_alternative': (eb.get('level_alternative') or {}).get('level'),
                     'flag_either': na['equity_flag'] or nb['equity_flag'], 'flag_both': na['equity_flag'] and nb['equity_flag'],
                     'regression_families_S': '|'.join(show(na['regression_families'])), 'regression_families_J': '|'.join(show(nb['regression_families'])),
                     'study_design_S': '|'.join(show(na['study_design'])), 'study_design_J': '|'.join(show(nb['study_design'])),
                     'n_disagreements': len(dis), 'title': open(f'packets/{i}/article.txt').read().split('\n')[5][:120]})
    json.dump(mapping, open('adj_mapping.json', 'w'), indent=1)
    with open('reports/agreement.tsv', 'w') as o:
        o.write('arm\tfield\tagree\tdisagree\n')
        for (arm, f), c in sorted(tally.items()):
            o.write(f"{arm}\t{f}\t{c['agree']}\t{c['disagree']}\n")
    if rows:
        with open('reports/between_readers.tsv', 'w', newline='') as o:
            w = csv.DictWriter(o, list(rows[0]), delimiter='\t'); w.writeheader(); w.writerows(rows)
    print(len(rows), 'articles compared;', sum(r['n_disagreements'] > 0 for r in rows), 'with any disagreement')


if __name__ == '__main__':
    main()
