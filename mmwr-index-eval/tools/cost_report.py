"""Cost per article joined to report characteristics, from prod_ledger.tsv. Prints counts and figures only.

    python3 tools/cost_report.py [VOL ...]        # default: every production volume in the ledger
      -> reports/cost_by_article.tsv              one row per article: cost, tokens, attempts, characteristics
      -> stdout                                   summary by volume and article type, and by record characteristics

An article's cost is the sum over its attempts (read, fix-ups, retry). The audit's second reads (VOL-audit) are quality
cost, not per article, and are summed separately. `fixed_usd` is the part of the cost that is the prefix (the same for
every request); `variable_usd` is the rest: packet input, output (record and thinking) and fix-up turns beyond the prefix.
Prefix tokens come from prod_calibration.tsv for the calibrated prefix, otherwise scaled from it by characters.
"""
import collections, csv, glob, json, os, statistics as st, sys

LEDGER, OUT = 'prod_ledger.tsv', 'reports/cost_by_article.tsv'
BATCH_INPUT, SYNC_INPUT = 2.00, 4.00          # $ per 1M input tokens (claude-opus-5-5), batch and standard
COLS = ['id', 'volume', 'article_type', 'brief_tag', 'cost_usd', 'fixed_usd', 'variable_usd', 'attempts', 'fixups',
        'packet_tokens', 'n_tables', 'output_tokens', 'data_focus', 'equity_level', 'n_procedures', 'has_regression',
        'n_groupwise', 'n_conditions', 'study_design']


def prefix_tokens():
    """{prefix sha: tokens}: measured where calibrated, otherwise scaled by characters from a calibrated prefix."""
    cal = list(csv.DictReader(open('prod_calibration.tsv'), delimiter='\t'))
    known = {r['prefix_sha']: int(r['prefix_tokens']) - 1 for r in cal}       # count included a 1-token user turn
    chars = {}
    for p in glob.glob('out/prod/*/prefix.json'):
        d = json.load(open(p))
        chars[d['sha']] = sum(len(b['text']) for b in d['system'])
    base = next((s for s in known if s in chars), None)
    for s, c in chars.items():
        if s not in known and base:
            known[s] = round(known[base] * c / chars[base])
    return known


def main(vols):
    led = list(csv.DictReader(open(LEDGER), delimiter='\t'))
    vols = vols or sorted({r['volume'] for r in led if r['volume'].isdigit()}, reverse=True)
    ptok = prefix_tokens()
    rows, audit = [], collections.Counter()
    for v in vols:
        frame = {r['id']: r['article_type'] for r in csv.DictReader(open(f'prod_packets/{v}/frame.tsv'), delimiter='\t')} \
            if os.path.exists(f'prod_packets/{v}/frame.tsv') else {}
        by = collections.defaultdict(list)
        for r in led:
            if r['volume'] == v and r['attempt'] in ('read', 'fixup', 'fixup2', 'retry'):
                by[r['id']].append(r)
            elif r['volume'] == f'{v}-audit':
                audit[v] += float(r['cost_usd'])
        for i, att in sorted(by.items()):
            read = next(r for r in att if r['attempt'] in ('read', 'retry'))
            pt = ptok.get(read['prefix_sha'], 0)
            tot_in = lambda r: int(r['input_tokens']) + int(r['cache_write_tokens']) + int(r['cache_read_tokens'])
            fixed = sum(pt * (BATCH_INPUT if r['environment'] == 'api-batch' else SYNC_INPUT) / 1e6 for r in att)
            cost = sum(float(r['cost_usd']) for r in att)
            rec_path = f'out/prod/{v}/{i}.json'
            rec = json.load(open(rec_path)) if os.path.exists(rec_path) else {}
            eq = rec.get('equity') or {}
            rows.append({
                'id': i, 'volume': v, 'article_type': frame.get(i, ''), 'brief_tag': read['brief_tag'],
                'cost_usd': f'{cost:.4f}', 'fixed_usd': f'{fixed:.4f}', 'variable_usd': f'{cost - fixed:.4f}',
                'attempts': len(att), 'fixups': sum(r['attempt'].startswith('fixup') for r in att),
                'packet_tokens': max(tot_in(read) - pt, 0),
                'n_tables': len(glob.glob(f'prod_packets/{v}/{i}/tables/*.csv')),
                'output_tokens': int(read['output_tokens']),
                'data_focus': (rec.get('data_focus') or {}).get('v', ''), 'equity_level': eq.get('level', ''),
                'n_procedures': len(rec.get('procedures', [])),
                'has_regression': int(any(p.get('class') == 'regression' for p in rec.get('procedures', []))),
                'n_groupwise': len(rec.get('groupwise', [])), 'n_conditions': len(rec.get('conditions', [])),
                'study_design': '|'.join(sorted(x['v'] for x in rec.get('study_design', []))),
            })
    os.makedirs('reports', exist_ok=True)
    with open(OUT, 'w', newline='') as o:
        w = csv.DictWriter(o, COLS, delimiter='\t', lineterminator='\n'); w.writeheader(); w.writerows(rows)
    print(f'{len(rows)} articles -> {OUT}; prefix tokens by sha: {ptok}')

    def line(label, g):
        c = [float(r['cost_usd']) for r in g]
        print(f"  {label:34s} n={len(g):4d}  ${sum(c):7.2f}  mean ${st.mean(c):.3f}  median ${st.median(c):.3f}  "
              f"variable ${st.mean(float(r['variable_usd']) for r in g):.3f}  packet {st.mean(r['packet_tokens'] for r in g):6,.0f}  "
              f"output {st.mean(r['output_tokens'] for r in g):5,.0f}  fix-ups {sum(r['fixups'] > 0 for r in g) / len(g):4.0%}")
    for v in vols:
        g = [r for r in rows if r['volume'] == v]
        if not g:
            continue
        print(f'vol {v} ({g[0]["brief_tag"]}); audit ${audit[v]:.2f}')
        line('all', g)
        for t, n in collections.Counter(r['article_type'] for r in g).most_common():
            line(t, [r for r in g if r['article_type'] == t])
    if rows:
        print('all volumes, by record characteristics')
        for label, key in (('data_focus', 'data_focus'), ('equity level', 'equity_level'), ('regression', 'has_regression'),
                           ('groupwise entries > 0', None)):
            vals = collections.Counter((r['n_groupwise'] > 0) if key is None else r[key] for r in rows)
            for val, _ in sorted(vals.items(), key=lambda x: str(x[0])):
                line(f'{label} = {val}', [r for r in rows if ((r['n_groupwise'] > 0) if key is None else r[key]) == val])


if __name__ == '__main__':
    main(sys.argv[1:])
