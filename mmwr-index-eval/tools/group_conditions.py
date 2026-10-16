"""Join reader records' free-text conditions to conditions_map.tsv (CONDITION_RUBRIC.md).

    python3 tools/group_conditions.py OUT.tsv RECORD.json [...] [--missing MISSING.tsv]

Writes one row per condition: id, term, canonical, role, groups. Applies the join-time rule of V18(a): a condition with
role `outcome` in a record whose vaccine_outcome_type is safety also maps to `vaccine safety`. Terms missing from the
map are listed on stderr, or with --missing written to a TSV in the map's columns (canonical and groups left blank, plus
record counts, roles and example ids) for the grouping pass to fill and append.
"""
import csv, json, sys

MAP = {r['term']: r for r in csv.DictReader(open('conditions_map.tsv'), delimiter='\t')}
# a term already written in canonical form also matches (first row per canonical term)
for _r in list(MAP.values()):
    if _r['canonical']:
        MAP.setdefault(_r['canonical'], _r)


def main(out, paths, missing_out=None):
    rows, missing = [], {}
    for p in paths:
        rec = json.load(open(p))
        safety = (rec.get('vaccine_outcome_type') or {}).get('v') == 'safety'
        for c in rec.get('conditions', []):
            term = ' '.join(c['v'].lower().split())
            m = MAP.get(term)
            if m is None:
                missing.setdefault(term, []).append((rec['id'], c['role']))
                groups, canonical = [], term
            else:
                groups, canonical = [g for g in m['groups'].split('|') if g], m['canonical']
            if safety and c['role'] == 'outcome' and 'vaccine safety' not in groups:
                groups.append('vaccine safety')
            rows.append({'id': rec['id'], 'term': term, 'canonical': canonical, 'role': c['role'], 'groups': '|'.join(groups)})
    with open(out, 'w', newline='') as o:
        w = csv.DictWriter(o, ['id', 'term', 'canonical', 'role', 'groups'], delimiter='\t', lineterminator='\n')
        w.writeheader(); w.writerows(rows)
    print(len(rows), 'conditions written to', out, file=sys.stderr)
    if missing and missing_out:
        # the grouping pass fills canonical and groups, then appends the rows (term, canonical, groups, note) to the map
        with open(missing_out, 'w', newline='') as o:
            w = csv.writer(o, delimiter='\t', lineterminator='\n')
            w.writerow(['term', 'canonical', 'groups', 'note', 'n_records', 'roles', 'example_ids'])
            for term, uses in sorted(missing.items(), key=lambda x: (-len({i for i, _ in x[1]}), x[0])):
                ids = sorted({i for i, _ in uses})
                w.writerow([term, '', '', '', len(ids), '|'.join(sorted({r for _, r in uses})), ' '.join(ids[:5])])
        print(len(missing), 'terms not in conditions_map.tsv ->', missing_out, file=sys.stderr)
    elif missing:
        print(len(missing), 'terms not in conditions_map.tsv:', '; '.join(sorted(missing)), file=sys.stderr)


if __name__ == '__main__':
    args = sys.argv[1:]
    miss = None
    if '--missing' in args:
        k = args.index('--missing'); miss = args[k + 1]; del args[k:k + 2]
    main(args[0], args[1:], miss)
