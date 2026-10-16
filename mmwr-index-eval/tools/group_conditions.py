"""Join reader records' free-text conditions to conditions_map.tsv (CONDITION_RUBRIC.md).

    python3 tools/group_conditions.py OUT.tsv RECORD.json [...]

Writes one row per condition: id, term, canonical, role, groups. Applies the join-time rule of V18(a): a condition with
role `outcome` in a record whose vaccine_outcome_type is safety also maps to `vaccine safety`. Terms missing from the
map are listed on stderr so the grouping pass can add them.
"""
import csv, json, sys

MAP = {r['term']: r for r in csv.DictReader(open('conditions_map.tsv'), delimiter='\t')}


def main(out, paths):
    rows, missing = [], set()
    for p in paths:
        rec = json.load(open(p))
        safety = (rec.get('vaccine_outcome_type') or {}).get('v') == 'safety'
        for c in rec.get('conditions', []):
            term = ' '.join(c['v'].lower().split())
            m = MAP.get(term)
            if m is None:
                missing.add(term)
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
    if missing:
        print(len(missing), 'terms not in conditions_map.tsv:', '; '.join(sorted(missing)), file=sys.stderr)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:])
