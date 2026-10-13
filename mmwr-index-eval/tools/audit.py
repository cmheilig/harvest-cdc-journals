"""Arm-B audit: an independent, blind full read of a sample of arm-B articles, compared with the merged record.

    python tools/audit.py sample    # -> audit_sample.tsv, audit_batches.json (deterministic)
    python tools/audit.py compare   # out/merged + out/opus_audit -> adj_packet_audit/, audit_mapping.json,
                                    #    reports/audit_agreement.tsv
    python tools/audit.py score     # adjudication_audit/ -> reports/audit_scores.tsv, reports/audit_brief_issues.md

Writes only audit-specific paths; the pilot's adj_packet/, adj_mapping.json and reports/*.tsv are left untouched.
See RUN_ARMB_AUDIT.md.
"""
import collections, csv, glob, json, os, random, sys
sys.path.insert(0, 'tools')
from compare import norm, show

SEED = 20261013
N_DISPUTED = {'2023-24': 5, '1999-2001': 1}   # calibration articles, added to every undisputed arm-B article
BATCH = 4


def arm_b():
    return [r for r in csv.DictReader(open('sample.tsv'), delimiter='\t') if r['arm'] == 'B']


def disputed(i):
    return bool(json.load(open(f'adj_packet/{i}/disagreements.json')))


def sample():
    rows = []
    for st in ('2023-24', '1999-2001'):
        b = sorted((r for r in arm_b() if r['stratum'] == st), key=lambda r: r['id'])
        und = [r for r in b if not disputed(r['id'])]
        dis = [r for r in b if disputed(r['id'])]
        picked = random.Random(f'{SEED}-{st}').sample(dis, N_DISPUTED[st])
        chosen = [(r, 'undisputed') for r in und] + [(r, 'disputed') for r in sorted(picked, key=lambda r: r['id'])]
        random.Random(f'{SEED}-order-{st}').shuffle(chosen)
        k = -(-len(chosen) // BATCH)                     # number of batches, sizes as even as possible
        for n, (r, status) in enumerate(chosen):
            rows.append({'id': r['id'], 'stratum': st, 'pilot_status': status,
                         'batch': f"AU{'R' if st == '2023-24' else 'O'}{n % k + 1:02d}"})
    rows.sort(key=lambda r: (r['batch'], r['id']))
    with open('audit_sample.tsv', 'w', newline='') as o:
        w = csv.DictWriter(o, list(rows[0]), delimiter='\t'); w.writeheader(); w.writerows(rows)
    batches = collections.defaultdict(list)
    for r in rows:
        batches[r['batch']].append(r['id'])
    json.dump(dict(sorted(batches.items())), open('audit_batches.json', 'w'), indent=1)
    c = collections.Counter((r['stratum'], r['pilot_status']) for r in rows)
    print(len(rows), 'articles in', len(batches), 'batches;', dict(c))


def audit_rows():
    return list(csv.DictReader(open('audit_sample.tsv'), delimiter='\t'))


def compare():
    os.makedirs('reports', exist_ok=True)
    tally = collections.defaultdict(collections.Counter)
    mapping, n_any, missing = {}, 0, []
    for s in audit_rows():
        i, status = s['id'], s['pilot_status']
        pm, pu, ph = f'out/merged/{i}.json', f'out/opus_audit/{i}.json', f'out/haiku/{i}.json'
        if not (os.path.exists(pm) and os.path.exists(pu)):
            missing.append(i); continue
        rm, ru = json.load(open(pm)), json.load(open(pu))
        nm, nu, nh = norm(rm), norm(ru), norm(json.load(open(ph)))
        dis = [f for f in nm if nm[f] != nu[f]]
        n_any += bool(dis)
        for f in nm:
            tally[(status, f)]['merged_vs_audit'] += nm[f] != nu[f]
            tally[(status, f)]['haiku_vs_audit'] += nh[f] != nu[f]
            tally[(status, f)]['n'] += 1
        labs = ['A', 'B']; who = [('merged', rm), ('audit', ru)]
        random.Random(f'{SEED}-{i}').shuffle(who)
        mapping[i] = {labs[k]: who[k][0] for k in range(2)}
        lab_of = {who[k][0]: labs[k] for k in range(2)}
        d = f'adj_packet_audit/{i}'; os.makedirs(d, exist_ok=True)
        for k, (_, rec) in enumerate(who):
            json.dump({**rec, 'reader': {'label': labs[k]}}, open(f'{d}/reader_{labs[k]}.json', 'w'), ensure_ascii=False, indent=1)
        json.dump([{'field': f, lab_of['merged']: show(nm[f]), lab_of['audit']: show(nu[f])} for f in dis],
                  open(f'{d}/disagreements.json', 'w'), ensure_ascii=False, indent=1)
    json.dump(mapping, open('audit_mapping.json', 'w'), indent=1)
    with open('reports/audit_agreement.tsv', 'w') as o:
        o.write('pilot_status\tfield\tn\tmerged_vs_audit_disagree\thaiku_vs_audit_disagree\n')
        for (status, f), c in sorted(tally.items()):
            o.write(f"{status}\t{f}\t{c['n']}\t{c['merged_vs_audit']}\t{c['haiku_vs_audit']}\n")
    print(len(mapping), 'articles compared;', n_any, 'with any merged-vs-audit disagreement;',
          'missing records:', ' '.join(missing) or 'none')


def score():
    mapping = json.load(open('audit_mapping.json'))
    meta = {r['id']: r for r in audit_rows()}
    err = collections.defaultdict(collections.Counter); n = collections.defaultdict(collections.Counter)
    issues, done = [], set()
    for f in sorted(glob.glob('adjudication_audit/*.json')):
        a = json.load(open(f)); i = a['id']; lab = mapping[i]; done.add(i)
        status, st = meta[i]['pilot_status'], meta[i]['stratum']
        rec = {'merged': norm(json.load(open(f'out/merged/{i}.json'))), 'audit': norm(json.load(open(f'out/opus_audit/{i}.json')))}
        ruled = {d['field']: d for d in a.get('fields', [])}; agreed = {e['field'] for e in a.get('agreed_errors') or []}
        for fld in rec['merged']:
            same = rec['merged'][fld] == rec['audit'][fld]
            for who in rec:
                if same:
                    wrong = fld in agreed
                elif fld in ruled:
                    wrong = who not in {lab[l] for l in ruled[fld].get('correct', []) if l in lab}
                else:
                    continue
                for key in ((status, 'all', fld), (status, st, fld), (status, 'all', '*'), (status, st, '*'),
                            ('all', 'all', '*'), ('all', 'all', fld)):
                    n[key][who] += 1; err[key][who] += wrong
        if a.get('brief_issues'):
            issues.append(f'- {i}: {a["brief_issues"]}')
    with open('reports/audit_scores.tsv', 'w') as o:
        o.write('pilot_status\tstratum\tfield\tmerged_err\taudit_err\tn\n')
        for key in sorted(n):
            o.write('\t'.join(key) + '\t' + '\t'.join(f"{err[key][w] / n[key][w]:.3f}" for w in ('merged', 'audit'))
                    + f"\t{n[key]['merged']}\n")
    open('reports/audit_brief_issues.md', 'w').write('\n'.join(issues) + '\n')
    left = sorted(set(mapping) - done)
    print('adjudicated', len(done), 'of', len(mapping), '; not adjudicated:', ' '.join(left) or 'none')
    for key in sorted(k for k in n if k[2] == '*'):
        print(key, {w: f"{err[key][w] / n[key][w]:.3f}" for w in ('merged', 'audit')}, 'n =', n[key]['merged'])


if __name__ == '__main__':
    {'sample': sample, 'compare': compare, 'score': score}[sys.argv[1] if len(sys.argv) > 1 else '']()
