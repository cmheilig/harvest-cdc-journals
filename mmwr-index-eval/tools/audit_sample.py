"""Per-volume quality assurance for production reads (PRODUCTION_PLAN.md §5, V19). Prints counts and ids only.

    python3 tools/audit_sample.py groupwise VOL   # mechanical check of groupwise entries -> audit/<VOL>/groupwise_check.tsv
    python3 tools/audit_sample.py draw VOL        # runs the check, then draws the audit -> audit/<VOL>/sample.tsv
    #   then: python3 tools/api_read.py submit VOL-audit; poll; collect VOL-audit   (second reads, same environment and effort)
    python3 tools/audit_sample.py compare VOL     # blinded A/B packets -> audit/<VOL>/adj_packet/, audit/<VOL>/mapping.json
    #   then: adjudicators write audit/<VOL>/adjudication/<id>.json (RUN_AUDIT.md)
    python3 tools/audit_sample.py score VOL       # -> audit/<VOL>/scores.tsv, audit/<VOL>/brief_issues.md; the gate figures

Strata in sample.tsv:
  random     5% of the volume's valid records (at least 10), seeded; second read, then adjudicated where they differ
  fallback   every record served by a model other than the production model (server-side fallback); second read
  groupwise  records with a groupwise entry that the mechanical check flagged, plus a random 10% of the unflagged ones
             (none of them already in `random`); one record, judged by an adjudicator, no second read
Scored fields are the method and equity fields of compare.norm (27, as in reports/cv5_scores.tsv); best-effort fields are
not scored. A production field is wrong when it differs from the second read and the adjudicator does not mark it correct.
"""
import collections, csv, glob, json, math, os, random, re, sys
sys.path.insert(0, 'tools')
from compare import norm, show
from pilot2 import group_of

SEED = 20261016
MODEL = 'claude-opus-5-5'
SHARE, MIN_N, GROUPWISE_SHARE = 0.05, 10, 0.10
GATE = 0.015                    # field error of the confirmation run's baseline (PRODUCTION_PLAN.md §4.2)
GROUPWISE_FIELDS = ('groupwise_any', 'groupwise_dims', 'groupwise_assessments')
TESTED = {'omnibus test', 'trend test', 'model joint test', 'interaction', 'pairwise (all pairs)', 'per-group vs referent only'}
FINER = {'pairwise (all pairs)', 'per-group vs referent only'}
COMPARED_ROLES = {'stratifier', 'primary comparison', 'cross-classifier'}
OMNIBUS_WORDS = re.compile(r'chi|rao.scott|wald|kruskal|anova|analysis of variance|trend|fisher', re.I)


def records(vol):
    """{id: record} for the volume's valid production records."""
    return {os.path.basename(f)[:-5]: json.load(open(f)) for f in sorted(glob.glob(f'out/prod/{vol}/mm*.json'))}


def types(vol):
    return {r['id']: r['article_type'] for r in csv.DictReader(open(f'prod_packets/{vol}/frame.tsv'), delimiter='\t')}


def write_tsv(path, rows, cols):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='') as o:
        w = csv.DictWriter(o, cols, delimiter='\t', lineterminator='\n', extrasaction='ignore')
        w.writeheader(); w.writerows(rows)


# ---------- groupwise mechanical check

def _norm_text(s):
    s = s.replace('’', "'").replace('‘', "'").replace('“', '"').replace('”', '"')
    return ' '.join(s.lower().split())


def haystack(vol, i):
    """Packet text with table cells one per line, normalized for quote matching."""
    d = f'prod_packets/{vol}/{i}'
    parts = [open(f'{d}/article.txt', encoding='utf-8').read()]
    for t in glob.glob(f'{d}/tables/*.csv'):
        parts += [c for row in csv.reader(open(t, encoding='utf-8')) for c in row]
    return _norm_text('\n'.join(parts))


def quote_found(q, hay):
    pieces = [p for p in re.split(r'\s*(?:›|…|\.\.\.)\s*', q) if p.strip()]
    return all(_norm_text(p) in hay for p in pieces)


def check_record(vol, i, rec):
    """Flags for one record: (entry index or '-', check, detail). Empty when nothing is flagged."""
    flags = []
    procs = rec['procedures']
    tests = [p for p in procs if p['class'] == 'test']
    has_test = bool(tests) or rec['inference']['v'] in ('tests', 'CIs and tests')
    dims = {d['v']: d['role'] for d in rec['equity']['dimensions']}
    hay = None
    for k, g in enumerate(rec['groupwise']):
        a = g['assessment']
        if a in TESTED and not has_test and not (a in ('model joint test', 'interaction') and
                                                 any(p['class'] == 'regression' for p in procs)):
            flags.append((k, 'assessment needs a test', a))
        if a == 'side by side' and tests:
            flags.append((k, 'side by side but the record has tests (rule 2?)', ','.join(p['name'] for p in tests)[:80]))
        if a in FINER and not g['resolves_to_finer']:
            flags.append((k, 'resolves_to_finer must be true', a))
        if a == 'trend test' and not g['ordered']:
            flags.append((k, 'trend test on unordered groups', ''))
        for dim in g['across']:
            if dim not in dims:
                flags.append((k, 'groupwise dimension missing from equity.dimensions', dim))
        hay = hay or haystack(vol, i)
        miss = sum(not quote_found(e['q'], hay) for e in g['ev'])
        if miss:
            flags.append((k, 'evidence quote not found in packet', f'{miss} of {len(g["ev"])}'))
    if not rec['groupwise'] and any(OMNIBUS_WORDS.search(p['name']) for p in tests) and \
            any(r in COMPARED_ROLES for r in dims.values()):
        flags.append(('-', 'no groupwise entry, but an omnibus-type test and a compared SPD (rule 2?)', ''))
    return flags


def cmd_groupwise(vol):
    recs = records(vol)
    rows, n_flag = [], collections.Counter()
    for i, rec in recs.items():
        for k, chk, det in check_record(vol, i, rec):
            rows.append({'id': i, 'entry': k, 'check': chk, 'detail': det}); n_flag[chk] += 1
    write_tsv(f'audit/{vol}/groupwise_check.tsv', rows, ['id', 'entry', 'check', 'detail'])
    with_gw = sum(bool(r['groupwise']) for r in recs.values())
    flagged = {r['id'] for r in rows}
    print(f'vol {vol}: {len(recs)} records, {with_gw} with groupwise entries; {len(flagged)} records flagged')
    for chk, c in n_flag.most_common():
        print(f'  {c:4d}  {chk}')
    return recs, flagged


# ---------- draw

def cmd_draw(vol):
    recs, flagged = cmd_groupwise(vol)
    if os.path.exists(f'audit/{vol}/sample.tsv'):
        sys.exit(f'audit/{vol}/sample.tsv exists; the draw is made once per volume')
    ids = sorted(recs)
    rng = random.Random(f'{SEED}-{vol}')
    k = max(MIN_N, math.ceil(SHARE * len(ids)))
    rand = set(rng.sample(ids, min(k, len(ids))))
    fallback = {i for i in ids if recs[i]['reader'].get('model') != MODEL} - rand
    gw = sorted(i for i in ids if recs[i]['groupwise'] and i not in rand | fallback)
    gw_flag = [i for i in gw if i in flagged]
    rest = [i for i in gw if i not in flagged]
    gw_rand = rng.sample(rest, min(len(rest), math.ceil(GROUPWISE_SHARE * len(rest)))) if rest else []
    t = types(vol)
    rows = [{'id': i, 'stratum': s, 'second_read': int(s != 'groupwise'), 'article_type': t.get(i, ''),
             'uncertain': int(bool(recs[i]['uncertain_fields'])), 'groupwise_flagged': int(i in flagged)}
            for s, group in (('random', rand), ('fallback', fallback), ('groupwise', set(gw_flag) | set(gw_rand)))
            for i in sorted(group)]
    write_tsv(f'audit/{vol}/sample.tsv', rows, ['id', 'stratum', 'second_read', 'article_type', 'uncertain', 'groupwise_flagged'])
    c = collections.Counter(r['stratum'] for r in rows)
    print(f'vol {vol}: drew random {c["random"]}, fallback {c["fallback"]}, groupwise {c["groupwise"]} '
          f'({len(gw_flag)} flagged, {len(gw_rand)} random); second reads {sum(r["second_read"] for r in rows)}')
    print('  random stratum by type: ' + ', '.join(f'{k} {v}' for k, v in
                                                  collections.Counter(t.get(i, '') for i in rand).most_common()))


# ---------- compare

def sample_rows(vol):
    return list(csv.DictReader(open(f'audit/{vol}/sample.tsv'), delimiter='\t'))


def scored(n):
    return [f for f in n if group_of(f) != 'best effort']


def cmd_compare(vol):
    recs = records(vol)
    base, mapping, missing, n_dis = f'audit/{vol}/adj_packet', {}, [], 0
    flags = collections.defaultdict(list)
    if os.path.exists(f'audit/{vol}/groupwise_check.tsv'):
        for r in csv.DictReader(open(f'audit/{vol}/groupwise_check.tsv'), delimiter='\t'):
            flags[r['id']].append(f"entry {r['entry']}: {r['check']}" + (f" ({r['detail']})" if r['detail'] else ''))
    for s in sample_rows(vol):
        i = s['id']; d = f'{base}/{i}'
        os.makedirs(d, exist_ok=True)
        if s['second_read'] == '1':
            p2 = f'out/prod_audit/{vol}/{i}.json'
            if not os.path.exists(p2):
                missing.append(i); continue
            a, b = recs[i], json.load(open(p2))
            na, nb = norm(a), norm(b)
            dis = [f for f in na if na[f] != nb[f]]
            who = ['prod', 'audit']
            random.Random(f'{SEED}-{vol}-{i}').shuffle(who)
            mapping[i] = {'A': who[0], 'B': who[1]}
            lab = {who[0]: 'A', who[1]: 'B'}
            for w, rec in (('prod', a), ('audit', b)):
                json.dump({**rec, 'reader': {'label': lab[w]}}, open(f'{d}/reader_{lab[w]}.json', 'w'), ensure_ascii=False, indent=1)
            json.dump([{'field': f, lab['prod']: show(na[f]), lab['audit']: show(nb[f])} for f in dis],
                      open(f'{d}/disagreements.json', 'w'), ensure_ascii=False, indent=1)
            n_dis += bool(dis)
        else:                                       # groupwise look: one record, its groupwise values to judge
            mapping[i] = {'A': 'prod'}
            na = norm(recs[i])
            json.dump({**recs[i], 'reader': {'label': 'A'}}, open(f'{d}/reader_A.json', 'w'), ensure_ascii=False, indent=1)
            json.dump([{'field': f, 'A': show(na[f])} for f in GROUPWISE_FIELDS],
                      open(f'{d}/disagreements.json', 'w'), ensure_ascii=False, indent=1)
            json.dump(flags.get(i, []), open(f'{d}/mechanical_flags.json', 'w'), ensure_ascii=False, indent=1)
    json.dump(mapping, open(f'audit/{vol}/mapping.json', 'w'), indent=1)
    looks = sum(1 for v in mapping.values() if 'B' not in v)
    print(f'vol {vol}: {len(mapping) - looks} second-read pairs ({n_dis} with differences), {looks} groupwise looks; '
          f'missing second reads: {" ".join(missing) or "none"}')


# ---------- score

def cmd_score(vol):
    recs = records(vol)
    mapping = json.load(open(f'audit/{vol}/mapping.json'))
    meta = {r['id']: r for r in sample_rows(vol)}
    n = collections.Counter(); err = collections.Counter(); err_audit = collections.Counter()
    by_flag = collections.defaultdict(collections.Counter); cats = collections.Counter()
    pending, issues, looks = [], [], collections.Counter()
    for i, lab in mapping.items():
        p = f'audit/{vol}/adjudication/{i}.json'
        adj = json.load(open(p)) if os.path.exists(p) else None
        if adj and adj.get('brief_issues'):
            issues.append(f"- {i}: {adj['brief_issues']}")
        ruled = {d['field']: d for d in (adj or {}).get('fields', [])}
        if 'B' not in lab:                          # groupwise look
            if adj is None:
                pending.append(i); continue
            for f in GROUPWISE_FIELDS:
                d = ruled.get(f)
                looks['fields'] += 1
                looks['wrong'] += bool(d) and 'A' not in d.get('correct', [])
            continue
        dis = {d['field'] for d in json.load(open(f'audit/{vol}/adj_packet/{i}/disagreements.json'))}
        inv = {v: k for k, v in lab.items()}
        flag = 'flagged' if meta[i]['uncertain'] == '1' else 'unflagged'
        for f in scored(norm(recs[i])):
            g = group_of(f)
            for key in (('all', '*'), ('all', g), (meta[i]['stratum'], '*'), (flag, '*')):
                n[key] += 1
            if f not in dis:
                continue
            if f not in ruled:
                pending.append(f'{i}:{f}'); continue
            ok = set(ruled[f].get('correct', []))
            wrong, wrong_a = inv['prod'] not in ok, inv['audit'] not in ok
            cats[ruled[f].get('category', '')] += 1
            for key in (('all', '*'), ('all', g), (meta[i]['stratum'], '*'), (flag, '*')):
                err[key] += wrong; err_audit[key] += wrong_a
            by_flag[f][flag] += wrong
    rate = err[('all', '*')] / n[('all', '*')] if n[('all', '*')] else float('nan')
    with open(f'audit/{vol}/scores.tsv', 'w') as o:
        o.write('segment\tgroup\tprod_err\taudit_err\twrong\tn\n')
        for key in sorted(n):
            o.write(f"{key[0]}\t{key[1]}\t{err[key] / n[key]:.4f}\t{err_audit[key] / n[key]:.4f}\t{err[key]}\t{n[key]}\n")
        o.write('\nadjudicator category\tcount\n')
        o.writelines(f'{k}\t{v}\n' for k, v in sorted(cats.items()))
        o.write(f"\ngroupwise looks\tfields {looks['fields']}\twrong {looks['wrong']}\n")
        uf = collections.Counter(f for r in recs.values() for f in r['uncertain_fields'])
        o.write(f"\nuncertain_fields (all {len(recs)} records; {sum(bool(r['uncertain_fields']) for r in recs.values())} "
                f"with any)\tcount\n")
        o.writelines(f'{k}\t{v}\n' for k, v in uf.most_common())
    open(f'audit/{vol}/brief_issues.md', 'w').write('\n'.join(issues) + '\n')
    print(f"vol {vol}: field error {err[('all', '*')]} of {n[('all', '*')]} = {rate:.2%} (gate {GATE:.1%}): "
          f"{'PASS' if rate <= GATE and not pending else 'FAIL' if not pending else 'PENDING'}")
    for key in sorted(k for k in n if k != ('all', '*')):
        print(f'  {key[0]}/{key[1]}: {err[key]} of {n[key]}')
    print(f"  groupwise looks: {looks['wrong']} wrong of {looks['fields']} fields")
    if pending:
        print(f'  not yet adjudicated: {len(pending)} ({" ".join(pending[:12])}{" …" if len(pending) > 12 else ""})')


if __name__ == '__main__':
    cmds = {'groupwise': cmd_groupwise, 'draw': cmd_draw, 'compare': cmd_compare, 'score': cmd_score}
    if len(sys.argv) != 3 or sys.argv[1] not in cmds:
        sys.exit(__doc__)
    cmds[sys.argv[1]](sys.argv[2])
