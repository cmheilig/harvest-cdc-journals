"""Find candidate teaching reports in the production index (vols 73-71) by how they make claims about data.

    python3 tools/query.py [FILTERS] [--show FACETS] [--tsv | --ids] [--out FILE]
    python3 tools/query.py --example NAME [FILTERS]   # a named query, optionally narrowed
    python3 tools/query.py examples                   # list the named queries
    python3 tools/query.py facets [FACET]             # the values each filter takes, with article counts
    python3 tools/query.py html [PATH]                # one static page with the same filters (default query.html)
    python3 tools/query.py meta [--packets DIR]       # rebuild query_meta.tsv (title, date, URL, type) from prod_packets/

Filters combine with AND. Commas inside one value mean OR (`--design case-control,test-negative`); repeating a flag
means AND (`--proc test --proc regression`). Values match the index's own vocabulary as case-insensitive substrings
(`--gw omnibus` finds "omnibus test"); a leading `=` asks for the exact value (`--inference =CIs`). Flags that take a
pattern (`--across`, `--condition`, `--text`, the NAME part of `--proc CLASS:NAME`) take a regular expression.

Every match is printed with the values that matched each filter and the evidence quotes behind them. A value the
reader listed in `uncertain_fields` is marked "reader unsure"; `--confident` drops such values from matching.
Field meanings are in READER_BRIEF.md §4; the fields the index answers less well are in QUERY_LIMITS.md.
"""
import argparse, collections, csv, glob, html, json, os, re, shlex, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from group_conditions import MAP as COND_MAP  # noqa: E402

VOLS = ('73', '72', '71')
META = 'query_meta.tsv'
META_COLS = ['id', 'vol', 'date', 'article_type', 'title', 'url']

# facet -> the uncertain_fields value that covers it (brief §4, Metadata)
UNC = {'focus': 'data_focus', 'outcome': 'outcome_types', 'depth': 'analytic_depth', 'proc': 'procedures',
       'family': 'procedures', 'framework': 'comparison', 'scale': 'comparison', 'referent': 'comparison',
       'across': 'comparison', 'inference': 'inference', 'gw': 'groupwise', 'design': 'study_design',
       'equity': 'equity.level', 'dim': 'equity.dimensions', 'rf': 'regression_fields', 'nonindep': 'regression_fields'}

LABEL = {'focus': 'data focus', 'outcome': 'outcome type', 'depth': 'analytic depth', 'proc': 'procedure',
         'family': 'regression family', 'framework': 'comparison framework', 'scale': 'comparison scale',
         'referent': 'referent', 'across': 'compared across', 'inference': 'inference', 'gw': 'groupwise',
         'design': 'study design', 'source': 'data source type', 'source_named': 'data source', 'setting': 'setting',
         'vaccine': 'vaccine outcome', 'equity': 'equity level', 'dim': 'equity dimension',
         'determinants': 'determinants of health', 'action': 'equity action', 'condition': 'condition',
         'rf': 'regression field', 'nonindep': 'nonindependence', 'text': 'title or question', 'vol': 'volume',
         'year': 'year', 'type': 'article type'}

RF_KEYS = {'unadjusted_results': 'unadjusted', 'adjusted_results': 'adjusted', 'interaction_modeled': 'interaction',
           'sensitivity_analysis': 'sensitivity'}


# ---------------------------------------------------------------- flattening

def ev(e):
    return [[x['loc'], x['q']] for x in (e or [])]


def vo(x):
    """value of a {"v", "ev"} object, or None"""
    return (x or {}).get('v')


def conditions(r):
    safety = vo(r.get('vaccine_outcome_type')) == 'safety'
    out = []
    for c in r.get('conditions', []):
        term = ' '.join(c['v'].lower().split())
        m = COND_MAP.get(term)
        groups = [g for g in m['groups'].split('|') if g] if m else []
        if safety and c['role'] == 'outcome' and 'vaccine safety' not in groups:  # V18(a), as tools/group_conditions.py
            groups.append('vaccine safety')
        canon = m['canonical'] if m else term
        out.append({'v': canon, 'term': c['v'], 'role': c['role'], 'groups': groups, 'ev': ev(c['ev']),
                    't': f"{c['v']} ({c['role']}) [{', '.join(groups) or 'ungrouped'}]"})
    return out


def flatten(r, m):
    """One article as {meta..., 'f': {facet: [item]}}. Each item has v (the matched value), t (display text), ev
    (evidence [[loc, quote]]) and u (the reader flagged the field uncertain), plus attributes some filters use."""
    unc = set(r.get('uncertain_fields') or [])
    comp, inf, eq = r['comparison'], r['inference'], r['equity']
    rf = r.get('regression_fields') or {}
    f = {
        'vol': [{'v': m['vol']}], 'year': [{'v': int(m['date'][:4]) if m['date'] else 1951 + int(m['vol'])}],
        'type': [{'v': m['article_type']}],
        'text': [{'v': m['title']}] + ([{'v': vo(r.get('public_health_question')) or '',
                                         'ev': ev(r['public_health_question']['ev'])}]
                                       if r.get('public_health_question') else []),
        'focus': [{'v': vo(r.get('data_focus')), 'ev': ev((r.get('data_focus') or {}).get('ev'))}],
        'outcome': [{'v': x['v'], 'ev': ev(x['ev'])} for x in r['outcome_types']],
        'depth': [{'v': x['v'], 'ev': ev(x['ev'])} for x in r['analytic_depth']],
        'proc': [{'v': p['class'], 'name': p['name'], 'role': p['role'], 'family': p.get('regression_family'),
                  't': f"{p['class']}: {p['name']} ({p['role']}"
                       + (f"; {p['regression_family']}" if p.get('regression_family') else '') + ')'
                       + (f" — inferred from: {p['inferred_from']}" if p.get('inferred_from') else ''),
                  'ev': ev(p['ev'])} for p in r['procedures']],
        'family': [{'v': p['regression_family'], 'role': p['role'],
                    't': f"{p['regression_family']}: {p['name']} ({p['role']})", 'ev': ev(p['ev'])}
                   for p in r['procedures'] if p['class'] == 'regression' and p.get('regression_family')],
        'framework': [{'v': comp['framework'], 'ev': ev(comp.get('ev')),
                       't': comp['framework'] + (f" across {'; '.join(comp['across'])}" if comp.get('across') else '')
                            + (f" vs {comp['referent']}" if comp.get('referent') else '')}],
        'scale': [{'v': comp['scale'], 'ev': ev(comp.get('ev'))}] if comp.get('scale') else [],
        'referent': [{'v': comp['referent'], 'ev': ev(comp.get('ev'))}] if comp.get('referent') else [],
        'across': [{'v': a, 'ev': ev(comp.get('ev'))} for a in comp.get('across') or []],
        'inference': [{'v': inf['v'], 'co': bool(inf.get('ci_overlap')), 'ms': bool(inf.get('method_stated')),
                       't': inf['v'] + ('; method stated' if inf.get('method_stated') else '')
                            + ('; rests on CI overlap' if inf.get('ci_overlap') else ''), 'ev': ev(inf.get('ev'))}],
        'gw': [{'v': g['assessment'], 'across': g['across'], 'n': g.get('n_groups'), 'ordered': bool(g.get('ordered')),
                'finer': bool(g.get('resolves_to_finer')),
                't': f"{g['assessment']} across {' × '.join(g['across'])} ("
                     + (f"{g['n_groups']} groups" if g.get('n_groups') else 'groups unclear')
                     + ('; ordered' if g.get('ordered') else '') + ('; resolves to finer' if g.get('resolves_to_finer') else '') + ')',
                'ev': ev(g['ev'])} for g in r.get('groupwise', [])],
        'design': [{'v': x['v'], 'ev': ev(x['ev'])} for x in r['study_design']],
        'source': [{'v': x['v'], 'ev': ev(x['ev'])} for x in r['data_source_type']],
        'source_named': [{'v': x['v'], 'ev': ev(x['ev'])} for x in r.get('data_source_named', [])],
        'setting': [{'v': vo(r.get('setting')), 'ev': ev((r.get('setting') or {}).get('ev'))}] if vo(r.get('setting')) else [],
        'vaccine': [{'v': vo(r['vaccine_outcome_type']), 'ev': ev(r['vaccine_outcome_type']['ev'])}]
                   if vo(r.get('vaccine_outcome_type')) else [],
        'equity': [{'v': eq['level'], 'conf': eq.get('level_confidence'), 'alt': (eq.get('level_alternative') or {}).get('level'),
                    't': f"level {eq['level']}" + ''.join(
                        f' ({x})' for x in ['; '.join(filter(None, [
                            f"confidence {eq['level_confidence']}" if eq.get('level_confidence') else '',
                            f"alternative {eq['level_alternative']['level']}: {eq['level_alternative']['reason']}"
                            if eq.get('level_alternative') else '']))] if x),
                    'why': eq.get('level_reason')}],
        'dim': [{'v': d['v'], 'role': d['role'], 'basis': d.get('basis'),
                 't': f"{d['v']}: {d['role']}" + (f" ({d['basis']})" if d.get('basis') else '')
                      + (f" [{', '.join(d['ses_components'])}]" if d.get('ses_components') else ''), 'ev': ev(d['ev'])}
                for d in eq['dimensions']],
        'determinants': [{'v': vo(eq['determinants_of_health']), 'ev': ev(eq['determinants_of_health']['ev'])}]
                        if vo(eq.get('determinants_of_health')) else [],
        'action': [{'v': 'true', 't': 'equity action', 'ev': ev(eq['equity_action']['ev'])}] if vo(eq.get('equity_action')) else [],
        'condition': conditions(r),
        'rf': [{'v': short, 'ev': ev(rf[k].get('ev')),
                't': short + (f" (confounders: {vo(rf['confounders'])})"
                              if short == 'adjusted' and vo(rf.get('confounders')) else '')}
               for k, short in RF_KEYS.items() if vo(rf.get(k)) is True],
        'nonindep': [{'v': vo(rf['nonindependence_approach']), 'ev': ev(rf['nonindependence_approach'].get('ev'))}]
                    if vo(rf.get('nonindependence_approach')) else [],
    }
    for k, items in f.items():
        for it in items:
            it.setdefault('t', str(it['v']))
            it.setdefault('ev', [])
            if UNC.get(k) in unc:
                it['u'] = True
    return {'id': r['id'], 'vol': m['vol'], 'date': m['date'], 'type': m['article_type'], 'title': m['title'],
            'url': m['url'], 'brief': r['reader'].get('brief_tag'), 'f': f}


def load():
    if not os.path.exists(META):
        sys.exit(f'{META} is missing: run python3 tools/query.py meta')
    meta = {m['id']: m for m in csv.DictReader(open(META), delimiter='\t')}
    arts = []
    for v in VOLS:
        for p in sorted(glob.glob(f'out/prod/{v}/mm*.json')):
            r = json.load(open(p))
            arts.append(flatten(r, meta.get(r['id']) or {'vol': v, 'date': '', 'article_type': '?', 'title': '?',
                                                          'url': f"https://www.cdc.gov/mmwr/volumes/{v}/wr/{r['id']}.htm"}))
    arts.sort(key=lambda a: (a['date'], a['id']), reverse=True)
    return arts


# ---------------------------------------------------------------- queries
# A query is a list of clauses {f: facet, op, vals, ...}; an article matches when every clause matches at least one of
# its items in that facet. The HTML page applies the same clauses (matchItem in PAGE mirrors match_item).

def vmatch(q, v):
    q, v = q.strip().lower(), str(v).lower()
    return v == q[1:] if q.startswith('=') else q in v


def match_item(c, it):
    op = c['op']
    if c.get('primary') and it.get('role') not in (None, 'primary') and c['f'] in ('proc', 'family'):
        return False
    if op == 'ctrl':
        return any(vmatch(x, it['v']) for x in c['vals'])
    if op == 'regex':
        return any(re.search(x, s or '', re.I) for x in c['vals'] for s in (str(it['v']), it.get('term')))
    if op == 'proc':
        for x in c['vals']:
            cls, _, rx = x.partition(':')
            if (not cls or vmatch(cls, it['v'])) and (not rx or re.search(rx, it['name'], re.I)):
                return True
        return False
    if op == 'flag':
        return bool(it.get(c['key'])) if c.get('key') else True
    if op == 'range':
        return c.get('lo', -1e9) <= it['v'] <= c.get('hi', 1e9)
    if op == 'gw':
        return ((not c.get('assess') or any(vmatch(x, it['v']) for x in c['assess']))
                and (not c.get('across') or any(vmatch(x, a) for x in c['across'] for a in it['across']))
                and (not c.get('ordered') or it['ordered'])
                and (c.get('finer') is None or it['finer'] == c['finer'])
                and (not c.get('min') or (it['n'] or 0) >= c['min']))
    if op == 'dim':
        for x in c['vals']:
            d, _, role = x.partition(':')
            if (not d or vmatch(d, it['v'])) and (not role or vmatch(role, it['role'])):
                return True
        return False
    if op == 'cgroup':
        return (any(vmatch(x, g) for x in c['vals'] for g in it['groups'])
                and (not c.get('role') or vmatch(c['role'], it['role'])))
    raise ValueError(op)


def run(arts, clauses, confident=False):
    out = []
    for a in arts:
        hits = []
        for c in clauses:
            h = [it for it in a['f'][c['f']] if match_item(c, it) and not (confident and it.get('u'))]
            if not h:
                break
            hits.append(h)
        else:
            out.append((a, hits))
    return out


def warn_unknown(arts, cl):
    """Say on stderr when a filter value matches no record at all: usually a typo or a value outside the vocabulary."""
    def items(f):
        return [it for a in arts for it in a['f'][f]]
    for c in cl:
        bad = []
        if c['op'] in ('ctrl', 'proc', 'dim', 'cgroup'):
            bad = [v for v in c['vals'] if not any(match_item({**c, 'vals': [v], 'primary': False}, it) for it in items(c['f']))]
        elif c['op'] == 'gw':
            bad = [x for x in c['assess'] if not any(vmatch(x, it['v']) for it in items('gw'))]
            bad += [x for x in c['across'] if not any(vmatch(x, a) for it in items('gw') for a in it['across'])]
        if bad:
            known = sorted({str(it['v']) for it in items(c['f'])})
            print(f"no record has a {LABEL[c['f']]} matching {', '.join(map(repr, bad))}; values: {'; '.join(known[:30])}"
                  + (' ... (python3 tools/query.py facets)' if len(known) > 30 else ''), file=sys.stderr)


def split(vs):
    return [x for v in vs for x in v.split(',') if x.strip()] if vs else []


def parser():
    p = argparse.ArgumentParser(prog='tools/query.py', add_help=True,
                                description='Query the MMWR index (vols 73-71). See the module docstring.')
    a = p.add_argument_group('article')
    a.add_argument('--vol', action='append', help='73, 72, 71')
    a.add_argument('--year', action='append', help='2023 or 2022-2023')
    a.add_argument('--type', action='append', help='full_report, quickstats, notes_from_the_field, vital_signs, outbreak_report')
    a.add_argument('--text', action='append', metavar='REGEX', help='in the title or the public health question')
    m = p.add_argument_group('methods')
    m.add_argument('--focus', action='append', help='data_focus: yes, limited, none')
    m.add_argument('--outcome', action='append', help='continuous, binary/categorical, count/rate, event-time')
    m.add_argument('--depth', action='append', help='summary, comparison, stratified/adjusted')
    m.add_argument('--proc', action='append', metavar='CLASS[:NAME]',
                   help='test, regression, standardization, weighting, transformation, other; NAME is a regex on the '
                        'procedure name, e.g. test:trend, test:chi, :bootstrap')
    m.add_argument('--primary', action='store_true', help='--proc and --family match primary procedures only')
    m.add_argument('--family', action='append', help='regression family: linear, logistic, loglinear, event-time, other')
    m.add_argument('--framework', action='append', help='none, fixed referent, pair, group')
    m.add_argument('--scale', action='append', help='absolute, relative, both')
    m.add_argument('--referent', action='append', metavar='REGEX', help="a referent is named ('.' for any)")
    m.add_argument('--across', action='append', metavar='REGEX', help='comparison.across, free text, e.g. race|ethnic')
    m.add_argument('--inference', action='append', help='none, CIs, tests, CIs and tests')
    m.add_argument('--ci-overlap', action='store_true', help='a claim rests on CI overlap')
    m.add_argument('--method-stated', action='store_true', help='the article says how CIs or tests were computed')
    m.add_argument('--rf', action='append', help='regression fields reported true: adjusted, unadjusted, interaction, sensitivity')
    m.add_argument('--nonindep', action='append', help='GEE, mixed effects, robust, conditional, complex survey design, ...')
    g = p.add_argument_group('groupwise (all --gw flags describe one and the same groupwise entry)')
    g.add_argument('--gw', action='append', metavar='ASSESSMENT',
                   help="side by side, omnibus test, trend test, model joint test, interaction, pairwise, per-group; 'any' for any entry")
    g.add_argument('--gw-across', action='append', metavar='DIM', help='race/ethnicity, SES, age, sex, rurality, ...')
    g.add_argument('--gw-ordered', action='store_true', help='ordered SPD levels')
    g.add_argument('--gw-finer', choices=['yes', 'no'], help='resolves to finer (pairwise or per-group) contrasts')
    g.add_argument('--gw-min', type=int, metavar='N', help='at least N groups')
    c = p.add_argument_group('context')
    c.add_argument('--design', action='append', help='case-control, test-negative, retrospective cohort, cross-sectional, ...')
    c.add_argument('--source', action='append', help='national survey, CDC surveillance system, registry, ...')
    c.add_argument('--source-named', action='append', metavar='REGEX', help='NHIS|BRFSS, ...')
    c.add_argument('--setting', action='append', help='US national, US subnational, international, mixed')
    c.add_argument('--vaccine', action='append', help='effectiveness, coverage/uptake, safety, other')
    c.add_argument('--cgroup', action='append', help='condition group (CONDITION_RUBRIC.md), e.g. mental health, COVID')
    c.add_argument('--crole', help='with --cgroup: the condition role, e.g. outcome')
    c.add_argument('--condition', action='append', metavar='REGEX', help='condition as the reader named it')
    e = p.add_argument_group('equity')
    e.add_argument('--equity-min', type=int, metavar='N')
    e.add_argument('--equity-max', type=int, metavar='N')
    e.add_argument('--dim', action='append', metavar='DIM[:ROLE]', help='e.g. race, SES:primary comparison, age:stratifier')
    e.add_argument('--determinants', action='store_true', help='determinants of health recorded')
    e.add_argument('--equity-action', action='store_true')
    o = p.add_argument_group('output')
    o.add_argument('--example', metavar='NAME', help='start from a named query (see: tools/query.py examples)')
    o.add_argument('--confident', action='store_true', help='ignore values the reader flagged uncertain')
    o.add_argument('--show', metavar='FACETS', help='also print these facets for each match, e.g. equity,design,proc')
    o.add_argument('--tsv', action='store_true', help='one row per article')
    o.add_argument('--ids', action='store_true', help='ids only')
    o.add_argument('--out', metavar='FILE')
    return p


def clauses_from(ns):
    cl = []
    for name in ('focus', 'outcome', 'depth', 'family', 'framework', 'scale', 'inference', 'design', 'source',
                 'setting', 'vaccine', 'rf', 'nonindep', 'type', 'vol'):
        for v in getattr(ns, name) or []:
            cl.append({'f': name, 'op': 'ctrl', 'vals': split([v])})
    for name in ('referent', 'across', 'source_named', 'condition', 'text'):
        for v in getattr(ns, name) or []:
            cl.append({'f': name, 'op': 'regex', 'vals': [v]})
    for v in ns.proc or []:
        cl.append({'f': 'proc', 'op': 'proc', 'vals': split([v])})
    if ns.primary:
        for x in cl:
            if x['f'] in ('proc', 'family'):
                x['primary'] = True
    for v in ns.year or []:
        lo, _, hi = v.partition('-')
        cl.append({'f': 'year', 'op': 'range', 'lo': int(lo), 'hi': int(hi or lo)})
    if ns.ci_overlap:
        cl.append({'f': 'inference', 'op': 'flag', 'key': 'co'})
    if ns.method_stated:
        cl.append({'f': 'inference', 'op': 'flag', 'key': 'ms'})
    if ns.gw or ns.gw_across or ns.gw_ordered or ns.gw_finer or ns.gw_min:
        assess = [x for x in split(ns.gw) if x.lower() != 'any']
        cl.append({'f': 'gw', 'op': 'gw', 'assess': assess, 'across': split(ns.gw_across), 'ordered': ns.gw_ordered,
                   'finer': None if ns.gw_finer is None else ns.gw_finer == 'yes', 'min': ns.gw_min})
    if ns.equity_min is not None or ns.equity_max is not None:
        cl.append({'f': 'equity', 'op': 'range', 'lo': ns.equity_min if ns.equity_min is not None else 0,
                   'hi': ns.equity_max if ns.equity_max is not None else 4})
    for v in ns.dim or []:
        cl.append({'f': 'dim', 'op': 'dim', 'vals': split([v])})
    if ns.determinants:
        cl.append({'f': 'determinants', 'op': 'flag'})
    if ns.equity_action:
        cl.append({'f': 'action', 'op': 'flag'})
    for v in ns.cgroup or []:
        cl.append({'f': 'condition', 'op': 'cgroup', 'vals': split([v]), 'role': ns.crole})
    return cl


# Named queries. Owner's priority (2026-10-17): method combinations; the others are kept as quick checks.
EXAMPLES = [
    ('weighting-omnibus-race', 'Full reports with survey weighting and an omnibus test across race/ethnicity groups',
     '--type full_report --proc weighting --gw omnibus --gw-across race'),
    ('weighting-ordered-spd', 'Survey weighting with an omnibus or trend test across ordered groups (age, income, education)',
     '--proc weighting --gw omnibus,trend --gw-ordered'),
    ('survey-chi-square', 'Survey-adjusted chi-square or Wald tests (Rao-Scott, Wald F) on weighted estimates',
     "--proc weighting --proc 'test:rao|wald|survey|design'"),
    ('case-control-regression', 'Case-control designs analyzed with regression',
     '--design case-control --proc regression'),
    ('test-negative-ve', 'Test-negative vaccine effectiveness with logistic regression',
     '--design test-negative --family logistic --vaccine effectiveness'),
    ('cohort-event-time', 'Cohort designs with event-time (Cox-type) regression',
     '--design cohort --family event-time'),
    ('crude-and-adjusted', 'Regression reporting both unadjusted and adjusted results for the same contrast',
     '--proc regression --rf unadjusted --rf adjusted'),
    ('regression-interaction', 'Regression with an interaction term or test (effect differs by group or period)',
     '--proc regression --rf interaction'),
    ('survey-regression', 'National-survey regression that accounts for the complex survey design',
     '--source "national survey" --proc regression --nonindep "complex survey"'),
    ('standardized-rates-race', 'Standardized rates compared across race/ethnicity',
     '--proc standardization --dim "race:primary comparison,race:stratifier"'),
    ('joinpoint-trends', 'Trend analyses with joinpoint regression',
     '--proc regression:joinpoint'),
    ('ci-overlap', 'Claims that rest on CI overlap (not a formal test)',
     '--ci-overlap'),
    ('ci-inversion', 'Conclusions drawn from a CI against a null value (test by CI inversion)',
     '--proc test:inversion'),
    ('qs-trend', 'QuickStats with a trend over time judged by a test (in vols 73–71 the test is always unnamed)',
     '--type quickstats --design trend --proc test'),
    ('nftf-fixed-referent', 'Notes from the Field comparing with a fixed referent (target, threshold, population value)',
     '--type notes_from_the_field --framework "fixed referent"'),
    ('equity3-mental-health', 'Equity level 3 or higher on mental health conditions',
     '--equity-min 3 --cgroup "mental health"'),
]


def example(name):
    for n, d, a in EXAMPLES:
        if n == name:
            return d, shlex.split(a)
    sys.exit(f'no example {name!r}; see: python3 tools/query.py examples')


# ---------------------------------------------------------------- output

def quote(e):
    return f'"{e[1]}" ({e[0]})'


def unsure(it):
    return ' *(reader unsure)*' if it.get('u') else ''


def markdown(res, cl, desc, cmd, show, total):
    out = [f'# {desc}' if desc else '# Query', '', f'`python3 tools/query.py {cmd}`', '']
    by_vol = collections.Counter(a['vol'] for a, _ in res)
    by_type = collections.Counter(a['type'] for a, _ in res)
    out.append(f"**{len(res)} of {total} articles.** By volume: "
               + ', '.join(f'{v}: {by_vol[v]}' for v in VOLS if by_vol[v])
               + '. By type: ' + ', '.join(f'{t}: {n}' for t, n in by_type.most_common()) + '.')
    for k, (a, hits) in enumerate(res, 1):
        out += ['', f"## {k}. {a['title']}", f"{a['id']} · {a['date']} · {a['type']} · {a['url']}"]
        shown = set()
        for c, h in zip(cl, hits):
            if c['f'] in ('vol', 'year', 'type'):
                continue
            for it in h:
                key = (c['f'], it['t'])
                if key in shown:
                    continue
                shown.add(key)
                out.append(f"- **{LABEL[c['f']]}:** {it['t']}{unsure(it)}")
                if it.get('why'):
                    out.append(f"  - reader's reason: {it['why']}")
                out += [f'  - {quote(e)}' for e in it['ev']]
        for f in show:
            for it in a['f'].get(f, []):
                if (f, it['t']) not in shown:
                    out.append(f"- {LABEL.get(f, f)}: {it['t']}{unsure(it)}")
    return '\n'.join(out) + '\n'


def tsv(res, cl):
    cols = ['id', 'vol', 'date', 'type', 'title', 'url'] + [f"{c['f']}{k + 1}" for k, c in enumerate(cl)]
    rows = ['\t'.join(cols)]
    for a, hits in res:
        cells = [a['id'], a['vol'], a['date'], a['type'], a['title'], a['url']]
        for h in hits:
            cells.append(' || '.join(it['t'] + (' [unsure]' if it.get('u') else '') + ''.join(f' | {quote(e)}' for e in it['ev'])
                                     for it in h))
        rows.append('\t'.join(x.replace('\t', ' ').replace('\n', ' ') for x in cells))
    return '\n'.join(rows) + '\n'


def facets(arts, which=None):
    spec = [('type', 'v'), ('year', 'v'), ('focus', 'v'), ('outcome', 'v'), ('depth', 'v'), ('proc', 'v'),
            ('proc-name', None), ('family', 'v'), ('framework', 'v'), ('scale', 'v'), ('inference', 'v'), ('gw', 'v'),
            ('gw-across', None), ('design', 'v'), ('source', 'v'), ('setting', 'v'), ('vaccine', 'v'),
            ('equity', 'v'), ('dim', 'v'), ('dim-role', None), ('cgroup', None), ('rf', 'v'), ('nonindep', 'v')]
    for name, _ in spec:
        if which and name != which:
            continue
        cnt = collections.Counter()
        for a in arts:
            if name == 'proc-name':
                vals = {it['name'].lower() for it in a['f']['proc']}
            elif name == 'gw-across':
                vals = {x for it in a['f']['gw'] for x in it['across']}
            elif name == 'dim-role':
                vals = {f"{it['v']}: {it['role']}" for it in a['f']['dim']}
            elif name == 'cgroup':
                vals = {g for it in a['f']['condition'] for g in it['groups']}
            else:
                vals = {str(it['v']) for it in a['f'][name]}
            cnt.update(vals)
        top = cnt.most_common(None if which or name != 'proc-name' else 25)
        print(f"\n{name} ({len(cnt)} values{', top 25' if len(top) < len(cnt) else ''}; articles)")
        for v, n in top:
            print(f'  {n:4d}  {v}')


# ---------------------------------------------------------------- html

def page(arts):
    exs = []
    for n, d, a in EXAMPLES:
        exs.append({'name': n, 'desc': d, 'cmd': a, 'clauses': clauses_from(parser().parse_args(shlex.split(a)))})
    vocab = {}
    for f in ('type', 'year', 'focus', 'outcome', 'depth', 'proc', 'family', 'framework', 'scale', 'inference',
              'design', 'source', 'setting', 'vaccine', 'rf', 'nonindep', 'dim'):
        cnt = collections.Counter(str(it['v']) for a in arts for it in {str(i['v']): i for i in a['f'][f]}.values())
        vocab[f] = sorted(cnt.items(), key=lambda x: (-x[1], x[0]))
    vocab['gw'] = sorted(collections.Counter(v for a in arts for v in {i['v'] for i in a['f']['gw']}).items(), key=lambda x: -x[1])
    vocab['gw-across'] = sorted(collections.Counter(v for a in arts for v in {x for i in a['f']['gw'] for x in i['across']}).items(), key=lambda x: -x[1])
    vocab['cgroup'] = sorted(collections.Counter(v for a in arts for v in {g for i in a['f']['condition'] for g in i['groups']}).items(), key=lambda x: -x[1])
    vocab['dim-role'] = sorted(collections.Counter(v for a in arts for v in {i['role'] for i in a['f']['dim']}).items(), key=lambda x: -x[1])
    data = json.dumps({'articles': arts, 'examples': exs, 'vocab': vocab, 'labels': LABEL}, ensure_ascii=False,
                      separators=(',', ':')).replace('</', '<\\/')
    return PAGE.replace('__DATA__', data)


PAGE = r'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>MMWR Method Finder</title>
<style>
:root{--bg:#fafaf8;--panel:#ffffff;--ink:#1d1d1b;--muted:#62625d;--line:#e2e1dc;--accent:#2b5d8a;--chip:#eef3f8;--warn:#8a5a00;--warnbg:#fff4dc;--q:#f4f3ef}
@media (prefers-color-scheme: dark){:root:not([data-theme="light"]){--bg:#161615;--panel:#1f1f1d;--ink:#ecebe6;--muted:#a3a29b;--line:#34332f;--accent:#8db8e0;--chip:#24303c;--warn:#f0c46a;--warnbg:#3a2f17;--q:#262624}}
:root[data-theme="dark"]{--bg:#161615;--panel:#1f1f1d;--ink:#ecebe6;--muted:#a3a29b;--line:#34332f;--accent:#8db8e0;--chip:#24303c;--warn:#f0c46a;--warnbg:#3a2f17;--q:#262624}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}
header{padding:16px;border-bottom:1px solid var(--line);background:var(--panel)}
header h1{margin:0 0 4px;font-size:20px}
header p{margin:0;color:var(--muted);font-size:13px}
.wrap{display:grid;grid-template-columns:320px 1fr;gap:0;min-height:calc(100vh - 70px)}
aside{border-right:1px solid var(--line);padding:12px 16px;background:var(--panel);overflow:auto;max-height:calc(100vh - 70px);position:sticky;top:0}
main{padding:12px 16px;min-width:0}
details{border-bottom:1px solid var(--line);padding:6px 0}
summary{cursor:pointer;font-weight:600;font-size:14px}
summary .n{color:var(--accent);font-weight:400}
label{display:flex;gap:6px;align-items:baseline;font-size:13px;margin:3px 0}
label .c{color:var(--muted);margin-left:auto;font-variant-numeric:tabular-nums}
select,input[type=text]{width:100%;padding:4px 6px;margin:3px 0;background:var(--bg);color:var(--ink);border:1px solid var(--line);border-radius:4px;font:inherit;font-size:13px}
button{font:inherit;font-size:13px;padding:4px 10px;border:1px solid var(--line);border-radius:4px;background:var(--bg);color:var(--ink);cursor:pointer}
.bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:10px}
.count{font-weight:600}
.ex{background:var(--chip);border-radius:6px;padding:8px 10px;margin-bottom:10px;font-size:13px}
.ex code{font-size:12px;word-break:break-word}
.card{background:var(--panel);border:1px solid var(--line);border-radius:6px;padding:10px 12px;margin:0 0 10px}
.card h3{margin:0 0 2px;font-size:15px}
.card h3 a{color:var(--accent);text-decoration:none}
.meta{color:var(--muted);font-size:12px;margin-bottom:6px}
.hit{margin:6px 0 0;font-size:13px}
.hit b{font-weight:600}
.u{background:var(--warnbg);color:var(--warn);border-radius:3px;padding:0 4px;font-size:11px;margin-left:4px}
.q{background:var(--q);border-left:3px solid var(--line);padding:3px 8px;margin:3px 0 0 8px;font-size:12.5px}
.q i{color:var(--muted);font-style:normal}
.why{color:var(--muted);font-size:12.5px;margin:2px 0 0 8px}
.ctx{color:var(--muted);font-size:12px;margin-top:6px}
@media (max-width:760px){.wrap{grid-template-columns:1fr}aside{position:static;max-height:none;border-right:0;border-bottom:1px solid var(--line)}}
</style></head><body>
<header><h1>MMWR method finder: vols 73–71 (2022–2024)</h1>
<p>Filters combine with AND across groups and OR within a group (regression fields: AND). Each match lists the values that matched and the article's own quotes behind them. “unsure” marks a field the reader flagged uncertain. Limits: QUERY_LIMITS.md.</p></header>
<div class="wrap"><aside id="side"></aside><main>
<div class="bar"><select id="ex" style="max-width:420px"><option value="">Named query…</option></select>
<label style="margin:0"><input type="checkbox" id="conf"> ignore unsure values</label>
<button id="reset">Clear</button><span class="count" id="count"></span></div>
<div id="exbox"></div><div id="out"></div></main></div>
<script id="data" type="application/json">__DATA__</script>
<script>
const D=JSON.parse(document.getElementById('data').textContent);const L=D.labels;
const esc=s=>String(s??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
function vm(q,v){q=q.trim().toLowerCase();v=String(v).toLowerCase();return q.startsWith('=')?v===q.slice(1):v.includes(q)}
function rx(p,s){try{return new RegExp(p,'i').test(s||'')}catch(e){return false}}
function matchItem(c,it){
 if(c.primary&&(c.f==='proc'||c.f==='family')&&it.role!=null&&it.role!=='primary')return false;
 switch(c.op){
 case'ctrl':return c.vals.some(x=>vm(x,it.v));
 case'regex':return c.vals.some(x=>rx(x,String(it.v))||rx(x,it.term));
 case'proc':return c.vals.some(x=>{const i=x.indexOf(':');const cls=i<0?x:x.slice(0,i),r=i<0?'':x.slice(i+1);return(!cls||vm(cls,it.v))&&(!r||rx(r,it.name))});
 case'flag':return c.key?!!it[c.key]:true;
 case'range':return (c.lo??-1e9)<=it.v&&it.v<=(c.hi??1e9);
 case'gw':return(!c.assess||!c.assess.length||c.assess.some(x=>vm(x,it.v)))&&(!c.across||!c.across.length||c.across.some(x=>it.across.some(a=>vm(x,a))))&&(!c.ordered||it.ordered)&&(c.finer==null||it.finer===c.finer)&&(!c.min||(it.n||0)>=c.min);
 case'dim':return c.vals.some(x=>{const i=x.indexOf(':');const d=i<0?x:x.slice(0,i),r=i<0?'':x.slice(i+1);return(!d||vm(d,it.v))&&(!r||vm(r,it.role))});
 case'cgroup':return c.vals.some(x=>it.groups.some(g=>vm(x,g)))&&(!c.role||vm(c.role,it.role));
 }return false}
const side=document.getElementById('side');
const CB=[['type','Article type'],['year','Year'],['proc','Procedure class'],['family','Regression family'],['design','Study design'],['depth','Analytic depth'],['outcome','Outcome type'],['framework','Comparison framework'],['scale','Comparison scale'],['inference','Inference'],['source','Data source type'],['setting','Setting'],['vaccine','Vaccine outcome'],['cgroup','Condition group'],['nonindep','Nonindependence (regression)']];
function group(id,title,inner,open){return `<details ${open?'open':''} data-g="${id}"><summary>${title} <span class="n" id="n-${id}"></span></summary>${inner}</details>`}
function boxes(f,vals){return vals.map(([v,n])=>`<label><input type="checkbox" data-f="${f}" value="${esc(v)}"> ${esc(v)}<span class="c">${n}</span></label>`).join('')}
let h='';
h+=group('text','Search title or question','<input type="text" id="text" placeholder="regex, e.g. suicide|overdose">',true);
for(const [f,t] of CB.slice(0,2))h+=group(f,t,boxes(f,D.vocab[f]),f==='type');
h+=group('proc','Procedure',boxes('proc',D.vocab.proc)+'<input type="text" id="pname" placeholder="name regex, e.g. chi|trend|Rao"><label><input type="checkbox" id="primary"> primary procedures only</label>',true);
h+=group('gw','Groupwise (one entry)','<select id="gwa"><option value="">any assessment</option>'+D.vocab.gw.map(([v,n])=>`<option value="=${esc(v)}">${esc(v)} (${n})</option>`).join('')+'</select><select id="gwx"><option value="">any dimension</option>'+D.vocab['gw-across'].map(([v,n])=>`<option value="=${esc(v)}">${esc(v)} (${n})</option>`).join('')+'</select><label><input type="checkbox" id="gwo"> ordered levels</label><label><input type="checkbox" id="gwany"> require a groupwise entry</label><select id="gwf"><option value="">resolves to finer: either</option><option value="1">resolves to finer: yes</option><option value="0">resolves to finer: no</option></select>',true);
for(const [f,t] of CB.slice(3))h+=group(f,t,boxes(f,D.vocab[f]),false);
h+=group('rf','Regression fields (all must hold)',boxes('rf',D.vocab.rf),false);
h+=group('infx','Inference details','<label><input type="checkbox" id="co"> a claim rests on CI overlap</label><label><input type="checkbox" id="ms"> CI/test method stated</label>',false);
h+=group('equity','Equity','<label>level from <select id="emin">'+[0,1,2,3,4].map(i=>`<option>${i}</option>`).join('')+'</select> to <select id="emax">'+[4,3,2,1,0].map(i=>`<option>${i}</option>`).join('')+'</select></label><select id="dimv"><option value="">any dimension</option>'+D.vocab.dim.map(([v,n])=>`<option value="=${esc(v)}">${esc(v)} (${n})</option>`).join('')+'</select><select id="dimr"><option value="">any role</option>'+D.vocab['dim-role'].map(([v,n])=>`<option value="=${esc(v)}">${esc(v)}</option>`).join('')+'</select><label><input type="checkbox" id="det"> determinants recorded</label><label><input type="checkbox" id="act"> equity action</label>',false);
side.innerHTML=h;
const exs=document.getElementById('ex');D.examples.forEach((e,i)=>exs.insertAdjacentHTML('beforeend',`<option value="${i}">${esc(e.desc)}</option>`));
const $=id=>document.getElementById(id);
function uiClauses(){const cl=[];
 const byF={};side.querySelectorAll('input[type=checkbox][data-f]:checked').forEach(b=>(byF[b.dataset.f]=byF[b.dataset.f]||[]).push(b.value));
 const prim=$('primary').checked,pn=$('pname').value.trim();
 for(const f in byF){const v=byF[f];
  if(f==='proc')cl.push({f:'proc',op:'proc',vals:v.map(x=>'='+x+(pn?':'+pn:'')),primary:prim});
  else if(f==='year')cl.push({f:'year',op:'range',lo:Math.min(...v.map(Number)),hi:Math.max(...v.map(Number)),set:v.map(Number)});
  else if(f==='cgroup')cl.push({f:'condition',op:'cgroup',vals:v.map(x=>'='+x)});
  else if(f==='rf')v.forEach(x=>cl.push({f:'rf',op:'ctrl',vals:['='+x]}));
  else cl.push({f,op:'ctrl',vals:v.map(x=>'='+x),primary:prim});}
 if(pn&&!byF.proc)cl.push({f:'proc',op:'proc',vals:[':'+pn],primary:prim});
 const ga=$('gwa').value,gx=$('gwx').value,go=$('gwo').checked,gf=$('gwf').value,gany=$('gwany').checked;
 if(ga||gx||go||gf||gany)cl.push({f:'gw',op:'gw',assess:ga?[ga]:[],across:gx?[gx]:[],ordered:go,finer:gf===''?null:gf==='1'});
 if($('co').checked)cl.push({f:'inference',op:'flag',key:'co'});
 if($('ms').checked)cl.push({f:'inference',op:'flag',key:'ms'});
 const lo=+$('emin').value,hi=+$('emax').value;if(lo>0||hi<4)cl.push({f:'equity',op:'range',lo,hi});
 const dv=$('dimv').value,dr=$('dimr').value;if(dv||dr)cl.push({f:'dim',op:'dim',vals:[(dv||'')+':'+(dr||'')]});
 if($('det').checked)cl.push({f:'determinants',op:'flag'});if($('act').checked)cl.push({f:'action',op:'flag'});
 const t=$('text').value.trim();if(t)cl.push({f:'text',op:'regex',vals:[t]});
 return cl}
function counts(){side.querySelectorAll('details').forEach(d=>{const g=d.dataset.g;const n=d.querySelectorAll('input:checked').length+[...d.querySelectorAll('select')].filter(s=>s.value&&!(s.id==='emin'&&s.value==='0')&&!(s.id==='emax'&&s.value==='4')).length+[...d.querySelectorAll('input[type=text]')].filter(i=>i.value.trim()).length;const el=document.getElementById('n-'+g);if(el)el.textContent=n?`(${n})`:''})}
function render(){counts();
 const ex=exs.value===''?null:D.examples[+exs.value];
 $('exbox').innerHTML=ex?`<div class="ex"><b>${esc(ex.desc)}</b><br><code>python3 tools/query.py --example ${esc(ex.name)}</code> &nbsp;=&nbsp; <code>${esc(ex.cmd)}</code></div>`:'';
 const cl=(ex?ex.clauses:[]).concat(uiClauses());const conf=$('conf').checked;const res=[];
 for(const a of D.articles){const hits=[];let ok=true;
  for(const c of cl){let hh=a.f[c.f].filter(it=>matchItem(c,it)&&!(conf&&it.u));if(c.set)hh=hh.filter(it=>c.set.includes(it.v));if(!hh.length){ok=false;break}hits.push([c,hh])}
  if(ok)res.push([a,hits])}
 $('count').textContent=`${res.length} of ${D.articles.length} articles`;
 const out=[];for(const [a,hits] of res.slice(0,300)){const seen=new Set();let hs='';
  for(const [c,hh] of hits){if(['vol','year','type'].includes(c.f))continue;for(const it of hh){const k=c.f+'|'+it.t;if(seen.has(k))continue;seen.add(k);
   hs+=`<div class="hit"><b>${esc(L[c.f]||c.f)}:</b> ${esc(it.t)}${it.u?'<span class="u">reader unsure</span>':''}</div>`;
   if(it.why)hs+=`<div class="why">reader's reason: ${esc(it.why)}</div>`;
   for(const [loc,q] of it.ev)hs+=`<div class="q">“${esc(q)}” <i>(${esc(loc)})</i></div>`}}
  const des=a.f.design.map(x=>x.v).join('; '),eq=a.f.equity[0];
  out.push(`<div class="card"><h3><a href="${esc(a.url)}" target="_blank" rel="noopener">${esc(a.title)}</a></h3><div class="meta">${esc(a.id)} · ${esc(a.date)} · ${esc(a.type.replace(/_/g,' '))}</div>${hs}<div class="ctx">design: ${esc(des)} · equity level ${eq.v} · procedures: ${esc([...new Set(a.f.proc.map(p=>p.v))].join(', ')||'none')}</div></div>`)}
 if(res.length>300)out.push(`<p>${res.length-300} more; narrow the filters.</p>`);
 $('out').innerHTML=out.join('')||'<p>No articles match.</p>'}
side.addEventListener('input',render);exs.addEventListener('change',render);$('conf').addEventListener('change',render);
$('reset').onclick=()=>{side.querySelectorAll('input[type=checkbox]').forEach(b=>b.checked=false);side.querySelectorAll('input[type=text]').forEach(b=>b.value='');side.querySelectorAll('select').forEach(s=>s.selectedIndex=0);exs.value='';render()};
render();
</script></body></html>
'''


# ---------------------------------------------------------------- meta

def build_meta(packets):
    rows = []
    for v in VOLS:
        frame = {r['id']: r for r in csv.DictReader(open(f'{packets}/{v}/frame.tsv'), delimiter='\t')}
        for p in sorted(glob.glob(f'out/prod/{v}/mm*.json')):
            i = os.path.basename(p)[:-5]
            head, title = {}, ''
            lines = open(f'{packets}/{v}/{i}/article.txt').read().split('\n')
            k = 0
            while k < len(lines) and lines[k].strip():
                key, _, val = lines[k].partition(': ')
                head[key] = val.strip(); k += 1
            for line in lines[k:]:
                if line.strip():
                    title = ' '.join(line.replace('\xad', '').split()); break
            rows.append({'id': i, 'vol': v, 'date': head.get('date', ''),
                         'article_type': head.get('article_type') or frame.get(i, {}).get('article_type', ''),
                         'title': title, 'url': head.get('url', '')})
    with open(META, 'w', newline='') as o:
        w = csv.DictWriter(o, META_COLS, delimiter='\t', lineterminator='\n')
        w.writeheader(); w.writerows(rows)
    print(len(rows), 'articles ->', META, file=sys.stderr)


# ---------------------------------------------------------------- main

def main(argv):
    if argv[:1] == ['meta']:
        k = argv.index('--packets') + 1 if '--packets' in argv else None
        return build_meta(argv[k] if k else 'prod_packets')
    arts = load()
    if argv[:1] == ['examples']:
        for n, d, a in EXAMPLES:
            k = len(run(arts, clauses_from(parser().parse_args(shlex.split(a)))))
            print(f'{n:24s} {k:4d}  {d}\n{"":30s}{a}')
        return
    if argv[:1] == ['facets']:
        return facets(arts, argv[1] if len(argv) > 1 else None)
    if argv[:1] == ['html']:
        path = argv[1] if len(argv) > 1 else 'query.html'
        open(path, 'w').write(page(arts))
        print(f'{len(arts)} articles -> {path} ({os.path.getsize(path) // 1024} KB)', file=sys.stderr)
        return
    p = parser()
    ns = p.parse_args(argv)
    args = list(argv)
    if ns.out:  # the echoed command lists filters only
        k = args.index('--out'); del args[k:k + 2]
    desc = None
    if ns.example:
        name = ns.example
        desc, base = example(name)
        k = args.index('--example'); del args[k:k + 2]
        args = base + args
        ns = p.parse_args(base + [x for i, x in enumerate(argv) if not (x == '--example' or (i and argv[i - 1] == '--example'))])
    cl = clauses_from(ns)
    if not cl:
        p.error('give at least one filter, or --example NAME (python3 tools/query.py examples)')
    warn_unknown(arts, cl)
    res = run(arts, cl, ns.confident)
    show = [s.strip() for s in ns.show.split(',')] if ns.show else []
    if ns.ids:
        text = ''.join(a['id'] + '\n' for a, _ in res)
    elif ns.tsv:
        text = tsv(res, cl)
    else:
        text = markdown(res, cl, desc, ' '.join(shlex.quote(x) for x in args), show, len(arts))
    if ns.out:
        open(ns.out, 'w').write(text)
        print(f'{len(res)} articles -> {ns.out}', file=sys.stderr)
    else:
        sys.stdout.write(text)


if __name__ == '__main__':
    main(sys.argv[1:])
