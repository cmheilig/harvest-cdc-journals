"""Surface rules for a first-pass MMWR index: mine cue phrases from production records, then score regex rules against them.

    python3 tools/surface_rules.py mine  [--split dev] [--out DIR]   # cue n-grams per label -> DIR/surface_cues.tsv
    python3 tools/surface_rules.py score [--split dev] [--out DIR]   # rules vs records -> DIR/surface_scores.tsv, DIR/surface_misses.tsv

Labels come from out/prod/<vol>/mm*.json (brief v5.1/v5.2), text from prod_packets/<vol>/<id>/article.txt.
Splits: dev = vols 72 and 73 (rules are tuned here); test = vol 71 (scored once, not tuned on); all = both.
Prints counts only; record and packet text stays in the output files.
"""
import argparse, collections, csv, glob, json, os, re

SPLITS = {'dev': ('72', '73'), 'test': ('71',), 'all': ('71', '72', '73')}
REG_FAMILIES = ('linear', 'logistic', 'loglinear', 'event-time', 'other')
DESIGNS = ('cross-sectional', 'case-control', 'test-negative', 'retrospective cohort', 'prospective cohort',
           'outbreak investigation', 'before/after or trend', 'evaluation or monitoring', 'case report/series',
           'guidance or recommendations', 'review or synthesis', 'ecologic', 'surveillance/descriptive')
COMPARE_ROLES = ('stratifier', 'primary comparison', 'cross-classifier')


def band(level):
    return '0-1' if level <= 1 else '2' if level == 2 else '3-4'


def labels(r):
    """Article-level labels, each paired with the evidence quotes that support it (for mining)."""
    L = collections.defaultdict(list)  # label -> [quote, ...]; presence of the key means the label is true
    procs = r['procedures']
    for p in procs:
        q = [e['q'] for e in p['ev']]
        L[f"proc:{p['class']}"] += q
        if p['class'] == 'regression':
            L['reg:any'] += q
            L[f"reg:{p['regression_family']}"] += q
    inf = r['inference']['v']
    if inf in ('CIs', 'CIs and tests'):
        L['inf:CIs'] += [e['q'] for e in r['inference']['ev']]
    if inf in ('tests', 'CIs and tests'):
        L['inf:tests'] += [e['q'] for e in r['inference']['ev']]
    for x in r['outcome_types']:
        L[f"out:{x['v']}"] += [e['q'] for e in x['ev']]
    for x in r['analytic_depth']:
        L[f"depth:{x['v']}"] += [e['q'] for e in x['ev']]
    for x in r['study_design']:
        L[f"design:{x['v']}"] += [e['q'] for e in x['ev']]
    for g in r.get('groupwise', []):
        L['groupwise:any'] += [e['q'] for e in g['ev']]
    eq = r['equity']
    L[f"eqband:{band(eq['level'])}"] += [e['q'] for d in eq['dimensions'] for e in d['ev']]
    if eq['level'] >= 2:
        L['eqband:2+'] += [e['q'] for d in eq['dimensions'] if d['role'] in COMPARE_ROLES for e in d['ev']]
    if eq['level'] >= 3:
        L['eqband:3+'] += [e['q'] for e in (eq['determinants_of_health'] or {}).get('ev', [])]
    for d in eq['dimensions']:
        if d['role'] in COMPARE_ROLES and d['v'] not in ('age', 'sex'):
            L['eqdim:compared non-age/sex'] += [e['q'] for e in d['ev']]
    return L


def load(split):
    """[(id, vol, article_type, text, labels)] for the split's articles that have both a record and a packet."""
    out = []
    for vol in SPLITS[split]:
        types = {r['id']: r['article_type'] for r in csv.DictReader(open(f'prod_packets/{vol}/frame.tsv'), delimiter='\t')}
        for f in sorted(glob.glob(f'out/prod/{vol}/mm*.json')):
            i = os.path.basename(f)[:-5]
            p = f'prod_packets/{vol}/{i}/article.txt'
            if os.path.exists(p):
                out.append((i, vol, types.get(i, ''), open(p).read(), labels(json.load(open(f)))))
    return out


# --- mining -------------------------------------------------------------------------------------------------------
TOKEN = re.compile(r"[a-z][a-z0-9'’\-]*|[0-9]+(?:\.[0-9]+)?%?|[≥≤<>]")
STOP = set('the of and a an in to for with by was were is are as on at from that this or be than among'.split())


def grams(text, n=3):
    t = TOKEN.findall(text.lower())
    g = set()
    for k in range(1, n + 1):
        for j in range(len(t) - k + 1):
            w = t[j:j + k]
            if w[0] in STOP or w[-1] in STOP or all(x[0].isdigit() for x in w):
                continue
            g.add(' '.join(w))
    return g


def mine(split, out):
    rows = load(split)
    docs = {i: grams(clean(text)) for i, _, _, text, _ in rows}
    n = len(rows)
    allL = sorted({k for *_, L in rows for k in L})
    res = []
    for lab in allL:
        pos = {i for i, *_, L in rows if lab in L}
        if len(pos) < 5:
            continue
        cand = collections.Counter()
        for i, *_, L in rows:
            if lab in L:
                for q in L[lab]:
                    cand.update(grams(q))
        for g, _ in cand.most_common(400):
            has = {i for i in docs if g in docs[i]}
            tp = len(has & pos)
            if tp < 3:
                continue
            prec, rec = tp / len(has), tp / len(pos)
            res.append((lab, g, len(pos), len(has), tp, round(prec, 3), round(rec, 3), round(2 * prec * rec / (prec + rec), 3)))
    res.sort(key=lambda x: (x[0], -x[7]))
    keep = []
    for lab in allL:
        keep += [x for x in res if x[0] == lab][:40]
    with open(f'{out}/surface_cues.tsv', 'w') as o:
        o.write('label\tcue\tn_pos\tn_docs_with\ttp\tprecision\trecall\tf1\n')
        for x in keep:
            o.write('\t'.join(map(str, x)) + '\n')
    print(n, 'articles;', len({x[0] for x in keep}), 'labels with cues;', len(keep), 'cue rows ->', f'{out}/surface_cues.tsv')


# --- text preparation and rules -------------------------------------------------------------------------------------
URL = re.compile(r'https?://\S+')


def clean(text):
    """Article text without the packet header and URLs. Notes stay: MMWR often states methods in footnotes."""
    body = text.split('\n', 5)[-1]
    return URL.sub(' ', body)


def sections(text):
    """(title, summary, body, tables): tables are the TABLE/FIGURE blocks' titles, columns and rows."""
    body = clean(text)
    lines = body.split('\n')
    title = lines[0] if lines else ''
    summary = next((l for l in lines if l.startswith('Summary:')), '')
    tab = '\n'.join(l for l in lines if re.match(r'(TABLE|FIGURE|Columns:|Rows:)', l))
    return title, summary, body, tab


def has(pats, s):
    return any(re.search(p, s, re.I) for p in pats)


def mask(s, traps):
    for t in traps:
        s = re.sub(t, ' ', s, flags=re.I)
    return s


RULES = {}  # label -> function(title, summary, body, tables, article_type) -> bool


def rule(label):
    def deco(f):
        RULES[label] = f
        return f
    return deco


# rules are tuned on the dev split only. Each label's traps are masked before its patterns are matched.
HAZARD_TRAP = r'\b(occupational|workplace|environmental|health|fire|safety|chemical|physical|electrical|fall|noise|potential|known|common) hazards?\b'
TRAPS_REG = [HAZARD_TRAP, r'\blogistical(ly)?\b', r'\blogistic (issues|challenges|support|barriers|constraints|considerations|reasons)',
             r'\bregression of (symptoms|lesions|disease)', r'(exact )?poisson (distribution|approximation|method|exact|confidence|interval)',
             r'exact poisson', r'assuming a poisson', r'log[- ]linear chi[- ]square', r'least[- ]squares means?', r'EPIC-HR']
REG_PATS = {
    'logistic': [r'\blogistic\b', r'adjusted odds ratios?', r'\baORs?\b', r'\brlogist\b', r'surveylogistic', r'proc logistic'],
    'loglinear': [r'\bpoisson\b', r'negative binomial', r'log[- ]binomial', r'\blog-?linear\b', r'adjusted (prevalence|rate|risk|incidence(?: rate)?) ratios?',
                  r'\ba(PR|RR|IRR)s?\b', r'proc genmod', r'\bglm\b.*poisson'],
    'linear': [r'(?<![-\w])linear regression', r'least[- ]squares', r'\bOLS\b', r'(?<!generalized )(?<![-\w])linear (mixed[- ](effects? )?)?models?'],
    'event-time': [r'\bcox\b', r'proportional[- ]hazards?', r'hazard ratios?', r'(?<![-\w])a?HRs?\b', r'fine[- ]gray', r'competing[- ]risks?', r'\bweibull\b'],
    'joinpoint': [r'joinpoint', r'segmented regression'],
    'generic': [r'\bregression', r'multivariable (model|analys)', r'generalized (estimating equations?|linear (mixed )?models?)', r'\bGEE\b',
                r'mixed[- ]effects? models?', r'interrupted time[- ]series', r'difference[- ]in[- ]differences?', r'\bserfling\b'],
}
CASE_SENSITIVE = {r'\bVE\b', r'\b(a)?ORs?\b', r'\b(a)?(RR|IRR|PR|HR)s?\b', r'\baORs?\b', r'\ba(PR|RR|IRR)s?\b', r'(?<![-\w])a?HRs?\b', r'\bOLS\b', r'\bGEE\b', r'\bA?APCs?\b', r'\bCIs?\b', r'\bSMRs?\b', r'\bIQR\b', r'\bSD\b'}


def hit(pats, s):
    return any(re.search(p, s) if p in CASE_SENSITIVE else re.search(p, s, re.I) for p in pats)


def reg_text(body, tab):
    return mask(body + '\n' + tab, TRAPS_REG)


@rule('reg:any')
def _(t, su, b, tab, at):
    s = reg_text(b, tab)
    return any(hit(p, s) for p in REG_PATS.values())


for _fam in ('logistic', 'loglinear', 'linear', 'event-time'):
    RULES[f'reg:{_fam}'] = (lambda fam: lambda t, su, b, tab, at: hit(REG_PATS[fam], reg_text(b, tab)))(_fam)

CI_PATS = [r'\b(95|90|99)%\s*(CI|confidence interval|credible interval)', r'\bCIs?\b', r'confidence intervals?', r'credible intervals?']
TEST_PATS = [r'chi-?square', r'χ\s*2', r'\bt[- ]tests?\b', r"fisher'?s? exact", r'wilcoxon', r'kruskal', r'mann[- ]whitney', r'\bz[- ]tests?\b',
             r'rao[- ]scott', r'\bwald\b', r'cochran', r'mantel[- ]haenszel', r'log[- ]rank', r'\bp\s*[<=≤]', r'p[- ]values?', r'mcnemar', r'\banova\b',
             r'statistically significant', r'significan(t|tly|ce)']


@rule('inf:CIs')
def _(t, su, b, tab, at):
    return hit(CI_PATS, b + tab)


@rule('inf:tests')
def _(t, su, b, tab, at):
    s = b + tab
    if RULES['design:guidance or recommendations'](t, su, b, tab, at) or hit(GUIDANCE_TITLE, t):
        return False
    named = hit(TEST_PATS[:-2], s)
    sig = hit(TEST_PATS[-2:], s) and (hit(CI_PATS, s) or named)
    inversion = hit(CI_PATS, s) and hit(RATIO_PATS, s)
    return named or sig or inversion


GUIDANCE_TITLE = [r'^(interim )?(recommendations|guidance|use of|considerations|clinical considerations|selection of)\b']
RATIO_PATS = [r'\bvaccine effectiveness\b', r'\bVE\b', r'odds ratios?', r'\b(a)?ORs?\b', r'rate ratios?', r'\b(a)?(RR|IRR|PR|HR)s?\b',
              r'(prevalence|risk|visit|hazard|incidence) ratios?']


RULES['proc:test'] = RULES['inf:tests']

WEIGHT_TRAP = [r'\b(not|un)[- ]?weighted\b', r'weighted (mean|average|moving)', r'(time|population|diffusion|T2)-weighted', r'weight-for-', r'weighting reported', r'\b(birth|body|low|high|healthy|gestational|excess|over|under)[- ]?weights?\b', r'weight (gain|loss|status|management|change)']


@rule('proc:weighting')
def _(t, su, b, tab, at):
    return hit(WEIGHT_PATS, mask(b + tab, WEIGHT_TRAP))


WEIGHT_PATS = [r'\bweighted (to|for|by|percent\w*|prevalence|estimates?|proportions?|data|samples?|analys\w*|national|population|counts?|numbers?)',
               r'(survey|sampling|sample|analytic|final|person|household|nonresponse|poststratification) weights?',
               r'weights? (were|was) (applied|used|calculated|adjusted|created|constructed|assigned)', r'\bweighting\b',
               r'\bSUDAAN\b', r'complex (survey|sampl\w*) design', r'survey design', r'\braking\b', r'post-?stratif', r'inverse probability',
               r'\bsvy\w*', r'proc survey\w+']


@rule('proc:standardization')
def _(t, su, b, tab, at):
    return hit([r'age[- ](adjusted|standardi[sz]ed)', r'standard population', r'standardi[sz]ed (mortality|incidence|morbidity) ratio', r'\bSMRs?\b',
                r'(direct|indirect)(ly)? (age[- ])?standardi'], b + tab)


@rule('depth:stratified/adjusted')
def _(t, su, b, tab, at):
    s = reg_text(b, tab)
    return (RULES['reg:any'](t, su, b, tab, at) or RULES['proc:standardization'](t, su, b, tab, at)
            or hit([r'adjusted for', r'after adjust', r'stratified analys', r'mantel[- ]haenszel', r'\badjusted\b'], s))


@rule('out:count/rate')
def _(t, su, b, tab, at):
    return hit([r'per (100|1,000|10,000|100,000|1,000,000|million|\d+ (live )?births)', r'\bincidence\b', r'rate ratios?', r'person-(years|months|days|time)',
                r'number of (cases|deaths|infections|visits|reports)', r'\bcases (were )?reported\b', r'\bcounts?\b'], b + tab)


@rule('out:continuous')
def _(t, su, b, tab, at):
    return hit([r'\bmeans?\b', r'\bIQR\b', r'interquartile', r'standard deviation', r'\bSD\b', r'\bmedian\b'], b + tab)


@rule('design:test-negative')
def _(t, su, b, tab, at):
    return hit([r'test[- ]negative'], b)


@rule('design:case-control')
def _(t, su, b, tab, at):
    s = mask(b, [r'test[- ]negative[- ](case[- –]control)?( design| study)?'])
    return hit([r'case[- –]control'], s)


@rule('design:cross-sectional')
def _(t, su, b, tab, at):
    return hit([r'cross[- ]sectional', r'noninstitutionalized', r'\brespondents?\b', r'\bsurvey(ed)?\b'], b)


@rule('design:outbreak investigation')
def _(t, su, b, tab, at):
    return hit([r'\boutbreaks?\b', r'\bclusters?\b'], t) or hit([r'outbreak investigation', r'epidemiologic(al)? investigation', r'traceback', r'attack rates?'], b)


@rule('design:before/after or trend')
def _(t, su, b, tab, at):
    return hit([r'\btrends?\b', r'\bchanges? in\b', r'before and (after|during)', r'\b(19|20)\d\d[–-](19|20)?\d\d\b'], t) or hit([r'\btrends?\b'], su)


@rule('design:evaluation or monitoring')
def _(t, su, b, tab, at):
    return hit([r'progress toward', r'\bevaluation\b', r'\btargets?\b', r'\bgoals?\b', r'\belimination\b'], t + su)


@rule('design:guidance or recommendations')
def _(t, su, b, tab, at):
    return hit([r'recommendations? of the advisory committee', r'\bACIP\b', r'interim (clinical )?(guidance|considerations|recommendations)',
                r'considerations for', r'recommendations? for (the )?use'], t + su)


@rule('design:review or synthesis')
def _(t, su, b, tab, at):
    return hit([r'\bGRADE\b', r'grading of recommendations', r'evidence to recommendations', r'\bEtR\b', r'systematic (literature )?review', r'meta-analys'], b)


@rule('design:case report/series')
def _(t, su, b, tab, at):
    return hit([r'\bthe patient\b', r'\ba patient\b', r'case report', r'\bindex (patient|case)\b', r'\bpatient [A-F]\b'], b)


RACE_WORDS = [r'\bblack\b', r'\bwhite\b', r'\bhispanic\b', r'\basian\b', r'american indian|\bai/an\b', r'pacific islander|\bnh/?opi\b', r'multiracial|multiple races']
SES_WORDS = [r'\bincome\b', r'\bpoverty\b', r'\beducation(al)? (level|attainment)', r'high school', r'\binsurance\b', r'\bmedicaid\b', r'\brural', r'\burban',
             r'social vulnerability|\bSVI\b', r'\bdisabilit', r'\bnativity\b', r'\bforeign[- ]born\b', r'\blanguage\b', r'\bsexual (orientation|minority)',
             r'\bgay\b', r'\blesbian\b', r'\bbisexual\b', r'\btransgender\b']
AGE_LEVELS = re.compile(r'\b\d{1,2}\s*[–-]\s*\d{1,2}\b|[≥≤]\s*\d{1,2}\b')


def race_levels(s):
    return sum(bool(re.search(p, s, re.I)) for p in RACE_WORDS)


@rule('groupwise:any')
def _(t, su, b, tab, at):
    if at == 'quickstats':
        return hit([r'\bby (\w+ )*(age|age group|race|hispanic origin|family income|income|poverty|education|urbanization|urbanicity|'
                    r'insurance|health insurance|disability)'], t)
    age_tab = re.search(r'\bage', tab, re.I) and len(set(AGE_LEVELS.findall(tab))) >= 3
    groups = race_levels(tab) >= 3 or bool(age_tab) or (race_levels(b) >= 3 and hit([r'\bby race', r'racial and ethnic groups'], b))
    return groups and (hit(CI_PATS, b + tab) or hit(TEST_PATS, b + tab) or hit(SPD_CONTRAST, b))


SPD_GROUP = (r'(black|white|hispanic|latino|asian|american indian|ai/an|alaska native|pacific islander|multiracial|racial|ethnic|income|poverty|fpl|'
             r'rural|urban|nonmetropolitan|metropolitan|uninsured|insured|medicaid|education|high school|college|disabilit|gay|bisexual|lesbian|'
             r'transgender|sexual minority|foreign[- ]born|u\.s\.[- ]born|svi|vulnerab|depriv|homeless|incarcerat)')
SPD_TITLE = [r'\bby (\w+ )?(race|hispanic origin|family income|income|poverty|education|urbanization|urbanicity|insurance|health insurance|'
             r'disability|sexual orientation|nativity|county urbanization|social vulnerability)', r'\bracial\b', r'\bethnic\b', r'disparit',
             r'\bequit', r'\bincome\b', r'\brural', r'\bmedicaid\b']
SPD_CONTRAST = [r'(highest|lowest|higher|lower|greater|more likely|less likely|increased with|decreased with|exceeded)[^.;]{0,90}\b(among|in|for|than)\b[^.;]{0,50}\b'
                + SPD_GROUP, r'\bby (race|race and ethnicity|race/ethnicity|income|household income|education|urbanicity|urbanization|insurance)\b',
                r'\bacross (racial|ethnic|income|socioeconomic) ', r'compared with (non-hispanic )?(white|black|hispanic|asian|insured|uninsured|urban|rural)\b',
                r'stratified by[^.;]{0,40}' + SPD_GROUP, r'(tertiles?|quartiles?|quintiles?)[^.;]{0,40}(poverty|income|svi|social vulnerability|depriv)',
                r'(poverty|income|svi|social vulnerability|depriv)[^.;]{0,40}(tertiles?|quartiles?|quintiles?)',
                r'disparit[^.;]{0,60}\b(race|racial|ethnic|income|black|hispanic|rural)']


@rule('eqdim:compared non-age/sex')
def _(t, su, b, tab, at):
    if hit(SPD_TITLE, t):
        return True
    return hit(SPD_CONTRAST, su) or (hit(SPD_CONTRAST, b) and (race_levels(tab) >= 2 or hit(SES_WORDS, tab) or at == 'quickstats'))


RULES['eqband:2+'] = RULES['eqdim:compared non-age/sex']

DISPARITY = [r'disparit', r'inequit', r'inequalit', r'\bequit(y|able)\b', r'times as (high|likely|many)', r'\b\d+(\.\d+)? times (higher|greater|as)',
             r'rate ratios?', r'social determinants', r'structural', r'racism', r'\baccess to\b', r'\bbarriers?\b']


FRAMING = [r'disparit', r'inequit', r'inequalit', r'\bequit(y|able)\b', r'disproportionate']


@rule('eqband:3+')
def _(t, su, b, tab, at):
    # a candidate set for the full equity reading: groups by social position, plus disparity framing anywhere in the text
    return RULES['eqband:2+'](t, su, b, tab, at) and hit(FRAMING, b)


def predict(text, atype):
    sec = sections(text)
    return {lab: bool(f(*sec, atype)) for lab, f in RULES.items()}


def score(split, out):
    rows = load(split)
    tally = collections.defaultdict(collections.Counter)
    misses = []
    for i, vol, atype, text, L in rows:
        pr = predict(text, atype)
        for lab, p in pr.items():
            truth = lab in L if not lab.startswith('eqband:') else truth_band(lab, L)
            k = ('tp' if truth else 'fp') if p else ('fn' if truth else 'tn')
            tally[lab][k] += 1
            if k in ('fp', 'fn'):
                misses.append((lab, k, i, vol, atype))
    with open(f'{out}/surface_scores_{split}.tsv', 'w') as o:
        o.write('label\tn_true\ttp\tfp\tfn\ttn\tprecision\trecall\tf1\n')
        for lab in RULES:
            c = tally[lab]
            p = c['tp'] / (c['tp'] + c['fp']) if c['tp'] + c['fp'] else float('nan')
            r_ = c['tp'] / (c['tp'] + c['fn']) if c['tp'] + c['fn'] else float('nan')
            f1 = 2 * p * r_ / (p + r_) if p + r_ else float('nan')
            o.write(f"{lab}\t{c['tp'] + c['fn']}\t{c['tp']}\t{c['fp']}\t{c['fn']}\t{c['tn']}\t{p:.3f}\t{r_:.3f}\t{f1:.3f}\n")
    with open(f'{out}/surface_misses_{split}.tsv', 'w') as o:
        o.write('label\tkind\tid\tvolume\tarticle_type\n')
        for m in misses:
            o.write('\t'.join(m) + '\n')
    print(len(rows), 'articles;', len(RULES), 'rules;', len(misses), 'misses ->', f'{out}/surface_scores_{split}.tsv')


def why(split, out, label):
    """For one label's misses: what fired (false positives) or the record's evidence (false negatives), to a file."""
    rows = load(split)
    f = RULES[label]
    with open(f'{out}/why_{label.replace(":", "_").replace("/", "-")}_{split}.txt', 'w') as o:
        for i, vol, atype, text, L in rows:
            sec = sections(text)
            p = bool(f(*sec, atype))
            truth = label in L if not label.startswith('eqband:') else truth_band(label, L)
            if p == truth:
                continue
            o.write(f'== {i} {atype} {"FP" if p else "FN"} | {sec[0][:100]}\n')
            if p:
                s = sec[2] + '\n' + sec[3]
                fam = label.split(':', 1)[1]
                own = (REG_PATS.get(fam) or [v for vs in REG_PATS.values() for v in vs] if label.startswith('reg:')
                       else TEST_PATS if label in ('inf:tests', 'proc:test') else CI_PATS if label == 'inf:CIs'
                       else FRAMING if label == 'eqband:3+' else WEIGHT_PATS if label == 'proc:weighting'
                       else SES_WORDS + RACE_WORDS)
                for pats in own:
                    m = re.search(pats, s) if pats in CASE_SENSITIVE else re.search(pats, s, re.I)
                    if m:
                        o.write(f'   [{pats}] …{s[max(0, m.start() - 70):m.end() + 50]!r}\n')
            else:
                key = label if label in L else None
                for q in (L.get(key) or [])[:3]:
                    o.write(f'   ev: {q[:160]!r}\n')
    print('misses written for', label)


def truth_band(lab, L):
    b = lab.split(':', 1)[1]
    if b == '2+':
        return 'eqband:2' in L or 'eqband:3-4' in L
    return f'eqband:{b}' in L


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=('mine', 'score', 'why'))
    ap.add_argument('--split', default='dev', choices=tuple(SPLITS))
    ap.add_argument('--out', default='reports')
    ap.add_argument('--label')
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    if a.cmd == 'why':
        if a.split == 'test':
            raise SystemExit('why reads misses for tuning: dev split only')
        why(a.split, a.out, a.label)
    else:
        (mine if a.cmd == 'mine' else score)(a.split, a.out)
