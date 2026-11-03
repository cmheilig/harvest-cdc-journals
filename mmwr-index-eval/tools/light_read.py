"""Light reads on the Claude API (LIGHT_BRIEF.md): data focus, outcome types, depth, design and methods sentences.

    python3 tools/light_read.py submit VOLS [--model haiku|sonnet] [--budget USD] [--dry-run]
                                                     # one or more batches for the packets of VOLS (e.g. 71-73, 48-70)
    python3 tools/light_read.py poll                 # status of open light batches
    python3 tools/light_read.py fixup [--model M] [--budget USD] [--dry-run]
                                                     # one fix-up turn, as a batch, for invalid or unparsed reads
    python3 tools/light_read.py collect              # results of ended batches -> out/light/<model>[-<version>]/<vol>/<id>.json
    python3 tools/light_read.py status               # counts by status and spend, per model
    python3 tools/light_read.py compare [--model M] [--version light-1.0]
                                                     # light records vs the production records (vols 71-73, older slice)

The test comes first: submit 71-73 with each model to be considered, compare, and only then the full run.
Packets: prod_packets/<vol>/ (tools/build_packets.py); the user turn is article.txt and the table CSVs (no prepass.json).
The driver checks every quote against the article and its length (25 words); a failing record is kept as invalid.
A fix-up resends the original turn, the reply as returned (thinking included: append-only) and the driver's errors, as
tools/api_read.py does. The corrected record replaces the first only if it passes every check (`fixed`); otherwise the
first record stays and the attempt is `failed`. Nothing in a record is changed by the driver itself.
Spend is checked against one budget line with tools/api_read.py: both ledgers count. Prints counts and ids only.
Credentials: ANTHROPIC_API_KEY (the project workspace key), as for tools/api_read.py.
"""
import collections, json, os, re, sys

import api_read as A

BRIEF, VERSION = 'LIGHT_BRIEF.md', 'light-1.1'
MODELS = {'haiku': 'claude-haiku-5-5', 'sonnet': 'claude-sonnet-5-5'}
EFFORT, MAX_TOKENS = 'medium', 16000
# $ per 1M tokens, standard (claude-api skill, cached 2026-10-06); Haiku 5.5 is dearer above 100K prompt tokens
PRICE = {'claude-haiku-5-5': (0.10, 0.50), 'claude-sonnet-5-5': (2.00, 10.00)}
HAIKU_LONG, LONG_AT = (0.50, 2.50), 100_000
OUT_TOKENS = 2500                    # estimate until the ledger has 20 reads of the model
BATCH_MB = 180                       # Message Batches accept 256 MB; stay well under
LEDGER, BATCHES, OUT = 'light_ledger.tsv', 'light_batches.tsv', 'out/light'
LEDGER_COLS = ['time', 'id', 'volume', 'attempt', 'model', 'effort', 'brief_version', 'batch_id', 'request_id',
               'input_tokens', 'output_tokens', 'cost_usd', 'status', 'note']
BATCH_COLS = ['batch_id', 'model', 'volumes', 'n', 'est_usd', 'submitted', 'status', 'brief_version', 'kind']
FIXABLE = {'invalid', 'unparsed'}

FIELDS = {'outcome_types': {'continuous', 'binary/categorical', 'count/rate', 'event-time'},
          'analytic_depth': {'summary', 'comparison', 'stratified/adjusted'},
          'study_design': {'cross-sectional', 'retrospective cohort', 'prospective cohort', 'case-control', 'test-negative',
                           'screening method', 'before/after or trend', 'ecologic', 'outbreak investigation',
                           'surveillance/descriptive', 'diagnostic accuracy', 'guidance or recommendations', 'case report/series',
                           'cost/economic', 'non-randomized trial', 'evaluation or monitoring', 'review or synthesis', 'mixed/other'}}
FOCUS = {'yes', 'limited', 'none'}
LOC = re.compile(r'^(title|summary|text|note|ocr|table \d+|figure \d+)$')


def rec_dir(model, version=None):
    """Records of one model and brief version; light-1.0 kept the plain model folder."""
    version = version or VERSION
    return f'{OUT}/{model}' if version == 'light-1.0' else f'{OUT}/{model}-{version}'


def vols_of(spec):
    """'71-73', '48-70', '71,73', or a named packet set such as 'older-slice' (comma-separated with the others)."""
    out = []
    for part in spec.split(','):
        a, _, b = part.partition('-')
        if not a.isdigit():
            out.append(part)
            continue
        out += list(range(int(a), int(b) + 1)) if b else [int(a)]
    return sorted(set(out), key=str, reverse=True)


def packets(vols):
    """[(id, vol)] of every packet in prod_packets/<vol>/frame.tsv."""
    out = []
    for v in vols:
        fr = f'{A.PACKETS}/{v}/frame.tsv'
        if not os.path.exists(fr):
            sys.exit(f'no {fr}: run python3 tools/build_packets.py vol {v}')
        out += [(r['id'], v) for r in A.read_tsv(fr) if r['status'] == 'packet']
    return out


def packet_text(v, i):
    d = f'{A.PACKETS}/{v}/{i}'
    tables = sorted(os.listdir(f'{d}/tables'), key=A._tnum) if os.path.isdir(f'{d}/tables') else []
    parts = [f'Article {i}.']
    for name in ['article.txt'] + [f'tables/{t}' for t in tables]:
        parts.append(f'=== {name} ===\n' + open(f'{d}/{name}', encoding='utf-8').read())
    return '\n\n'.join(parts)


def system():
    return [{'type': 'text', 'text': open(BRIEF, encoding='utf-8').read()}]


def params(model, text):
    return {'model': model, 'max_tokens': MAX_TOKENS, 'system': system(), 'messages': [{'role': 'user', 'content': text}],
            'output_config': {'effort': EFFORT}}


# ---------- checks

def norm(s):
    s = s.replace('’', "'").replace('‘', "'").replace('“', '"').replace('”', '"').replace('–', '-').replace('—', '-')
    return re.sub(r'[^a-z0-9%]+', ' ', s.lower()).strip()


def check(rec, i, text):
    """Shape, values, locations, quote length, and that each quote is in the article (case and punctuation aside)."""
    errs, hay = [], norm(text)
    if rec.get('id') != i:
        errs.append(f"record id {rec.get('id')!r} is not {i}")
    df = rec.get('data_focus')
    if not (isinstance(df, dict) and isinstance(df.get('v'), str) and df['v'] in FOCUS):
        errs.append('data_focus.v must be yes, limited or none')
    quotes = list((df or {}).get('ev', []) if isinstance(df, dict) else [])
    for f, allowed in FIELDS.items():
        vals = rec.get(f)
        if not isinstance(vals, list):
            errs.append(f'{f} must be a list')
            continue
        for x in vals:
            if not (isinstance(x, dict) and isinstance(x.get('v'), str) and x['v'] in allowed):
                errs.append(f'{f}: value {(x.get("v") if isinstance(x, dict) else x)!r} is not allowed'[:120])
            else:
                quotes += x.get('ev', [])
    mq = rec.get('methods_quotes')
    if not isinstance(mq, list) or len(mq) > 6:
        errs.append('methods_quotes must be a list of at most 6')
    else:
        quotes += mq
    if rec.get('confidence') not in ('high', 'medium', 'low'):
        errs.append('confidence must be high, medium or low')
    for e in quotes:
        if not (isinstance(e, dict) and isinstance(e.get('q'), str) and isinstance(e.get('loc'), str)):
            errs.append('each quote is {"loc", "q"}')
            continue
        if not LOC.match(e['loc']):
            errs.append(f"loc {e['loc']!r} is not allowed")
        if len(e['q'].split()) > 25:
            errs.append(f"quote over 25 words: {e['q'][:60]}…")
        if norm(e['q']) and norm(e['q']) not in hay:
            errs.append(f"quote not found in the article: {e['q'][:60]}…")
    return errs


# ---------- cost

def cost_of(model, us, batch=True):
    pin, pout = PRICE[model]
    if model == 'claude-haiku-5-5' and us['input'] > LONG_AT:
        pin, pout = HAIKU_LONG
    return (us['input'] * pin + us['output'] * pout) / 1e6 * (A.BATCH_DISCOUNT if batch else 1)


def spent():
    return A.spent() + sum(float(r['cost_usd']) for r in A.read_tsv(LEDGER))


def pending():
    return A.pending() + sum(float(b['est_usd']) for b in A.read_tsv(BATCHES) if b['status'] != 'collected')


def estimate(model, texts):
    reads = [r for r in A.read_tsv(LEDGER) if r['model'] == model and r['status'] in ('ok', 'invalid')]
    t_out = sum(int(r['output_tokens']) for r in reads) / len(reads) if len(reads) >= 20 else OUT_TOKENS
    t_sys = len(system()[0]['text']) / A.CHARS_PER_TOKEN
    return sum(cost_of(model, {'input': t_sys + len(t) / A.CHARS_PER_TOKEN, 'output': t_out}) for t in texts)


# ---------- commands

def batches():
    """The batch log, rewritten first if its header predates a column (older rows read as kind `read`)."""
    rows = A.read_tsv(BATCHES)
    if os.path.exists(BATCHES) and open(BATCHES).readline().rstrip('\n').split('\t') != BATCH_COLS:
        A.write_tsv(BATCHES, BATCH_COLS, rows)
    return rows


def cmd_submit(spec, model, budget, dry):
    vols = vols_of(spec)
    done = {(r['id'], r['model']) for r in A.read_tsv(LEDGER) if r['brief_version'] == VERSION}   # a new brief re-reads
    if any(b['status'] != 'collected' and b['model'] == model for b in batches()):
        sys.exit(f'{model} has an open light batch: poll and collect it first')
    todo = [(i, v) for i, v in packets(vols) if (i, model) not in done]
    if not todo:
        sys.exit(f'{model}: every packet of {spec} already has a {VERSION} read')
    texts = {i: packet_text(v, i) for i, v in todo}
    est = estimate(model, texts.values())
    left = budget - spent() - pending()
    print(f'{model} ({VERSION}), vols {spec}: {len(todo)} requests; estimate ~${est:.2f} (batch)')
    print(f'budget ${budget:.2f}: spent ${spent():.2f} (both ledgers), open batches ~${pending():.2f}, remaining ~${left:.2f}')
    if est > left:
        sys.exit('refused: the estimate exceeds the remaining budget')
    chunks, cur, size = [], [], 0
    for i, v in todo:
        req = {'custom_id': i, 'params': params(model, texts[i])}
        b = len(json.dumps(req).encode())
        if cur and size + b > BATCH_MB * 1e6:
            chunks.append(cur); cur, size = [], 0
        cur.append(req); size += b
    chunks.append(cur)
    print(f'{len(chunks)} batch(es): ' + ', '.join(str(len(c)) for c in chunks) + ' requests')
    if dry:
        return
    c = A.client()
    vol_of = dict(todo)
    os.makedirs(OUT, exist_ok=True)
    for reqs in chunks:
        batch = c.messages.batches.create(requests=reqs)
        n_est = estimate(model, [texts[r['custom_id']] for r in reqs])
        A.append_tsv(BATCHES, BATCH_COLS, {'batch_id': batch.id, 'model': model, 'volumes': spec, 'n': len(reqs),
                                           'est_usd': f'{n_est:.2f}', 'submitted': A.now(), 'status': batch.processing_status,
                                           'brief_version': VERSION, 'kind': 'read'})
        with open(f'{OUT}/exposure_rows.tsv', 'a') as o:   # for the owner to append to the extraction's exposure_log.tsv
            o.writelines(f"{r['custom_id']}\t{A.now()[:10]}\tClaude API batch (light read, {model})\t"
                         f"mmwr-index-eval light read, vol {vol_of[r['custom_id']]}; records public on GitHub\n" for r in reqs)
        print(f'batch {batch.id} submitted: {len(reqs)} requests')


def feedback(raw):
    """The user turn that answers one failed read, in the words of tools/api_read.py."""
    if raw['status'] == 'unparsed':
        ask = f"The driver could not parse your reply as one JSON object ({raw['errors'][0]})."
    else:
        ask = "The driver's checks rejected your record:\n" + '\n'.join(f'- {e}' for e in raw['errors'])
    return (ask + '\nQuotes must be copied exactly from the article, at most 25 words, cut rather than reworded; `loc` is where '
            'the quote sits (title, summary, text, note, ocr, table N, figure N). Reply with the corrected full JSON record only.')


def cmd_fixup(model, budget, dry):
    rows = batches()
    if any(b['status'] != 'collected' and b['model'] == model and b.get('kind') == 'fixup' for b in rows):
        sys.exit(f'{model} has an open fix-up batch: poll and collect it first')
    s = latest(model, VERSION)
    ids = sorted(i for i, rs in s.items() if rs[-1]['attempt'] == 'read' and rs[-1]['status'] in FIXABLE)
    print(f'{model} ({VERSION}): {len(ids)} fix-ups')
    if not ids:
        return
    msgs = {}
    for i in ids:
        vol = s[i][-1]['volume']
        raw = json.load(open(f'{rec_dir(model)}/raw/{i}.json'))
        replay = [b for b in raw['content'] if b.get('type') in ('thinking', 'redacted_thinking', 'text')]
        msgs[i] = [{'role': 'user', 'content': packet_text(vol, i)}, {'role': 'assistant', 'content': replay},
                   {'role': 'user', 'content': feedback(raw)}]
    est = estimate(model, [json.dumps(m) for m in msgs.values()])
    left = budget - spent() - pending()
    print(f'estimate ~${est:.2f} (batch); budget ${budget:.2f}: spent ${spent():.2f}, open batches ~${pending():.2f}, '
          f'remaining ~${left:.2f}')
    if est > left:
        sys.exit('refused: the estimate exceeds the remaining budget')
    if dry:
        return
    c = A.client()
    reqs = [{'custom_id': i, 'params': {**params(model, ''), 'messages': msgs[i]}} for i in ids]
    batch = c.messages.batches.create(requests=reqs)
    A.append_tsv(BATCHES, BATCH_COLS, {'batch_id': batch.id, 'model': model, 'volumes': 'fixup', 'n': len(ids),
                                       'est_usd': f'{est:.2f}', 'submitted': A.now(), 'status': batch.processing_status,
                                       'brief_version': VERSION, 'kind': 'fixup'})
    print(f'fix-up batch {batch.id} submitted: {len(ids)} requests')


def cmd_poll():
    rows = batches()
    if not any(b['status'] != 'collected' for b in rows):
        print('no open light batches')
        return
    c = A.client()
    for b in rows:
        if b['status'] == 'collected':
            continue
        r = c.messages.batches.retrieve(b['batch_id'])
        b['status'] = r.processing_status
        k = r.request_counts
        print(f"{b['batch_id']} {b['model']} {b['volumes']}: {r.processing_status}; processing {k.processing}, "
              f'succeeded {k.succeeded}, errored {k.errored}, canceled {k.canceled}, expired {k.expired}')
    A.write_tsv(BATCHES, BATCH_COLS, rows)


def handle(model, vol, i, msg, batch_id, version, attempt='read'):
    content = [b.to_dict() for b in msg.content]
    u = msg.usage
    us = {'input': (u.input_tokens or 0) + (u.cache_read_input_tokens or 0) + (u.cache_creation_input_tokens or 0),
          'output': u.output_tokens or 0}
    errs, note = [], ''
    if msg.stop_reason == 'refusal':
        status, note = 'refused', str(getattr(msg.stop_details, 'category', '') or '')
    elif msg.stop_reason == 'max_tokens':
        status = 'truncated'
    else:
        try:
            rec = A.parse(content)
        except ValueError as e:
            status, errs = 'unparsed', [f'{e.__class__.__name__}: {str(e)[:120]}']
        else:
            errs = check(rec, i, packet_text(vol, i))
            rec['reader'] = {'model': msg.model, 'effort': EFFORT, 'brief_version': version, 'request_id': msg.id,
                             'batch_id': batch_id, **({'errors': errs} if errs else {})}
            status = ('invalid' if errs else 'ok') if attempt == 'read' else ('failed' if errs else 'fixed')
            if attempt == 'read' or not errs:      # a failed fix-up leaves the first record in place
                d = f'{rec_dir(model, version)}/{vol}'
                os.makedirs(d, exist_ok=True)
                with open(f'{d}/{i}.json', 'w') as o:
                    json.dump(rec, o, ensure_ascii=False, indent=1)
            note = f'{len(errs)} errors' if errs else ''
    if attempt != 'read' and status in FIXABLE:
        status = 'failed'
    os.makedirs(f'{rec_dir(model, version)}/raw', exist_ok=True)
    with open(f'{rec_dir(model, version)}/raw/{i}{"" if attempt == "read" else "." + attempt}.json', 'w') as o:
        json.dump({'id': i, 'status': status, 'message_id': msg.id, 'model': msg.model, 'stop_reason': msg.stop_reason,
                   'errors': errs, 'usage': us, 'content': content}, o, ensure_ascii=False)
    A.append_tsv(LEDGER, LEDGER_COLS, {'time': A.now(), 'id': i, 'volume': vol, 'attempt': attempt, 'model': model,
                                       'effort': EFFORT, 'brief_version': version, 'batch_id': batch_id, 'request_id': msg.id,
                                       'input_tokens': us['input'], 'output_tokens': us['output'],
                                       'cost_usd': f'{cost_of(model, us):.5f}', 'status': status, 'note': note})
    return status


def cmd_collect():
    rows = batches()
    todo = [b for b in rows if b['status'] != 'collected']
    if not todo:
        sys.exit('no open light batches')
    c = A.client()
    for b in todo:
        r = c.messages.batches.retrieve(b['batch_id'])
        if r.processing_status != 'ended':
            print(f"{b['batch_id']}: {r.processing_status}, not collected")
            continue
        kind, version = b.get('kind') or 'read', b.get('brief_version') or 'light-1.0'
        if kind == 'read':
            vol_of = {i: v for i, v in packets(vols_of(b['volumes']))}
        else:                                       # a fix-up batch spans volumes: take each id's volume from its read
            vol_of = {r['id']: r['volume'] for r in A.read_tsv(LEDGER) if r['model'] == b['model'] and r['brief_version'] == version}
        seen = {r['id'] for r in A.read_tsv(LEDGER) if r['batch_id'] == b['batch_id']}   # an interrupted collect resumes
        n = collections.Counter({'already collected': len(seen)} if seen else {})
        for res in c.messages.batches.results(b['batch_id']):
            i = res.custom_id
            if i in seen:
                continue
            if res.result.type == 'succeeded':
                n[handle(b['model'], vol_of[i], i, res.result.message, b['batch_id'], version, kind)] += 1
            else:
                n[res.result.type] += 1
                A.append_tsv(LEDGER, LEDGER_COLS, {'time': A.now(), 'id': i, 'volume': vol_of[i], 'attempt': kind,
                                                   'model': b['model'], 'effort': EFFORT, 'brief_version': version,
                                                   'batch_id': b['batch_id'], 'cost_usd': '0', 'status': res.result.type})
        b['status'] = 'collected'
        print(f"{b['batch_id']} {b['model']} {kind}: " + ', '.join(f'{k} {v}' for k, v in sorted(n.items())))
    A.write_tsv(BATCHES, BATCH_COLS, rows)
    cmd_status()


def latest(model, version):
    """{id: [ledger rows in order]} for one model and brief version."""
    s = {}
    for r in A.read_tsv(LEDGER):
        if r['model'] == model and r['brief_version'] == version:
            s.setdefault(r['id'], []).append(r)
    return s


def cmd_status():
    rows = A.read_tsv(LEDGER)
    for m, ver in sorted({(r['model'], r['brief_version']) for r in rows}):
        s = latest(m, ver)
        n = collections.Counter(rs[-1]['status'] for rs in s.values())
        cost = sum(float(r['cost_usd']) for rs in s.values() for r in rs)
        print(f'{m} {ver}: {len(s)} articles; ' + ', '.join(f'{k} {v}' for k, v in sorted(n.items()))
              + f'; ${cost:.2f} (${cost / max(len(s), 1):.4f}/article)')
    print(f'spent ${spent():.2f} in both ledgers; open batches ~${pending():.2f}')


def cmd_compare(model, version):
    """Field by field against out/prod/<vol>/<id>.json for vols 71-73 and the older slice: agreement, precision, recall."""
    tally = collections.defaultdict(collections.Counter)
    n = 0
    for v in (71, 72, 73, 'older-slice'):
        d = f'{rec_dir(model, version)}/{v}'
        if not os.path.isdir(d):
            continue
        for f in sorted(os.listdir(d)):
            light = json.load(open(f'{d}/{f}'))
            prod_path = f'out/prod/{v}/{f}'
            if not os.path.exists(prod_path):
                continue
            prod, n = json.load(open(prod_path)), n + 1
            g = 'older slice' if v == 'older-slice' else 'vols 71-73'
            a, b = (light.get('data_focus') or {}).get('v'), (prod.get('data_focus') or {}).get('v')
            tally[(g, 'data_focus')]['agree' if a == b else 'disagree'] += 1
            for fld in FIELDS:
                la = {x['v'] for x in light.get(fld, []) if isinstance(x, dict) and isinstance(x.get('v'), str)}
                pb = {x['v'] for x in prod.get(fld, [])}
                tally[(g, fld)]['agree' if la == pb else 'disagree'] += 1
                for val in la | pb:
                    k = 'tp' if val in la and val in pb else 'fp' if val in la else 'fn'
                    tally[(g, f'{fld}: {val}')][k] += 1
    out = f'reports/light_vs_opus_{model}.tsv' if version == 'light-1.0' else f'reports/light_vs_opus_{model}_{version}.tsv'
    with open(out, 'w') as o:
        o.write('set\tfield\tagree\tdisagree\ttp\tfp\tfn\tprecision\trecall\n')
        for g, k in sorted(tally, key=lambda x: (x[0] != 'vols 71-73', x[1])):
            c = tally[(g, k)]
            p = c['tp'] / (c['tp'] + c['fp']) if c['tp'] + c['fp'] else float('nan')
            r = c['tp'] / (c['tp'] + c['fn']) if c['tp'] + c['fn'] else float('nan')
            o.write(f"{g}\t{k}\t{c['agree']}\t{c['disagree']}\t{c['tp']}\t{c['fp']}\t{c['fn']}\t{p:.3f}\t{r:.3f}\n")
    print(f'{model} ({version}): {n} articles compared with the production records -> {out}')


def main(argv):
    args = list(argv)
    opt = {}
    for k in ('--model', '--budget', '--version'):
        if k in args:
            j = args.index(k); opt[k] = args[j + 1]; del args[j:j + 2]
    dry = '--dry-run' in args
    args = [a for a in args if a != '--dry-run']
    model = MODELS[opt.get('--model', 'haiku')]
    budget = float(opt.get('--budget', A.BUDGET))
    cmd, rest = (args[0], args[1:]) if args else ('', [])
    if cmd == 'submit' and len(rest) == 1:
        cmd_submit(rest[0], model, budget, dry)
    elif cmd == 'poll':
        cmd_poll()
    elif cmd == 'fixup':
        cmd_fixup(model, budget, dry)
    elif cmd == 'collect':
        cmd_collect()
    elif cmd == 'status':
        cmd_status()
    elif cmd == 'compare':
        cmd_compare(model, opt.get('--version', VERSION))
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main(sys.argv[1:])
