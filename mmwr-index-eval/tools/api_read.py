"""Production reads on the Claude API (PRODUCTION_PLAN.md §3, HANDOFF_LOCAL.md §4.2). Prints counts and ids only.

    python3 tools/api_read.py prefix                        # sizes and SHA-256 of the fixed prefix (no API call)
    python3 tools/api_read.py calibrate [N] [--vol VOL]     # count_tokens on the prefix and N packets (default 5)
    python3 tools/api_read.py slice ID [ID ...]             # synchronous reads of out/v5 articles, compared with compare.norm
    python3 tools/api_read.py submit VOL [--dry-run]        # one batch for prod_packets/<VOL>/ (cache warmed, budget checked)
    python3 tools/api_read.py poll                          # status of open batches
    python3 tools/api_read.py collect VOL                   # results of ended batches -> out/prod/<VOL>/<id>.json, ledger
    python3 tools/api_read.py fixup VOL [--sync] [--dry-run]   # one validator fix-up turn for invalid or unparsed reads
    python3 tools/api_read.py retry VOL [--dry-run]         # one synchronous retry, with fallbacks, for refused/truncated/errored
    python3 tools/api_read.py status [VOL]                  # counts by status; spend against the budget
    option: --budget USD (default BUDGET)

Request: `system` = API_PREAMBLE.md, then READER_BRIEF.md and the two examples exactly as at tag BRIEF_TAG, with a 1-hour
cache breakpoint on the last block; the user turn is the packet. Thinking is adaptive (always on; not sent); effort is set.
Batches accept no server-side fallbacks, so a refusal is retried once synchronously with `fallbacks: "default"`.
A fix-up resends the original turn and the model's response unchanged (thinking blocks included), then the validator's
messages. Records that pass tools/validate.py go to out/prod/<VOL>/<id>.json; every response goes to raw/.
prod_ledger.tsv has one row per attempt; prod_batches.tsv one row per batch. Credentials: ANTHROPIC_API_KEY (workspace key).
"""
import csv, datetime, hashlib, json, os, re, subprocess, sys

from validate import check

MODEL, EFFORT, MAX_TOKENS, RETRY_MAX_TOKENS = 'claude-opus-5-5', 'medium', 32000, 64000
BRIEF_TAG = 'brief-v5.1'
READER_ID, ROLE = 'prod', 'judgment'
FALLBACK_BETA = 'server-side-fallback-2026-07-01'
BUDGET = 100.0                 # owner's spending note (HANDOFF_LOCAL.md): ~$100 on the platform, then reassess
# $ per 1M tokens, claude-opus-5-5 (claude-api skill, 2026-10): standard; batches halve every class
PRICE = {'input': 4.00, 'output': 20.00, 'cache_read': 0.20, 'write_1h': 8.00, 'write_5m': 5.00}
BATCH_DISCOUNT = 0.5
# estimate defaults until calibrate and the ledger have figures (tools/estimate_cost.py)
# OUT_TOKENS: the 3-read slice gave 2,659-5,515 output tokens (thinking included); 4,500 leans to full reports
CHARS_PER_TOKEN, OUT_TOKENS, FIXUP_SHARE = 2.3, 4500, 0.15

PREAMBLE, BRIEF, EXAMPLES = 'API_PREAMBLE.md', 'READER_BRIEF.md', ('examples/mm6925a1.json', 'examples/mm7223a3.json')
PACKETS, LEDGER, BATCHES, CALIB = 'prod_packets', 'prod_ledger.tsv', 'prod_batches.tsv', 'prod_calibration.tsv'
LEDGER_COLS = ['time', 'id', 'volume', 'attempt', 'environment', 'model', 'effort', 'brief_tag', 'prefix_sha', 'batch_id',
               'request_id', 'input_tokens', 'cache_write_tokens', 'cache_read_tokens', 'output_tokens', 'cost_usd', 'status', 'note']
BATCH_COLS = ['batch_id', 'volume', 'kind', 'n', 'est_usd', 'prefix_sha', 'submitted', 'status']
DONE = {'ok', 'fixed'}
FIXABLE = {'invalid', 'unparsed'}
RETRYABLE = {'refused', 'truncated', 'errored', 'expired', 'canceled'}


# ---------- files

def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='seconds')


def read_tsv(path):
    return list(csv.DictReader(open(path), delimiter='\t')) if os.path.exists(path) else []


def write_tsv(path, cols, rows):
    with open(path, 'w') as o:
        o.write('\t'.join(cols) + '\n')
        o.writelines('\t'.join(str(r.get(c, '')) for c in cols) + '\n' for r in rows)


def append_tsv(path, cols, row):
    new = not os.path.exists(path)
    with open(path, 'a') as o:
        if new:
            o.write('\t'.join(cols) + '\n')
        o.write('\t'.join(str(row.get(c, '')) for c in cols) + '\n')


def outdir(vol):
    return 'out/prod_slice' if vol == 'slice' else f'out/prod/{vol}'


def packet_dir(vol, i):
    return f'packets/{i}' if vol == 'slice' else f'{PACKETS}/{vol}/{i}'


def frame_packets(vol):
    path = f'{PACKETS}/{vol}/frame.tsv'
    if not os.path.exists(path):
        sys.exit(f'no {path}: run python3 tools/build_packets.py vol {vol}')
    return [r['id'] for r in read_tsv(path) if r['status'] == 'packet']


# ---------- request

def _tagged(path):
    r = subprocess.run(['git', 'show', f'{BRIEF_TAG}:./{path}'], capture_output=True)
    if r.returncode:
        sys.exit(f'cannot read {path} at tag {BRIEF_TAG}')
    if open(path, 'rb').read() != r.stdout:
        sys.exit(f'{path} differs from tag {BRIEF_TAG}; a brief change needs a new tag (PRODUCTION_PLAN.md §1)')
    return r.stdout.decode('utf-8')


def fixed_prefix():
    """The cached system blocks and their SHA-256. Byte-stable: any change invalidates the cache and the tag check."""
    ex = '\n\n'.join(f'=== {p} ===\n{_tagged(p)}' for p in EXAMPLES)
    system = [{'type': 'text', 'text': open(PREAMBLE, encoding='utf-8').read()},
              {'type': 'text', 'text': f'=== {BRIEF} ===\n{_tagged(BRIEF)}'},
              {'type': 'text', 'text': ex, 'cache_control': {'type': 'ephemeral', 'ttl': '1h'}}]
    return system, hashlib.sha256(json.dumps(system, sort_keys=True).encode()).hexdigest()[:12]


def _tnum(name):
    m = re.search(r'_T(\d+)\.csv$', name)
    return (int(m.group(1)) if m else 0, name)


def packet_text(vol, i):
    d = packet_dir(vol, i)
    tables = sorted(os.listdir(f'{d}/tables'), key=_tnum) if os.path.isdir(f'{d}/tables') else []
    parts = [f'Article {i}.']
    for name in ['article.txt', 'prepass.json'] + [f'tables/{t}' for t in tables]:
        parts.append(f'=== {name} ===\n' + open(f'{d}/{name}', encoding='utf-8').read())
    return '\n\n'.join(parts)


def params(system, messages, max_tokens=MAX_TOKENS):
    return {'model': MODEL, 'max_tokens': max_tokens, 'system': system, 'messages': messages,
            'output_config': {'effort': EFFORT}}


def read_messages(vol, i):
    return [{'role': 'user', 'content': packet_text(vol, i)}]


def fixup_messages(vol, i, raw):
    """Append-only: the original turn, the response as returned, then the validator's messages."""
    replay = [b for b in raw['content'] if b.get('type') in ('thinking', 'redacted_thinking', 'text')]
    if raw['status'] == 'unparsed':
        ask = f"The driver could not parse your reply as one JSON object ({raw['errors'][0]})."
    else:
        ask = 'The driver\'s validator rejected your record:\n' + '\n'.join(f'- {e}' for e in raw['errors'])
    ask += '\nReply with the corrected full JSON record only: one JSON object, with no text before or after it.'
    return read_messages(vol, i) + [{'role': 'assistant', 'content': replay}, {'role': 'user', 'content': ask}]


# ---------- response

def client():
    import anthropic
    if not os.environ.get('ANTHROPIC_API_KEY'):
        sys.exit('no API key: export ANTHROPIC_API_KEY with the key minted in the project workspace')
    return anthropic.Anthropic()


def usage_of(u):
    cc = getattr(u, 'cache_creation', None)
    w1 = (getattr(cc, 'ephemeral_1h_input_tokens', 0) or 0) if cc else (u.cache_creation_input_tokens or 0)
    w5 = (getattr(cc, 'ephemeral_5m_input_tokens', 0) or 0) if cc else 0
    return {'input': u.input_tokens or 0, 'write_1h': w1, 'write_5m': w5, 'cache_read': u.cache_read_input_tokens or 0,
            'output': u.output_tokens or 0}


def cost_of(us, batch):
    c = sum(us[k] * PRICE[k] for k in PRICE) / 1e6
    return c * (BATCH_DISCOUNT if batch else 1)


def parse(content):
    text = ''.join(b.get('text', '') for b in content if b.get('type') == 'text').strip()
    text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text)
    rec = json.loads(text)
    if not isinstance(rec, dict):
        raise ValueError('reply is not a JSON object')
    return rec


def handle(vol, i, msg, attempt, env, batch_id, sha):
    """Write raw/<id>.<attempt>.json, the record if valid, and a ledger row. Returns the status."""
    content = [b.to_dict() for b in msg.content]
    us, errs, note = usage_of(msg.usage), [], ''
    if msg.stop_reason == 'refusal':
        status, note = 'refused', str(getattr(msg.stop_details, 'category', '') or '')
    elif msg.stop_reason == 'max_tokens':
        status = 'truncated'
    else:
        try:
            rec = parse(content)
        except ValueError as e:
            status, errs = 'unparsed', [f'{e.__class__.__name__}: {str(e)[:120]}']
        else:
            reader = rec['reader'] if isinstance(rec.get('reader'), dict) else {}
            rec['reader'] = {**reader, 'model': msg.model, 'reader_id': READER_ID, 'role': ROLE, 'effort': EFFORT,
                             'brief_tag': BRIEF_TAG, 'prefix_sha': sha, 'request_id': msg.id,
                             **({'batch_id': batch_id} if batch_id else {})}
            try:
                errs = check(rec) if rec.get('id') == i else [f"record id {rec.get('id')!r} is not {i}"]
            except (KeyError, TypeError, AttributeError) as e:
                errs = [f'record shape: {e.__class__.__name__} {e}']
            status = ('fixed' if attempt == 'fixup' else 'ok') if not errs else 'invalid'
            if not errs:
                with open(f'{outdir(vol)}/{i}.json', 'w') as o:
                    json.dump(rec, o, ensure_ascii=False, indent=1)
            note = f'{len(errs)} errors' if errs else ''
    if attempt == 'fixup' and status in FIXABLE:
        status = 'failed'
    os.makedirs(f'{outdir(vol)}/raw', exist_ok=True)
    with open(f'{outdir(vol)}/raw/{i}.{attempt}.json', 'w') as o:
        json.dump({'id': i, 'attempt': attempt, 'status': status, 'message_id': msg.id, 'model': msg.model,
                   'stop_reason': msg.stop_reason, 'errors': errs, 'usage': us, 'content': content}, o, ensure_ascii=False)
    ledger(i, vol, attempt, env, msg.model, sha, batch_id, msg.id, us, cost_of(us, env == 'api-batch'), status, note)
    return status


def ledger(i, vol, attempt, env, model, sha, batch_id, request_id, us, cost, status, note=''):
    append_tsv(LEDGER, LEDGER_COLS, {
        'time': now(), 'id': i, 'volume': vol, 'attempt': attempt, 'environment': env, 'model': model, 'effort': EFFORT,
        'brief_tag': BRIEF_TAG, 'prefix_sha': sha, 'batch_id': batch_id or '', 'request_id': request_id or '',
        'input_tokens': us.get('input', 0), 'cache_write_tokens': us.get('write_1h', 0) + us.get('write_5m', 0),
        'cache_read_tokens': us.get('cache_read', 0), 'output_tokens': us.get('output', 0), 'cost_usd': f'{cost:.4f}',
        'status': status, 'note': note})


def state(vol):
    """{id: [ledger rows in order]} for one volume."""
    s = {}
    for r in read_tsv(LEDGER):
        if r['volume'] == str(vol) and r['attempt'] in ('read', 'fixup', 'retry'):
            s.setdefault(r['id'], []).append(r)
    return s


def latest_raw(vol, i, rows):
    return json.load(open(f"{outdir(vol)}/raw/{i}.{rows[-1]['attempt']}.json"))


# ---------- cost control

def spent():
    return sum(float(r['cost_usd']) for r in read_tsv(LEDGER))


def pending():
    return sum(float(b['est_usd']) for b in read_tsv(BATCHES) if b['status'] != 'collected')


def estimate(packet_chars, n_prefix_chars):
    """(cached, uncached) batch cost for these packets, with FIXUP_SHARE of them needing a fix-up turn."""
    cal = read_tsv(CALIB)
    cpt = (sum(int(r['packet_chars']) for r in cal) / sum(int(r['packet_tokens']) for r in cal)) if cal else CHARS_PER_TOKEN
    t_prefix = int(cal[-1]['prefix_tokens']) if cal else n_prefix_chars / CHARS_PER_TOKEN
    reads = [r for r in read_tsv(LEDGER) if r['attempt'] == 'read' and r['status'] in ('ok', 'invalid')]
    t_out = sum(int(r['output_tokens']) for r in reads) / len(reads) if len(reads) >= 20 else OUT_TOKENS
    cached = uncached = 0.0
    for ch in packet_chars:
        t_pkt = ch / cpt
        pkt_out = t_pkt * PRICE['input'] + t_out * PRICE['output']
        fix = FIXUP_SHARE * (t_prefix * PRICE['cache_read'] + (t_pkt + t_out) * PRICE['input'] + t_out * PRICE['output'])
        cached += (t_prefix * PRICE['cache_read'] + pkt_out + fix) / 1e6
        uncached += (t_prefix * PRICE['input'] + pkt_out + fix) / 1e6
    return cached * BATCH_DISCOUNT, uncached * BATCH_DISCOUNT


def guard(est, budget):
    left = budget - spent() - pending()
    print(f'budget ${budget:.2f}: spent ${spent():.2f}, open batches ~${pending():.2f}, remaining ~${left:.2f}; '
          f'this request ~${est:.2f} (uncached)')
    if est > left:
        sys.exit('refused: the estimate exceeds the remaining budget')


# ---------- commands

def cmd_prefix():
    system, sha = fixed_prefix()
    n = sum(len(b['text']) for b in system)
    print(f'prefix {sha}: {len(system)} blocks, {n:,} chars (~{n / CHARS_PER_TOKEN:,.0f} tokens at {CHARS_PER_TOKEN} chars/token); '
          f'brief and examples match tag {BRIEF_TAG}')


def cmd_calibrate(n, vol):
    system, sha = fixed_prefix()
    vol = vol or ('73' if os.path.isdir(f'{PACKETS}/73') else 'slice')
    ids = frame_packets(vol) if vol != 'slice' else sorted(os.listdir('packets'))
    ids = [i for i in ids if os.path.isdir(packet_dir(vol, i))]
    size = {i: len(packet_text(vol, i)) for i in ids}
    ids = sorted(ids, key=size.get)
    pick = sorted({ids[round(k * (len(ids) - 1) / max(n - 1, 1))] for k in range(n)})
    c = client()
    base = c.messages.count_tokens(model=MODEL, system=system, messages=[{'role': 'user', 'content': 'x'}],
                                   output_config={'effort': EFFORT}).input_tokens
    for i in pick:
        t = c.messages.count_tokens(model=MODEL, system=system, messages=read_messages(vol, i),
                                    output_config={'effort': EFFORT}).input_tokens
        append_tsv(CALIB, ['time', 'id', 'volume', 'prefix_sha', 'prefix_tokens', 'packet_chars', 'packet_tokens'],
                   {'time': now(), 'id': i, 'volume': vol, 'prefix_sha': sha, 'prefix_tokens': base,
                    'packet_chars': size[i], 'packet_tokens': t - base})
        print(f'{i}: packet {size[i]:,} chars, {t - base:,} tokens ({size[i] / (t - base):.2f} chars/token)')
    print(f'prefix {sha}: {base:,} tokens (incl. a 1-token user turn) -> {CALIB}')


def stream(c, p, fallback=False):
    if fallback:
        with c.beta.messages.stream(**p, betas=[FALLBACK_BETA], extra_body={'fallbacks': 'default'}) as s:
            return s.get_final_message()
    with c.messages.stream(**p) as s:
        return s.get_final_message()


def cmd_slice(ids):
    """Equivalence slice (PRODUCTION_PLAN.md §2): synchronous reads of articles already read in out/v5/."""
    from compare import norm
    system, sha = fixed_prefix()
    os.makedirs(outdir('slice'), exist_ok=True)
    c = client()
    for i in ids:
        if not os.path.exists(f'out/v5/{i}.json'):
            sys.exit(f'{i} has no out/v5 record')
        st = handle('slice', i, stream(c, params(system, read_messages('slice', i))), 'read', 'api-sync', None, sha)
        if st in FIXABLE:
            raw = latest_raw('slice', i, state('slice')[i])
            st = handle('slice', i, stream(c, params(system, fixup_messages('slice', i, raw))), 'fixup', 'api-sync', None, sha)
        if st not in DONE:
            print(f'{i}: {st}')
            continue
        a, b = norm(json.load(open(f'out/v5/{i}.json'))), norm(json.load(open(f'{outdir("slice")}/{i}.json')))
        diff = sorted(k for k in a if a[k] != b.get(k))
        print(f'{i}: {st}; {len(diff)} of {len(a)} normalized fields differ from out/v5' + (f": {', '.join(diff)}" if diff else ''))


def cmd_submit(vol, dry, budget):
    system, sha = fixed_prefix()
    s = state(vol)
    ids = [i for i in frame_packets(vol) if i not in s]
    if not ids:
        sys.exit(f'vol {vol}: every packet already has a read')
    texts = {i: packet_text(vol, i) for i in ids}
    cached, uncached = estimate([len(t) for t in texts.values()], sum(len(b['text']) for b in system))
    reqs = [{'custom_id': i, 'params': params(system, [{'role': 'user', 'content': texts[i]}])} for i in ids]
    mb = len(json.dumps(reqs).encode()) / 1e6
    print(f'vol {vol}: {len(ids)} requests, {mb:.1f} MB; estimate ~${cached:.2f} with cache hits, ~${uncached:.2f} without')
    guard(uncached, budget)
    if dry:
        return
    c = client()
    warm = c.messages.create(model=MODEL, max_tokens=0, system=system, messages=[{'role': 'user', 'content': 'warmup'}],
                             output_config={'effort': EFFORT})
    us = usage_of(warm.usage)
    ledger('-', vol, 'warm', 'api-sync', warm.model, sha, '', warm.id, us, cost_of(us, False), 'ok')
    batch = c.messages.batches.create(requests=reqs)
    append_tsv(BATCHES, BATCH_COLS, {'batch_id': batch.id, 'volume': vol, 'kind': 'read', 'n': len(ids),
                                     'est_usd': f'{uncached:.2f}', 'prefix_sha': sha, 'submitted': now(),
                                     'status': batch.processing_status})
    os.makedirs(outdir(vol), exist_ok=True)
    with open(f'{outdir(vol)}/exposure_rows.tsv', 'a') as o:   # for the owner to append to the extraction's exposure_log.tsv
        o.writelines(f'{i}\t{now()[:10]}\tClaude API batch (production)\tmmwr-index-eval production read, vol {vol}; '
                     f'records public on GitHub\n' for i in ids)
    print(f'cache warmed ({us["write_1h"]:,} tokens written); batch {batch.id} submitted: {len(ids)} requests')


def cmd_poll():
    rows = read_tsv(BATCHES)
    if not any(b['status'] != 'collected' for b in rows):
        print('no open batches')
        return
    c = client()
    for b in rows:
        if b['status'] == 'collected':
            continue
        r = c.messages.batches.retrieve(b['batch_id'])
        b['status'] = r.processing_status
        k = r.request_counts
        print(f"{b['batch_id']} vol {b['volume']} {b['kind']}: {r.processing_status}; processing {k.processing}, "
              f'succeeded {k.succeeded}, errored {k.errored}, canceled {k.canceled}, expired {k.expired}')
    write_tsv(BATCHES, BATCH_COLS, rows)


def cmd_collect(vol):
    rows = read_tsv(BATCHES)
    todo = [b for b in rows if b['volume'] == str(vol) and b['status'] != 'collected']
    if not todo:
        sys.exit(f'vol {vol}: no open batches')
    c = client()
    os.makedirs(outdir(vol), exist_ok=True)
    for b in todo:
        r = c.messages.batches.retrieve(b['batch_id'])
        if r.processing_status != 'ended':
            print(f"{b['batch_id']}: {r.processing_status}, not collected")
            continue
        n = {}
        for res in c.messages.batches.results(b['batch_id']):
            if res.result.type == 'succeeded':
                st = handle(vol, res.custom_id, res.result.message, b['kind'], 'api-batch', b['batch_id'], b['prefix_sha'])
            else:
                st = res.result.type
                ledger(res.custom_id, vol, b['kind'], 'api-batch', '', b['prefix_sha'], b['batch_id'], '', {}, 0.0, st,
                       getattr(getattr(res.result, 'error', None), 'type', '') or '')
            n[st] = n.get(st, 0) + 1
        b['status'] = 'collected'
        print(f"{b['batch_id']} vol {vol} {b['kind']}: " + ', '.join(f'{k} {v}' for k, v in sorted(n.items())))
    write_tsv(BATCHES, BATCH_COLS, rows)
    cmd_status(vol)


def cmd_fixup(vol, sync, dry, budget):
    system, sha = fixed_prefix()
    s = state(vol)
    ids = sorted(i for i, rows in s.items() if rows[-1]['status'] in FIXABLE and not any(r['attempt'] == 'fixup' for r in rows))
    print(f'vol {vol}: {len(ids)} fix-ups' + (f": {' '.join(ids)}" if ids else ''))
    if not ids:
        return
    msgs = {i: fixup_messages(vol, i, latest_raw(vol, i, s[i])) for i in ids}
    _, uncached = estimate([len(json.dumps(m)) for m in msgs.values()], sum(len(b['text']) for b in system))
    guard(uncached * (2 if sync else 1), budget)   # the estimate is at batch prices; synchronous costs twice that
    if dry:
        return
    c = client()
    if sync:
        n = {}
        for i in ids:
            st = handle(vol, i, stream(c, params(system, msgs[i])), 'fixup', 'api-sync', None, sha)
            n[st] = n.get(st, 0) + 1
        print(', '.join(f'{k} {v}' for k, v in sorted(n.items())))
        return
    batch = c.messages.batches.create(requests=[{'custom_id': i, 'params': params(system, msgs[i])} for i in ids])
    append_tsv(BATCHES, BATCH_COLS, {'batch_id': batch.id, 'volume': vol, 'kind': 'fixup', 'n': len(ids),
                                     'est_usd': f'{uncached:.2f}', 'prefix_sha': sha, 'submitted': now(),
                                     'status': batch.processing_status})
    print(f'fix-up batch {batch.id} submitted: {len(ids)} requests')


def cmd_retry(vol, dry, budget):
    system, sha = fixed_prefix()
    s = state(vol)
    ids = sorted(i for i, rows in s.items() if rows[-1]['status'] in RETRYABLE and not any(r['attempt'] == 'retry' for r in rows))
    print(f'vol {vol}: {len(ids)} retries' + (f": {' '.join(ids)}" if ids else ''))
    if not ids:
        return
    _, uncached = estimate([len(packet_text(vol, i)) for i in ids], sum(len(b['text']) for b in system))
    guard(uncached * 2, budget)                    # synchronous: no batch discount
    if dry:
        return
    c = client()
    n = {}
    for i in ids:
        mt = RETRY_MAX_TOKENS if s[i][-1]['status'] == 'truncated' else MAX_TOKENS
        msg = stream(c, params(system, read_messages(vol, i), mt), fallback=True)
        st = handle(vol, i, msg, 'retry', 'api-sync', None, sha)
        n[st] = n.get(st, 0) + 1
        if msg.model != MODEL:
            print(f'{i}: served by {msg.model}')
    print(', '.join(f'{k} {v}' for k, v in sorted(n.items())))


def cmd_status(vol=None, budget=BUDGET):
    vols = [vol] if vol else sorted({r['volume'] for r in read_tsv(LEDGER) if r['volume'] not in ('', 'slice')}, reverse=True)
    for v in vols:
        s = state(v)
        n = {}
        for rows in s.values():
            n[rows[-1]['status']] = n.get(rows[-1]['status'], 0) + 1
        total = len(frame_packets(v)) if os.path.exists(f'{PACKETS}/{v}/frame.tsv') else len(s)
        cost = sum(float(r['cost_usd']) for r in read_tsv(LEDGER) if r['volume'] == str(v))
        print(f'vol {v}: {total} packets, {len(s)} read; ' + ', '.join(f'{k} {c}' for k, c in sorted(n.items()))
              + f'; ${cost:.2f} (${cost / max(len(s), 1):.3f}/article)')
        bad = sorted(i for i, rows in s.items() if rows[-1]['status'] not in DONE)
        if bad:
            print('  not done: ' + ' '.join(bad))
    print(f'spent ${spent():.2f} of ${budget:.2f}; open batches ~${pending():.2f}')


def main(argv):
    args = list(argv)
    budget = BUDGET
    if '--budget' in args:
        k = args.index('--budget'); budget = float(args[k + 1]); del args[k:k + 2]
    vol = None
    if '--vol' in args:
        k = args.index('--vol'); vol = args[k + 1]; del args[k:k + 2]
    flags = {a for a in args if a.startswith('--')}
    args = [a for a in args if not a.startswith('--')]
    dry, sync = '--dry-run' in flags, '--sync' in flags
    cmd, rest = (args[0], args[1:]) if args else ('', [])
    if cmd == 'prefix':
        cmd_prefix()
    elif cmd == 'calibrate':
        cmd_calibrate(int(rest[0]) if rest else 5, vol)
    elif cmd == 'slice' and rest:
        cmd_slice(rest)
    elif cmd == 'submit' and len(rest) == 1:
        cmd_submit(rest[0], dry, budget)
    elif cmd == 'poll':
        cmd_poll()
    elif cmd == 'collect' and len(rest) == 1:
        cmd_collect(rest[0])
    elif cmd == 'fixup' and len(rest) == 1:
        cmd_fixup(rest[0], sync, dry, budget)
    elif cmd == 'retry' and len(rest) == 1:
        cmd_retry(rest[0], dry, budget)
    elif cmd == 'status':
        cmd_status(rest[0] if rest else None, budget)
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main(sys.argv[1:])
