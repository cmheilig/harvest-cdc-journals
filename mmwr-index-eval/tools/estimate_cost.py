"""Production cost estimate for API reads (PRODUCTION_PLAN.md). Sizes only: no packet or record text is printed.

    python3 tools/estimate_cost.py frame            # MMWR article counts by volume from ../csv-output/cdc_corpus_df.csv
    python3 tools/estimate_cost.py cost [VOL ...]   # per-article and total cost for those volumes (default 73..65)

Token counts are estimated from characters (CHARS_PER_TOKEN); replace them with `messages.count_tokens` figures from a
pilot batch before committing a budget. Thinking tokens at effort medium are a guess until measured.
"""
import collections, csv, glob, json, os, re, statistics, sys

CHARS_PER_TOKEN = 3.3      # English prose and JSON on the Opus 4.7+ tokenizer; calibrate with count_tokens
PRICE_IN, PRICE_OUT = 4.00, 20.00        # $ per 1M tokens, claude-opus-5-5, standard
CACHE_READ, CACHE_WRITE_1H = 0.20, 8.00  # $ per 1M; 1-hour write assumed at 2x input
BATCH = 0.5                              # Message Batches discount on every token class
THINKING = 6000            # assumed thinking tokens per read at effort medium
FIXUP = 0.15               # assumed share of reads needing one validator fix-up turn
CORPUS = '../csv-output/cdc_corpus_df.csv'


def tokens(chars):
    return chars / CHARS_PER_TOKEN


def frame():
    """{volume: article count} for English MMWR Weekly articles."""
    n = collections.Counter()
    for r in csv.DictReader(open(CORPUS)):
        if r['series'] != 'mmwr' or r['level'] != 'article' or r['lang'] != 'en':
            continue
        m = re.match(r'\d+', r['dl_vol_iss'] or '')
        if m:
            n[int(m.group())] += 1
    return n


def sizes():
    fixed = os.path.getsize('READER_BRIEF.md') + sum(os.path.getsize(f) for f in ('examples/mm6925a1.json', 'examples/mm7223a3.json'))
    packets = [sum(os.path.getsize(f) for f in glob.glob(f'{d}/**/*', recursive=True) if os.path.isfile(f))
               for d in glob.glob('packets/*')]
    records = [os.path.getsize(f) for f in glob.glob('out/v5/*.json')]
    return fixed, packets, records


def cost(vols):
    fixed, packets, records = sizes()
    t_fixed, t_pkt, t_rec = tokens(fixed), tokens(statistics.mean(packets)), tokens(statistics.mean(records))
    t_out = t_rec + THINKING
    # first request writes the cache; the rest read it. A fix-up turn resends the packet and record as input.
    per = (t_fixed * CACHE_READ + t_pkt * PRICE_IN + t_out * PRICE_OUT
           + FIXUP * ((t_fixed * CACHE_READ) + (t_pkt + t_out) * PRICE_IN + t_rec * PRICE_OUT)) / 1e6
    uncached = ((t_fixed + t_pkt) * PRICE_IN + t_out * PRICE_OUT) / 1e6
    n = frame(); total = sum(n[v] for v in vols)
    print(f'fixed prefix ~{t_fixed:,.0f} tok; packet mean ~{t_pkt:,.0f} (max ~{tokens(max(packets)):,.0f}); '
          f'record mean ~{t_rec:,.0f}; thinking assumed {THINKING:,}')
    print(f'per article: ${per:.3f} cached, ${uncached:.3f} uncached; batch halves both -> ${per * BATCH:.3f}')
    print(f'volumes {min(vols)}-{max(vols)}: {total:,} articles -> ${total * per:,.0f} standard+cache, '
          f'${total * per * BATCH:,.0f} batch+cache (+ cache writes ~${t_fixed * CACHE_WRITE_1H / 1e6 * BATCH:.2f} each)')
    for v in sorted(vols, reverse=True):
        print(f'  vol {v}: {n[v]:4d} articles  ~${n[v] * per * BATCH:,.0f} batch')


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'cost'
    if cmd == 'frame':
        n = frame()
        for v in sorted(n):
            print(v, n[v])
        print('total', sum(n.values()))
    else:
        cost([int(v) for v in sys.argv[2:]] or list(range(65, 74)))
