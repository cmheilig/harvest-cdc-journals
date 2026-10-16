"""Build reader packets <out>/<id>/{article.txt, prepass.json, tables/*.csv} from the extraction's frozen outputs.

A port of the extraction's stage6b/build_packets.py (reports/LOCAL_ASSESSMENT.md §1). It copies; it does not parse HTML:
  article.txt   4-line header (id, url, date, article_type) + the `text` of views/index_text/<year>.jsonl
  prepass.json  the article's line of stage6b/prepass.jsonl (python -m mmwrx.index_prepass build), indent=1
  tables/       views/tables/<year>/<id>_T*.csv; the directory is made even when empty
The extraction is read only. Its data are pinned by its MANIFEST_DATA.tsv (python -m mmwrx.manifest --check there).
Prints counts and ids only, never packet text. Holdout ids are counted, never printed or written.

    python3 tools/build_packets.py check                  # rebuild the pilot ids (sample.tsv) and compare with packets/
    python3 tools/build_packets.py ids LIST.tsv OUT       # packets for the `id` column of LIST.tsv
    python3 tools/build_packets.py vol VOL [--dry-run]    # production packets for one volume -> prod_packets/<VOL>/
        [--types t1,t2,...]                               # override SCOPE (required for volumes not in SCOPE)
    options: --extract DIR (default $MMWR_EXTRACT or ~/cdc-corpora/_test/mmwr-extract-v2)

`vol` writes prod_packets/<VOL>/frame.tsv: one row per frame article (corpus CSV, as tools/estimate_cost.py frame)
with status `packet`, `out_of_scope` or `not_in_views` (errata), and article_type. Holdout ids are left out of it.
"""
import collections, csv, filecmp, glob, json, os, re, shutil, sys, tempfile

EXTRACT = os.environ.get('MMWR_EXTRACT', os.path.expanduser('~/cdc-corpora/_test/mmwr-extract-v2'))
CORPUS = '../csv-output/cdc_corpus_df.csv'
OUT_PROD = 'prod_packets'
ID_RE = re.compile(r'^mm(\d{2})\d{2,4}[ae]\d+$')       # mm7301a1, mm695152a1 (double issue)
URL_ID = re.compile(r'/(mm\d{4,6}[a-z]\d+)\.htm')

# Frame decisions (reports/LOCAL_ASSESSMENT.md, L2, L3, L5). Holdout ids are always left out (L3).
REPORT_FAMILY = ('full_report', 'notes_from_the_field', 'vital_signs', 'outbreak_report', 'grand_rounds')
SCOPE = {v: REPORT_FAMILY + ('quickstats',) for v in (73, 72, 71)}
# vols 70-65: pending the evaluation after vols 73-71 (full reports only, or the report family)


def vol_of(i):
    m = ID_RE.match(i)
    return int(m.group(1)) if m else None


def load(want):
    """{id: index_text record} and {id: prepass record} for the wanted ids."""
    view, pre = {}, {}
    for f in sorted(glob.glob(f'{EXTRACT}/views/index_text/*.jsonl')):
        for line in open(f, encoding='utf-8'):
            d = json.loads(line)
            if want(d):
                view[d['id']] = d
    for line in open(f'{EXTRACT}/stage6b/prepass.jsonl', encoding='utf-8'):
        r = json.loads(line)
        if r['id'] in view:
            pre[r['id']] = r
    return view, pre


def write(out, view, pre, ids):
    """Write one packet per id. Returns the number of table CSVs copied."""
    n_t = 0
    for i in sorted(ids):
        d, p = view[i], f'{out}/{i}'
        os.makedirs(f'{p}/tables', exist_ok=True)
        hdr = f"id: {i}\nurl: {d['url']}\ndate: {d['date']}\narticle_type: {d['article_type']}\n\n"
        with open(f'{p}/article.txt', 'w', encoding='utf-8') as o:
            o.write(hdr + d['text'])
        with open(f'{p}/prepass.json', 'w', encoding='utf-8') as o:
            json.dump(pre[i], o, ensure_ascii=False, indent=1)
        for t in sorted(glob.glob(f"{EXTRACT}/views/tables/{d['date'][:4]}/{i}_T*.csv")):
            shutil.copy(t, f'{p}/tables/')
            n_t += 1
    return n_t


def build_ids(ids, out):
    ids = set(ids)
    view, pre = load(lambda d: d['id'] in ids)
    missing = sorted(ids - set(pre))
    if missing:
        sys.exit(f'{len(missing)} ids have no view or prepass: {" ".join(missing[:20])}')
    n_t = write(out, view, pre, ids)
    print(f'{len(ids)} packets, {n_t} table CSVs -> {out}')


def read_ids(path):
    return [r['id'] for r in csv.DictReader(open(path), delimiter='\t')]


def check(ref='packets', sample='sample.tsv'):
    """Rebuild the pilot ids into a temporary directory and compare every file with `ref` byte for byte."""
    with tempfile.TemporaryDirectory() as tmp:
        build_ids(read_ids(sample), tmp)
        files = lambda root: {os.path.relpath(os.path.join(a, f), root) for a, _, fs in os.walk(root)
                              for f in fs if f != '.DS_Store'}
        new, old = files(tmp), files(ref)
        same = [f for f in new & old if filecmp.cmp(f'{tmp}/{f}', f'{ref}/{f}', shallow=False)]
        diff = sorted(new & old - set(same))
        print(f'identical {len(same)}, differing {len(diff)}, only in rebuild {len(new - old)}, only in {ref} {len(old - new)}')
        for label, fs in (('differing', diff), ('only in rebuild', sorted(new - old)), (f'only in {ref}', sorted(old - new))):
            for f in fs[:20]:
                print(f'  {label}: {f}')
        sys.exit(0 if not diff and new == old else 1)


def holdout_set():
    """Holdout ids, as a set used only to exclude (the extraction's dev_ids(); DECISIONS 2026-10-04)."""
    sys.path.insert(0, EXTRACT)
    from mmwrx.holdout import dev_ids
    from mmwrx.corpus import read_ids as scope_ids
    return set(scope_ids('index_scope.txt')) - set(dev_ids())


def frame_ids(v):
    """Ids of English MMWR Weekly articles of volume v in the corpus CSV (as estimate_cost.py frame)."""
    out = []
    for r in csv.DictReader(open(CORPUS)):
        if r['series'] != 'mmwr' or r['level'] != 'article' or r['lang'] != 'en':
            continue
        m = re.match(r'\d+', r['dl_vol_iss'] or '')
        if m and int(m.group()) == v:
            u = URL_ID.search(r['url'])
            if not u:
                sys.exit(f'frame row without an article id in its url (vol {v})')
            out.append(u.group(1))
    return out


def build_vol(v, types=None, dry=False):
    types = types or SCOPE.get(v)
    if not types:
        sys.exit(f'vol {v}: frame not decided (SCOPE); pass --types to build anyway')
    frame = set(frame_ids(v))
    view, pre = load(lambda d: d['id'] in frame)
    held = holdout_set()
    rows, n = [], collections.Counter()
    for i in sorted(frame):
        if i not in view:
            st, t = 'not_in_views', ''
        else:
            t = view[i]['article_type']
            st = 'holdout' if i in held else 'packet' if t in types else 'out_of_scope'
        n[st] += 1
        if st != 'holdout':
            rows.append((i, st, t))
    ids = [i for i, st, _ in rows if st == 'packet']
    lost = [i for i in ids if i not in pre]
    if lost:
        sys.exit(f'{len(lost)} in-scope ids have no prepass: {" ".join(lost[:20])}')
    by_type = collections.Counter(view[i]['article_type'] for i in ids)
    print(f'vol {v}: frame {len(frame)}; ' + ', '.join(f'{k} {n[k]}' for k in ('packet', 'holdout', 'out_of_scope', 'not_in_views')))
    print('  packets by type: ' + ', '.join(f'{k} {c}' for k, c in by_type.most_common()))
    if dry:
        return
    out = f'{OUT_PROD}/{v}'
    n_t = write(out, view, pre, ids)
    with open(f'{out}/frame.tsv', 'w') as o:
        o.write('id\tstatus\tarticle_type\n')
        o.writelines(f'{i}\t{st}\t{t}\n' for i, st, t in rows)
    print(f'  {len(ids)} packets, {n_t} table CSVs -> {out}/ (and frame.tsv; holdout ids omitted)')


def main(argv):
    global EXTRACT
    args = list(argv)
    if '--extract' in args:
        k = args.index('--extract'); EXTRACT = os.path.expanduser(args[k + 1]); del args[k:k + 2]
    types = None
    if '--types' in args:
        k = args.index('--types'); types = tuple(args[k + 1].split(',')); del args[k:k + 2]
    dry = '--dry-run' in args
    args = [a for a in args if a != '--dry-run']
    if args[:1] == ['check']:
        check()
    elif args[:1] == ['ids'] and len(args) == 3:
        build_ids(read_ids(args[1]), args[2])
    elif args[:1] == ['vol'] and len(args) == 2:
        build_vol(int(args[1]), types, dry)
    else:
        sys.exit(__doc__)


if __name__ == '__main__':
    main(sys.argv[1:])
