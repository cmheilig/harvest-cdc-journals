"""Arm B: merge the judgment reader's review into the structural record.

    python tools/merge_review.py            # out/haiku/<id>.json + out/opus_review/<id>.json -> out/merged/<id>.json

The merged record is the structural record with each `changes[].field` replaced, the review's complete `equity`
object, reader = the judgment reader (role "judgment", with `based_on` = the structural reader), and validates.
A review with `agree_data_focus_none: true` yields the structural record unchanged, marked as reviewed.
"""
import glob, json, os, sys
sys.path.insert(0, 'tools')
from validate import check

os.makedirs('out/merged', exist_ok=True)
bad = 0
for p in sorted(glob.glob('out/opus_review/*.json')):
    rv = json.load(open(p)); i = rv['id']
    base = json.load(open(f'out/haiku/{i}.json'))
    m = json.loads(json.dumps(base))
    m['reader'] = {**rv['reader'], 'role': 'judgment', 'based_on': base['reader'].get('model')}
    if not rv.get('agree_data_focus_none'):
        for c in rv.get('changes', []):
            m[c['field']] = c['value']
        m['equity'] = rv['equity']
        m['confidence'] = rv.get('confidence', m['confidence'])
        if rv.get('notes'):
            m['notes'] = ((m.get('notes') or '') + ' [review] ' + rv['notes']).strip()[:800]
    errs = check(m)
    if errs:
        bad += 1; print('FAIL', i, errs[:5])
    json.dump(m, open(f'out/merged/{i}.json', 'w'), ensure_ascii=False, indent=1)
print('merged; failures:', bad)
