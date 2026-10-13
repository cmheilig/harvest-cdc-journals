"""MMWR index record (schema v2.2, reader brief v4) as a JSON Schema, and a validator.

    python tools/validate.py FILE.json [...]     # validate records (one JSON object per file, or JSONL)
    python tools/validate.py --schema            # print the JSON Schema

Short keys keep output lean (output tokens cost 5x input): every value is {"v": ..., "ev": [{"loc": ..., "q": ...}]},
where loc is title | summary | text | table N | figure N | note | ocr, and q is a verbatim quote of at most 25 words.
"""
import json, sys

EV = {"type": "array", "maxItems": 3, "items": {"type": "object", "additionalProperties": False, "required": ["loc", "q"],
      "properties": {"loc": {"type": "string", "pattern": "^(title|summary|text|table [0-9]+|figure [0-9]+|note|ocr)$"},
                     "q": {"type": "string", "maxLength": 240}}}}

def val(enum=None, kind="string", nullable=False):
    v = {"enum": enum} if enum else {"type": kind}
    if nullable:
        v = {"anyOf": [v, {"type": "null"}]}
    return {"type": "object", "additionalProperties": False, "required": ["v", "ev"], "properties": {"v": v, "ev": EV}}

def lst(enum=None, extra=None):
    item = {"type": "object", "additionalProperties": False, "required": ["v", "ev"],
            "properties": {"v": {"enum": enum} if enum else {"type": "string"}, "ev": EV, **(extra or {})}}
    return {"type": "array", "items": item}

OUTCOME = ["continuous", "binary/categorical", "count/rate", "event-time"]
DEPTH = ["summary", "comparison", "stratified/adjusted"]
DESIGN = ["cross-sectional", "retrospective cohort", "prospective cohort", "case-control", "test-negative", "screening method",
          "before/after or trend", "ecologic", "outbreak investigation", "surveillance/descriptive", "diagnostic accuracy",
          "guidance or recommendations", "case report/series", "cost/economic", "non-randomized trial",
          "evaluation or monitoring", "mixed/other"]
SOURCE = ["CDC surveillance system", "state or local surveillance", "national survey", "other survey", "registry", "electronic health records",
          "administrative/claims", "vaccination registry", "sentinel/network", "international surveillance", "record abstraction or interview", "other"]
CONDITION = ["COVID", "influenza", "mpox", "RSV", "HIV", "TB/respiratory", "STI", "vaccine-preventable (other)", "diabetes",
             "cardiovascular", "cancer", "obesity/nutrition", "maternal/reproductive", "mental health", "substance use", "tobacco",
             "injury/poisoning", "environmental", "health behaviors", "other acute", "other chronic"]
DIMENSION = ["race/ethnicity", "SES", "rurality", "age", "sex", "sexual orientation and gender identity", "disability",
             "country of birth/language/nativity", "other"]
ROLE = ["population", "covariate", "descriptor", "stratifier", "primary comparison", "cross-classifier"]
ASSESSMENT = ["side by side", "omnibus test", "trend test", "model joint test", "interaction", "per-group vs referent only"]
UNCERTAIN = ["procedures", "regression_fields", "inference", "comparison", "analytic_depth", "outcome_types", "study_design",
             "groupwise", "equity.level", "equity.dimensions"]

SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "MMWR index record (schema v2.2, brief v4), one reader, one article",
    "type": "object", "additionalProperties": False,
    "required": ["id", "reader", "data_focus", "outcome_types", "analytic_depth", "procedures", "comparison", "inference",
                 "primary_measures", "regression_fields", "public_health_question", "condition_group", "study_design",
                 "data_source_type", "data_source_named", "setting", "vaccine_outcome_type", "equity", "confidence", "notes",
                 "groupwise", "uncertain_fields"],
    "properties": {
        "id": {"type": "string", "pattern": "^mm[0-9a-z]+$"},
        "reader": {"type": "object", "required": ["model"], "properties": {"model": {"type": "string"}, "reader_id": {"type": "string"}, "role": {"enum": ["structural", "judgment", "example"]}}},
        "data_focus": val(["yes", "limited", "none"]),
        "outcome_types": lst(OUTCOME),
        "analytic_depth": lst(DEPTH),
        "procedures": {"type": "array", "items": {"type": "object", "additionalProperties": False,
            "required": ["name", "class", "regression_family", "role", "ev"],
            "properties": {"name": {"type": "string"},
                           "class": {"enum": ["test", "regression", "standardization", "weighting", "transformation", "other"]},
                           "regression_family": {"enum": ["linear", "logistic", "loglinear", "event-time", "other", None]},
                           "inferred_from": {"type": "string"}, "role": {"enum": ["primary", "ancillary"]},
                           "alternative_reading": {"type": "string"}, "ev": EV}}},
        "comparison": {"type": "object", "additionalProperties": False, "required": ["framework", "scale", "referent", "across", "ev"],
            "properties": {"framework": {"enum": ["none", "fixed referent", "pair", "group"]},
                           "scale": {"enum": ["absolute", "relative", "both", None]},
                           "referent": {"type": ["string", "null"]}, "across": {"type": "array", "items": {"type": "string"}}, "ev": EV}},
        "inference": {"type": "object", "additionalProperties": False, "required": ["v", "method_stated", "ev"],
            "properties": {"v": {"enum": ["none", "CIs", "tests", "CIs and tests"]}, "method_stated": {"type": "boolean"},
                           "ci_overlap": {"type": "boolean"}, "ev": EV}},
        "groupwise": {"type": "array", "items": {"type": "object", "additionalProperties": False,
            "required": ["across", "n_groups", "ordered", "assessment", "resolves_to_finer", "ev"],
            "properties": {"across": {"type": "array", "minItems": 1, "items": {"enum": DIMENSION}},
                           "n_groups": {"anyOf": [{"type": "integer", "minimum": 3}, {"type": "null"}]},
                           "ordered": {"type": "boolean"}, "assessment": {"enum": ASSESSMENT},
                           "resolves_to_finer": {"type": "boolean"}, "ev": EV}}},
        "uncertain_fields": {"type": "array", "uniqueItems": True, "items": {"enum": UNCERTAIN}},
        "primary_measures": lst(),
        "regression_fields": {"anyOf": [{"type": "null"}, {"type": "object", "additionalProperties": False,
            "required": ["nonindependence_approach", "unadjusted_results", "adjusted_results", "confounders",
                         "interaction_modeled", "sensitivity_analysis"],
            "properties": {
                "nonindependence_approach": val(["GEE", "mixed effects", "clustered or robust SE", "conditional (matched)",
                                                 "complex survey design", "serial correlation", "none"], nullable=True),
                "unadjusted_results": val(kind="boolean", nullable=True), "adjusted_results": val(kind="boolean", nullable=True),
                "confounders": val(nullable=True), "interaction_modeled": val(kind="boolean", nullable=True),
                "sensitivity_analysis": val(kind="boolean", nullable=True)}}]},
        "public_health_question": val(),
        "condition_group": lst(CONDITION),
        "study_design": lst(DESIGN),
        "data_source_type": lst(SOURCE),
        "data_source_named": lst(),
        "setting": val(["US national", "US subnational", "international", "mixed"], nullable=True),
        "vaccine_outcome_type": {"anyOf": [{"type": "null"}, val(["effectiveness", "coverage/uptake", "safety", "other"])]},
        "equity": {"type": "object", "additionalProperties": False,
            "required": ["level", "level_reason", "dimensions", "determinants_of_health", "equity_action", "equity_measurement"],
            "properties": {
                "level": {"enum": [0, 1, 2, 3, 4]}, "level_reason": {"type": "string", "maxLength": 400},
                "level_confidence": {"enum": ["high", "medium", "low"]},
                "level_alternative": {"anyOf": [{"type": "null"}, {"type": "object", "additionalProperties": False, "required": ["level", "reason"],
                                      "properties": {"level": {"enum": [0, 1, 2, 3, 4]}, "reason": {"type": "string", "maxLength": 300}}}]},
                "dimensions": lst(DIMENSION, {"role": {"enum": ROLE},
                                              "ses_components": {"type": "array", "maxItems": 3, "items": {"type": "string"}}}),
                "determinants_of_health": val(nullable=True),
                "equity_action": val(kind="boolean"),
                "equity_measurement": {"type": ["string", "null"], "maxLength": 400}}},
        "confidence": {"enum": ["high", "medium", "low"]},
        "notes": {"type": ["string", "null"], "maxLength": 800},
        "view_issues": {"type": ["string", "null"], "maxLength": 400},
    },
}


def check(rec):
    """JSON Schema errors plus the rules the schema cannot express."""
    import jsonschema
    errs = [f"{'/'.join(map(str, e.path))}: {e.message}" for e in jsonschema.Draft202012Validator(SCHEMA).iter_errors(rec)]
    if errs:
        return errs
    quotes = [e['q'] for v in _values(rec) for e in v.get('ev', [])]
    errs += [f"quote over 25 words: {q[:60]}…" for q in quotes if len(q.split()) > 25]
    has_reg = any(p['class'] == 'regression' for p in rec['procedures'])
    if has_reg and rec['regression_fields'] is None:
        errs.append('regression in procedures but regression_fields is null')
    if not has_reg and rec['regression_fields'] is not None:
        errs.append('regression_fields given but no regression procedure')
    for p in rec['procedures']:
        if p['class'] == 'regression' and p['regression_family'] is None:
            errs.append(f"regression without family: {p['name']}")
    for d in rec['equity']['dimensions']:
        if d.get('ses_components') and d['v'] != 'SES':
            errs.append('ses_components on a non-SES dimension')
    if rec['data_focus']['v'] == 'none' and (rec['procedures'] or rec['groupwise']):
        errs.append('data_focus none but procedures or groupwise given')
    for g in rec['groupwise']:
        if not g['ev']:
            errs.append('groupwise entry without evidence')
    if rec['reader'].get('role') == 'judgment' and 'level_confidence' not in rec['equity']:
        errs.append('judgment reader must give equity.level_confidence')
    return errs


def _values(o):
    if isinstance(o, dict):
        if 'ev' in o:
            yield o
        for v in o.values():
            yield from _values(v)
    elif isinstance(o, list):
        for v in o:
            yield from _values(v)


def load(path):
    txt = open(path).read().strip()
    return [json.loads(l) for l in txt.splitlines() if l.strip()] if path.endswith('.jsonl') else [json.loads(txt)]


if __name__ == '__main__':
    if sys.argv[1:] == ['--schema']:
        print(json.dumps(SCHEMA, indent=1))
        sys.exit()
    bad = 0
    for p in sys.argv[1:]:
        for r in load(p):
            e = check(r)
            bad += bool(e)
            print(('OK  ' if not e else 'FAIL') + f" {p} {r.get('id')}" + ('' if not e else '\n    ' + '\n    '.join(e[:12])))
    sys.exit(1 if bad else 0)
