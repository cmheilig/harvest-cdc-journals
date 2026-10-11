# Adjudicator brief (cloud pilot)

Two readers (labelled A and B; you are not told which model or role each is) wrote index records for an MMWR article, following `READER_BRIEF.md` (version 3). Your job is to decide, field by field where they disagree, which value is best supported **by the article**. **Justification over agreement:** judge the evidence each reader cites, not the majority.

## Per article, read
- `READER_BRIEF.md` (once, in full: the rules readers had to follow);
- `adj_packet/<id>/disagreements.json` (normalized values per label);
- `reader_A.json`, `reader_B.json` (their evidence);
- the article: `packets/<id>/article.txt` and `tables/*.csv` — check the article whenever readers' evidence conflicts.

## Decide
For each disagreement: the verdict value (in the reader-schema's terms), which labels got it right (several may, if the field is genuinely ambiguous), and a category:
- `reader error` — the article clearly supports one value;
- `ambiguity` — two values are defensible under the brief (say which);
- `omission` — a reader missed something present (e.g. a test in a table note);
- `brief gap` — the brief does not decide the case (say what rule would);
- `view issue` — the view or CSV is wrong or incomplete.

Also:
- `justification`: for each label, `good` · `adequate` · `weak` (quotes verbatim and on point? values supported?);
- `agreed_errors`: if both readers agree on a value you can see is wrong (check the high-value fields: procedures and regression families, study design, equity level), list it.

## Output
Write `adjudication/<id>.json`:
```json
{"id": "...",
 "fields": [{"field": "...", "verdict": "...", "correct": ["A"], "category": "reader error", "why": "≤2 sentences"}],
 "justification": {"A": "good", "B": "adequate"},
 "agreed_errors": [{"field": "...", "verdict": "...", "why": "..."}],
 "brief_issues": "short, or null"}
```
Use field names exactly as in `disagreements.json`. Keep `why` short. Do not open `adj_mapping.json`, `out/` or `reports/`.
