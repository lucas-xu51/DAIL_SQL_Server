#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Parse the detailed error report and compare items with execution-empty to the mismatched id list.
Outputs:
  dataset/spider/empty_vs_mismatch.json
  dataset/spider/empty_vs_mismatch.txt
"""
import re
import json
from pathlib import Path
try:
    from mismatched_ids import MISMATCHED_IDS
except Exception:
    # fallback: try several possible paths
    possible = [Path('test_validation/mismatched_ids.py'), Path('test_validation/empty_test/mismatched_ids.py'), Path('test_validation/mismatched_ids.py')]
    MISMATCHED_IDS = []
    for mismatched_path in possible:
        if mismatched_path.exists():
            txt = mismatched_path.read_text(encoding='utf-8')
            MISMATCHED_IDS = [int(x) for x in re.findall(r"\b(\d{1,5})\b", txt)]
            break

REPORT = Path('dataset/spider/error_dev_all_detailed_report.txt')
OUT_JSON = Path('dataset/spider/empty_vs_mismatch.json')
OUT_TXT = Path('dataset/spider/empty_vs_mismatch.txt')

if not REPORT.exists():
    print('Detailed report not found:', REPORT)
    raise SystemExit(1)

content = REPORT.read_text(encoding='utf-8')

# Split entries by separator lines (----...)
entries = re.split(r"\n-+\n", content)

empty_qids = []
pattern_qid = re.compile(r"问题索引:\s*(\d+)")
for e in entries:
    # find qid
    m = pattern_qid.search(e)
    if not m:
        continue
    qid = int(m.group(1))
    # look for execution empty markers
    if 'Execution returned empty result' in e or 'returned no results' in e or 'EXECUTION: Query executed but returned no results' in e:
        empty_qids.append(qid)

empty_qids = sorted(set(empty_qids))

mismatch_set = set(MISMATCHED_IDS)
empty_in_mismatch = sorted([q for q in empty_qids if q in mismatch_set])
empty_not_in_mismatch = sorted([q for q in empty_qids if q not in mismatch_set])

out = {
    'total_empty': len(empty_qids),
    'empty_qids': empty_qids,
    'empty_in_mismatch': empty_in_mismatch,
    'empty_not_in_mismatch': empty_not_in_mismatch,
}

OUT_JSON.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding='utf-8')

with OUT_TXT.open('w', encoding='utf-8') as f:
    f.write('Empty vs Mismatch analysis\n')
    f.write(f"Total empty results: {len(empty_qids)}\n")
    f.write('\nEmpty QIDs:\n')
    f.write(','.join(map(str, empty_qids)) + '\n')
    f.write('\nEmpty that are in your mismatched list (true positives):\n')
    f.write(','.join(map(str, empty_in_mismatch)) + '\n')
    f.write('\nEmpty that are NOT in your mismatched list (false positives):\n')
    f.write(','.join(map(str, empty_not_in_mismatch)) + '\n')

print('Wrote', OUT_JSON, OUT_TXT)
