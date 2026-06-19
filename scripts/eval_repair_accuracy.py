#!/usr/bin/env python3
"""
Reproducible reliability eval for the ETL repair agent.

Runs a labeled set of malformed user-signup events through the pipeline's real
decision logic -- the Great Expectations validator, the agent's repair proposer
(src.agent.propose_repair), and the autopilot reviewer
(src.approval_api.evaluate_repair) -- and scores:

  1. Repair-strategy selection  : did the agent pick an action that fits the defect?
  2. Reviewer decision accuracy  : did approve/reject match ground truth?
  3. End-to-end outcome accuracy  : did the pipeline KEEP good data and DROP bad?

Only the Postgres persistence hop between propose and review is skipped; it has
no effect on the model's decisions, so the scores reflect the shipped system.

Ground truth is deterministic:
  - string that converts cleanly to the target type  -> REPAIR (coerce, approve)
  - string that cannot (e.g. "twenty-five" for age)   -> DROP   (reject)
  - missing optional field (age: no not-null rule)    -> REPAIR (add null)
  - missing identity field (user_id)                  -> DROP   (cannot fabricate)

Usage:
  python3 scripts/eval_repair_accuracy.py            # one pass over the set
  python3 scripts/eval_repair_accuracy.py --runs 3   # average over 3 passes

Requires ANTHROPIC_API_KEY (in .env). No Kafka or Postgres needed.
"""
import argparse
import json  # noqa: F401  (kept for parity with the modules under test)
import logging
import os
import sys
import warnings
from collections import Counter

warnings.filterwarnings("ignore")
logging.disable(logging.WARNING)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.validator import validate_user_signup  # noqa: E402
from src.agent import propose_repair  # noqa: E402
from src.approval_api import evaluate_repair  # noqa: E402

TS = "2024-01-01T00:00:00Z"
CATEGORIES = ["coercible_age", "uncoercible_age", "missing_optional_age", "missing_identity_userid"]
SAFE_TO_NULL = {"age"}  # optional in the model AND has no not-null rule


def base(**over):
    e = {"user_id": "u-001", "email": "x@example.com", "name": "Sam", "age": 28, "signup_timestamp": TS}
    e.update(over)
    return e


def build_dataset():
    data = []
    for a in ["25", "30", "42", "18", "56", "8", "100", "21"]:
        data.append(("coercible_age", base(age=a), "REPAIR"))
    for a in ["twenty-five", "unknown", "N/A", "old", "thirtyish", "??", "adult", "none"]:
        data.append(("uncoercible_age", base(age=a), "DROP"))
    for i in range(4):
        e = base(user_id=f"u-1{i:02d}")
        e.pop("age")
        data.append(("missing_optional_age", e, "REPAIR"))
    for i in range(4):
        e = base()
        e.pop("user_id")
        e["email"] = f"d{i}@example.com"
        data.append(("missing_identity_userid", e, "DROP"))
    return data


def is_coercible(value, target_type):
    try:
        {"int": int, "float": float, "str": str}.get(target_type, str)(value)
        return True
    except Exception:
        return False


def ground_truth_decision(event, p):
    """Objective: should THIS proposed repair be approved?"""
    action = p["repair_action"]
    if action == "reject_event":
        return "approved"  # rejecting un-repairable data is the correct call
    if action == "coerce_to_type":
        return "approved" if is_coercible(event.get(p["field_name"]), p.get("target_type")) else "rejected"
    if action == "add_nullable_field":
        return "approved" if p["field_name"] in SAFE_TO_NULL else "rejected"
    return "rejected"


def system_outcome(p, decision):
    """What actually happens to the event."""
    if decision == "rejected":
        return "DROP"
    return "DROP" if p["repair_action"] == "reject_event" else "REPAIR"


def strategy_ok(category, action):
    """Did the agent pick an action appropriate to the defect class?"""
    if category in ("coercible_age", "uncoercible_age"):
        return action in ("coerce_to_type", "reject_event")
    return action in ("add_nullable_field", "reject_event")  # missing-field defects


def run_once(dataset, show_table):
    rows = []
    if show_table:
        hdr = f"{'#':>2}  {'category':<24}{'proposed':<19}{'tgt':<5}{'judge':<10}{'outcome':<8}{'exp':<8}ok"
        print(hdr)
        print("-" * len(hdr))
    for i, (cat, event, expected) in enumerate(dataset, 1):
        passed, violations = validate_user_signup(event)
        if passed:
            continue
        p = propose_repair(event, violations)
        decision, _ = evaluate_repair(
            p["field_name"], p["repair_action"], p.get("target_type"), p["reasoning"], event
        )
        gt = ground_truth_decision(event, p)
        outcome = system_outcome(p, decision)
        ok = outcome == expected
        rows.append((cat, p["repair_action"], decision, gt, outcome, expected, ok))
        if show_table:
            print(
                f"{i:>2}  {cat:<24}{p['repair_action']:<19}{(p.get('target_type') or '-'):<5}"
                f"{decision:<10}{outcome:<8}{expected:<8}{'OK' if ok else 'XX'}"
            )
    return rows


def summarize(rows):
    n = len(rows)
    e2e = sum(r[6] for r in rows)
    judge = sum(r[2] == r[3] for r in rows)
    strat = sum(strategy_ok(r[0], r[1]) for r in rows)
    return n, e2e, judge, strat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=1, help="number of passes over the set (averaged)")
    args = ap.parse_args()

    dataset = build_dataset()
    print(f"ETL repair-agent reliability eval -- {len(dataset)} labeled events x {args.runs} run(s)\n")

    e2e_rates, judge_rates, strat_rates = [], [], []
    for run in range(args.runs):
        rows = run_once(dataset, show_table=(run == 0))
        n, e2e, judge, strat = summarize(rows)
        e2e_rates.append(e2e / n)
        judge_rates.append(judge / n)
        strat_rates.append(strat / n)
        if run == 0:
            print()
            for c in CATEGORIES:
                sub = [r for r in rows if r[0] == c]
                dist = dict(Counter(r[1] for r in sub))
                print(f"  {c:<26} {sum(r[6] for r in sub)}/{len(sub)} correct   actions={dist}")
            print()
        print(
            f"run {run + 1}: end-to-end {e2e}/{n} ({e2e / n * 100:.0f}%) | "
            f"reviewer {judge}/{n} ({judge / n * 100:.0f}%) | strategy {strat}/{n} ({strat / n * 100:.0f}%)"
        )

    def pct(xs):
        return sum(xs) / len(xs) * 100

    print("\n" + "=" * 62)
    print(f"Repair-strategy selection : {pct(strat_rates):.0f}%")
    print(f"Reviewer decision accuracy : {pct(judge_rates):.0f}%")
    spread = f"  (runs: {[f'{r * 100:.0f}%' for r in e2e_rates]})" if args.runs > 1 else ""
    print(f"End-to-end outcome accuracy : {pct(e2e_rates):.0f}%{spread}")


if __name__ == "__main__":
    main()
