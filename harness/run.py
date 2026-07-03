#!/usr/bin/env python3
"""Your entrypoint. Usage:
  python3 harness/run.py --phase practice --defense solution/defense.py --out solution/practice_report.json
Decrypts the named phase's schedule in-memory, spawns your defense.py in an
isolated subprocess, streams events through it, scores, and writes a signed
result. See ../README.md and ../RULES.md before you start."""
import sys
import json
import shutil
import argparse
import tempfile
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(Path(__file__).parent))  # this dir only: crypto/scoring/signing/isolation/toolkit
import crypto
import scoring
import signing
from isolation import IsolatedRun

# Private is scoped tighter on fault difficulty than practice/public, so its
# budget is set separately: full single-pass coverage (one metered call per
# event) costs ~300 credits on private's 200-event stream — 220 would tax
# that at ~0.36 overage regardless of skill, compressing the gap between a
# real detector and doing nothing. 320 gives genuine full coverage a little
# headroom while still penalizing wasteful/redundant calls.
BUDGET_BY_PHASE = {"practice": 220.0, "public": 220.0, "private": 320.0}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", required=True, choices=["practice", "public", "private"])
    ap.add_argument("--defense", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    phases_dir = ROOT / "phases"
    key_path = phases_dir / f"{args.phase}.key"
    enc_path = phases_dir / f"{args.phase}_schedule.json.enc"
    if not key_path.exists():
        sys.exit(f"'{args.phase}.key' not released yet — check the phase schedule in README.md.")
    key = key_path.read_bytes()
    ciphertext = enc_path.read_bytes()
    schedule = crypto.decrypt_schedule(ciphertext, key)

    baseline_path = ROOT / "data" / "baselines.json"
    baseline = json.loads(baseline_path.read_text())

    events = schedule["events"]
    truths = schedule["ground_truth"]
    labels = schedule["labels"]
    gt_by_key = {(t["type"], t["batch_id_or_ref"]): t["gt"] for t in truths}

    # Both files are already decrypted into memory above — from here on they
    # only exist on disk as bait for a child process that shouldn't have any
    # way to reach them. Python-level patching of open()/FileIO/etc. turned
    # out to be an incomplete defense (module-level code can run before a
    # patch lands, and a stashed class reference or object.__subclasses__()
    # walk survives a later rebind-only "patch" regardless). The only thing
    # that actually closes it: the files genuinely aren't at any discoverable
    # path for the child's entire lifetime, so no technique — however
    # sophisticated — has anything to open. Move them to a randomized temp
    # dir named only in this process's memory, restore afterward either way.
    park_dir = Path(tempfile.mkdtemp(prefix="datasiege-parked-"))
    parked_key = park_dir / key_path.name
    parked_enc = park_dir / enc_path.name
    shutil.move(str(key_path), str(parked_key))
    shutil.move(str(enc_path), str(parked_enc))

    try:
        BUDGET = BUDGET_BY_PHASE[args.phase]
        run = IsolatedRun(args.defense, str(baseline_path), gt_by_key, budget=BUDGET)
        verdicts = []
        try:
            for ev in events:
                verdicts.append(run.dispatch(ev))
        finally:
            run.shutdown()
    finally:
        shutil.move(str(parked_key), str(key_path))
        shutil.move(str(parked_enc), str(enc_path))
        park_dir.rmdir()

    result = scoring.score_run(verdicts, labels, run.toolkit.cost_ledger, BUDGET)
    result["phase"] = args.phase
    result["defense_file"] = str(args.defense)

    if args.phase == "private":
        public_result = {"phase": "private", "score": result["score"],
                          "tpr": result["tpr"], "fpr": result["fpr"],
                          "cost_overage": result["cost_overage"]}
    else:
        public_result = dict(result)
        public_result["per_pillar_band"] = scoring.banded_diagnostics(result)
        del public_result["per_pillar"]
        del public_result["tp"], public_result["fp"], public_result["tn"], public_result["fn"]
        del public_result["tier_breakdown"]
        if args.phase == "practice":
            # practice_answer_key.json is already handed out in full, so this
            # exposes nothing new — but README.md advertises "diff your own
            # verdicts against it" as the practice workflow, and defense.py
            # can't write files, so without this there was nowhere to actually
            # get "your verdicts" from.
            public_result["verdicts"] = [
                {"seq": label["seq"], "alert": bool(v.get("alert"))}
                for v, label in zip(verdicts, labels)
            ]

    signature = signing.sign(public_result, key)
    signed = {"result": public_result, "signature": signature}

    Path(args.out).write_text(json.dumps(signed, indent=2))
    print(json.dumps(signed, indent=2))


if __name__ == "__main__":
    main()
