"""
PARENT-PROCESS-ONLY. Real Toolkit implementation + cost ledger (spec §5 FIX A).
This module, the ground-truth dict, and the cost ledger never cross into the
child process that runs defense.py — only serialized dicts do, via siege/isolation.py.
"""

COSTS = {
    "batch_profile": 1.0,
    "contract_diff": 1.5,
    "lineage_graph_slice": 1.0,
    "feature_drift": 2.0,
    "embedding_drift": 2.0,
    "spend_so_far": 0.0,
    "budget_remaining": 0.0,
}


class ServerToolkit:
    """Lives only in the parent process. isolation.py calls these methods on
    behalf of RPC requests from the (untrusted) child, and only ever ships the
    RETURN VALUE across the pipe — never the ground_truth dict itself."""

    def __init__(self, ground_truth_by_key, budget):
        # ground_truth_by_key: {(type, ref): gt_dict}, populated incrementally
        # as events are revealed (see reveal()) — prevents look-ahead.
        self._gt = {}
        self._all_gt = ground_truth_by_key
        self.cost_ledger = 0.0
        self.budget = budget
        self.call_log = []

    def reveal(self, etype, ref):
        """Called by the harness right before dispatching an event — makes that
        event's ground truth visible to subsequent tool calls. Events not yet
        dispatched are simply absent from self._gt, so a lookup for them fails
        closed (returns a 'not yet available' sentinel) rather than leaking."""
        key = (etype, ref)
        if key in self._all_gt:
            self._gt[key] = self._all_gt[key]

    def _charge(self, method):
        self.cost_ledger += COSTS[method]
        self.call_log.append(method)

    def batch_profile(self, batch_id):
        self._charge("batch_profile")
        gt = self._gt.get(("data_batch", batch_id))
        if gt is None:
            return {"error": "unknown or not-yet-visible batch_id"}
        return {"row_count": gt["row_count"], "null_rate": {"customer_id": gt["null_rate_customer_id"]},
                "mean_amount": gt["mean_amount"], "std_amount": gt["std_amount"], "staleness_min": gt["staleness_min"]}

    def contract_diff(self, contract_id, checkpoint_batch_id):
        self._charge("contract_diff")
        gt = self._gt.get(("contract_checkpoint", checkpoint_batch_id))
        if gt is None:
            return {"error": "unknown or not-yet-visible checkpoint"}
        violations = []
        if gt["actual_schema_hash"] != "sha256:5f2a":
            violations.append("schema_hash_mismatch")
        if gt["actual_amount_type"] != "float":
            violations.append("type_violation")
        return {"freshness_delay_min": gt["freshness_delay_min"], "violations": violations}

    def lineage_graph_slice(self, run_id):
        self._charge("lineage_graph_slice")
        gt = self._gt.get(("lineage_run", run_id))
        if gt is None:
            return {"error": "unknown or not-yet-visible run_id"}
        return {"duration_ms": gt["lineage_duration_ms"], "actual_upstream": gt["actual_upstream"],
                "actual_downstream_count": gt["actual_downstream_count"]}

    def feature_drift(self, feature_view, ref):
        self._charge("feature_drift")
        gt = self._gt.get(("feature_materialization", ref))
        if gt is None:
            return {"error": "unknown or not-yet-visible feature batch"}
        mean_shift_sigma = abs(gt["feature_serve_mean"] - gt["train_mean"]) / gt["train_std"]
        return {"serve_mean": gt["feature_serve_mean"], "train_mean": gt["train_mean"],
                "train_std": gt["train_std"], "mean_shift_sigma": round(mean_shift_sigma, 3)}

    def embedding_drift(self, corpus, ref):
        self._charge("embedding_drift")
        gt = self._gt.get(("embedding_batch", ref))
        if gt is None:
            return {"error": "unknown or not-yet-visible embedding batch"}
        return {"centroid_shift": gt["embedding_centroid_shift"], "avg_doc_age_days": gt["corpus_avg_doc_age_days"]}

    def spend_so_far(self):
        return self.cost_ledger

    def budget_remaining(self):
        return self.budget - self.cost_ledger
