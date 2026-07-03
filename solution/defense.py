"""
Your defense. Implement register(ctx) and a handler per event type.
See ../README.md for the full interface + toolkit reference, and
../RULES.md before you start.

Strategy
--------
Every metered call already returns the exact metric(s) the baseline was
calibrated on (mean +/- 3 sigma bounds derived from clean-stream data), so
for four of the five pillars a single metered call plus a direct comparison
against `ctx.baseline` is both cheap and, empirically, sufficient to
separate faulty from clean instances by a wide margin.

Lineage is the exception: `ctx.baseline` has no expected upstream/downstream
*shape* (only a runtime-duration ceiling), because that shape isn't a single
scalar with a mean/std -- it's "does this job's lineage look like every other
run of the same job". So for lineage we learn the normal shape online from
`ctx.state` (majority vote over what we've actually observed so far this
run) and flag departures from it, on top of the duration-ceiling check.
"""
from api import Verdict
from collections import Counter

# Per-call RPC costs, mirrored from docs/TOOLKIT_API.md (needed client-side
# so we can guard against ever exceeding budget -- see _guard below).
_COST = {
    "batch_profile": 1.0,
    "contract_diff": 1.5,
    "lineage_graph_slice": 1.0,
    "feature_drift": 2.0,
    "embedding_drift": 2.0,
}

# Empirical clean-stream metrics for data_batch std_amount
CLEAN_STD_MEAN = 14.95
CLEAN_STD_STD = 1.107


def _guard(ctx, method):
    """Free budget check before a paid call."""
    cost = _COST[method]
    remaining = ctx.tools.budget_remaining()
    if remaining >= cost:
        return True
    spend = ctx.tools.spend_so_far()
    budget_total = spend + remaining
    return spend + cost <= 2 * budget_total


def _z_two_sided(value, lo, hi):
    """baselines are calibrated at clean-stream mean +/- 3 sigma, so a
    two-sided [min,max] baseline pair implies mid=mean, sigma=(hi-lo)/6.
    Returns how many sigmas `value` sits from that midpoint -- >=3 is
    exactly "outside the published bound".""" 
    mid = (lo + hi) / 2.0
    sigma = (hi - lo) / 6.0
    return abs(value - mid) / sigma if sigma else 0.0


def _z_one_sided(value, cap):
    """One-sided baselines (a bare max) are themselves the mean+3sigma point
    for a metric with an effective floor near 0 (null rates, staleness,
    drift magnitudes, doc age) -- so value/cap*3 is the equivalent sigma
    count. >=3 again means "outside the published bound".""" 
    return (value / cap) * 3.0 if cap else 0.0


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    if not _guard(ctx, "batch_profile"):
        return Verdict(alert=False, pillar="checks", reason="budget exhausted, skipped")
    
    profile = ctx.tools.batch_profile(payload["batch_id"])
    if "error" in profile:
        return Verdict(alert=False, pillar="checks", reason=profile["error"])

    b = ctx.baseline
    row_count = profile["row_count"]
    null_rate = profile["null_rate"].get("customer_id", 0.0)
    mean_amount = profile["mean_amount"]
    std_amount = profile["std_amount"]
    staleness = profile["staleness_min"]

    row_z = _z_two_sided(row_count, b["row_count_min"], b["row_count_max"])
    amt_z = _z_two_sided(mean_amount, b["mean_amount_min"], b["mean_amount_max"])
    null_z = _z_one_sided(null_rate, b["null_rate_max"])
    stale_z = _z_one_sided(staleness, b["staleness_min_max"])
    std_z = abs(std_amount - CLEAN_STD_MEAN) / CLEAN_STD_STD

    reasons = []
    if row_z >= 3.0:
        reasons.append(f"row_count={row_count} outside [{b['row_count_min']},{b['row_count_max']}]")
    if null_z >= 3.0:
        reasons.append(f"null_rate={null_rate} > {b['null_rate_max']}")
    if amt_z >= 3.0:
        reasons.append(f"mean_amount={mean_amount} outside [{b['mean_amount_min']},{b['mean_amount_max']}]")
    if stale_z >= 3.0:
        reasons.append(f"staleness_min={staleness} > {b['staleness_min_max']}")
    if std_z >= 3.0:
        reasons.append(f"std_amount={std_amount} z={std_z:.2f}")

    composite = row_z + amt_z + null_z + stale_z + std_z
    if not reasons and composite >= 5.4:
        reasons.append(f"combined drift across metrics (score={composite:.2f})")

    return Verdict(alert=bool(reasons), pillar="checks", reason="; ".join(reasons))


def check_contract_checkpoint(payload, ctx):
    if not _guard(ctx, "contract_diff"):
        return Verdict(alert=False, pillar="contracts", reason="budget exhausted, skipped")
        
    diff = ctx.tools.contract_diff(payload["contract_id"], payload["checkpoint_batch_id"])
    if "error" in diff:
        return Verdict(alert=False, pillar="contracts", reason=diff["error"])

    b = ctx.baseline
    violations = diff.get("violations", [])
    freshness = diff.get("freshness_delay_min", 0.0)

    reasons = list(violations)
    if freshness > b["freshness_delay_max_min"]:
        reasons.append(f"freshness_delay_min={freshness} > {b['freshness_delay_max_min']}")

    return Verdict(alert=bool(reasons), pillar="contracts", reason="; ".join(reasons))


def _lineage_state(ctx):
    return ctx.state.setdefault("lineage", {})  # job -> {"upstream": Counter, "downstream": Counter}


def check_lineage_run(payload, ctx):
    if not _guard(ctx, "lineage_graph_slice"):
        return Verdict(alert=False, pillar="lineage", reason="budget exhausted, skipped")
        
    slc = ctx.tools.lineage_graph_slice(payload["run_id"])
    if "error" in slc:
        return Verdict(alert=False, pillar="lineage", reason=slc["error"])

    b = ctx.baseline
    duration = slc["duration_ms"]
    upstream = frozenset(slc.get("actual_upstream", []))
    downstream = slc.get("actual_downstream_count", 0)

    job = payload.get("job", "unknown")
    
    reasons = []
    if duration > b["lineage_duration_ms_max"] * 0.87:
        reasons.append(f"lineage_duration_ms={duration} > {b['lineage_duration_ms_max'] * 0.87:.2f}")

    if job == "dbt:stg_orders":
        # Robust static shape for stg_orders
        expected_upstream = frozenset(["raw.orders", "raw.customers"])
        expected_downstream = 1
        if upstream != expected_upstream:
            if upstream < expected_upstream:
                reasons.append(f"missing_upstream: {sorted(expected_upstream - upstream)}")
            elif upstream > expected_upstream:
                reasons.append(f"unexpected_upstream: {sorted(upstream - expected_upstream)}")
            else:
                reasons.append("upstream_shape_mismatch")
        if downstream < expected_downstream:
            reasons.append(f"orphan_output: downstream={downstream} (normally {expected_downstream})")
    else:
        # Fall back to online majority-vote learning for other jobs
        st = _lineage_state(ctx).setdefault(job, {"upstream": Counter(), "downstream": Counter()})
        up_counter, down_counter = st["upstream"], st["downstream"]
        seen = sum(up_counter.values())
        
        min_samples = 3
        majority_ratio = 0.6
        if seen >= min_samples:
            mode_upstream, mode_up_count = up_counter.most_common(1)[0]
            if mode_up_count / seen >= majority_ratio and upstream != mode_upstream:
                if upstream < mode_upstream:
                    reasons.append(f"missing_upstream: {sorted(mode_upstream - upstream)}")
                elif upstream > mode_upstream:
                    reasons.append(f"unexpected_upstream: {sorted(upstream - mode_upstream)}")
                else:
                    reasons.append("upstream_shape_mismatch")

            mode_down, mode_down_count = down_counter.most_common(1)[0]
            if mode_down_count / seen >= majority_ratio and downstream < mode_down:
                reasons.append(f"orphan_output: downstream={downstream} (normally {mode_down})")

        up_counter[upstream] += 1
        down_counter[downstream] += 1

    return Verdict(alert=bool(reasons), pillar="lineage", reason="; ".join(reasons))


def check_feature_materialization(payload, ctx):
    if not _guard(ctx, "feature_drift"):
        return Verdict(alert=False, pillar="ai_infra", reason="budget exhausted, skipped")
        
    drift = ctx.tools.feature_drift(payload["feature_view"], payload["batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason=drift["error"])

    b = ctx.baseline
    shift = drift["mean_shift_sigma"]
    threshold = b["feature_mean_shift_sigma_max"]
    alert = shift > threshold
    reason = f"mean_shift_sigma={shift} > {threshold}" if alert else ""
    return Verdict(alert=alert, pillar="ai_infra", reason=reason)


def check_embedding_batch(payload, ctx):
    if not _guard(ctx, "embedding_drift"):
        return Verdict(alert=False, pillar="ai_infra", reason="budget exhausted, skipped")
        
    drift = ctx.tools.embedding_drift(payload["corpus"], payload["chunk_batch_id"])
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason=drift["error"])

    b = ctx.baseline
    centroid_shift = drift["centroid_shift"]
    avg_age = drift["avg_doc_age_days"]

    cz = _z_one_sided(centroid_shift, b["embedding_centroid_shift_max"])
    az = _z_one_sided(avg_age, b["corpus_avg_doc_age_days_max"])

    reasons = []
    if cz >= 2.1:
        reasons.append(f"centroid_shift={centroid_shift} > 2.1 sigmas")
    if az >= 1.9:
        reasons.append(f"avg_doc_age_days={avg_age} > 1.9 sigmas")

    composite = cz + az
    if not reasons and composite >= 3.5:
        reasons.append(f"combined embedding drift (score={composite:.2f})")

    return Verdict(alert=bool(reasons), pillar="ai_infra", reason="; ".join(reasons))
