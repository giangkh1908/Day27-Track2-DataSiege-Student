# Toolkit API reference

Everything you call through `ctx.tools` inside a handler is a metered RPC to the
harness — the return value is real, computed from the actual pipeline state,
but every call costs credits (deducted from your run's budget). The source is
at `harness/toolkit/metering.py` — reading it is fair game and won't tell you
anything about which events are faulty, only how each check is computed.

| Method | Cost | Returns |
|---|---|---|
| `batch_profile(batch_id)` | 1.0 | `{row_count, null_rate:{customer_id}, mean_amount, std_amount, staleness_min}` |
| `contract_diff(contract_id, checkpoint_batch_id)` | 1.5 | `{freshness_delay_min, violations: [...]}` |
| `lineage_graph_slice(run_id)` | 1.0 | `{duration_ms, actual_upstream, actual_downstream_count}` |
| `feature_drift(feature_view, ref)` | 2.0 | `{serve_mean, train_mean, train_std, mean_shift_sigma}` |
| `embedding_drift(corpus, ref)` | 2.0 | `{centroid_shift, avg_doc_age_days}` |
| `spend_so_far()` | free | running total cost |
| `budget_remaining()` | free | `budget - spend_so_far()`; going negative is a scoring penalty, never a crash |

A call for an event id that hasn't been dispatched to you yet, or an unknown
one, returns `{"error": "..."}` — check for that key, don't assume the shape.

Only these methods exist. Calling anything else raises no exception but
returns `{"error": "'<name>' is not a callable tool"}` — see RULES.md.

## `ctx.baseline` — published, calibrated constants

```
row_count_min / row_count_max, null_rate_max, mean_amount_min / mean_amount_max,
staleness_min_max, freshness_delay_max_min, lineage_duration_ms_max,
feature_mean_shift_sigma_max, embedding_centroid_shift_max, corpus_avg_doc_age_days_max
```
Each was derived from real clean-stream measurements at mean ± 3σ. They reliably
catch large deviations. Some faults are deliberately closer to normal variance —
a single static threshold won't reliably catch every instance of those; combining
signals or being more statistically careful will do better than eyeballing the
bare numbers.

## `ctx.state`

A plain dict, yours to use, that persists for your whole run (one process, one
run). Free to read/write — no RPC cost.

## Debugging

`defense.py` has no file I/O and stdout is the RPC channel to the harness —
`print()` there will desync the run. **`sys.stderr` is yours for debug
output** (e.g. `print(payload, file=sys.stderr)`); it's never scored or
inspected, purely for your own local troubleshooting.
