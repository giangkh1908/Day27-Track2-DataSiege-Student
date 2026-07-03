"""
CHILD-PROCESS-SIDE types. This is the only interface defense.py ever sees.
ToolkitProxy holds no ground truth, no cost ledger, no live handles — every
method call is serialized over the pipe to the parent (siege/isolation.py),
which owns the real ServerToolkit (toolkit/metering.py).
"""
import json
import sys
from dataclasses import dataclass, field


@dataclass
class Verdict:
    alert: bool
    confidence: float = 1.0
    reason: str = ""
    pillar: str = ""


class ToolkitProxy:
    def __init__(self, send, recv):
        self._send = send
        self._recv = recv
        self._next_id = 0

    def _call(self, method, **args):
        self._next_id += 1
        cid = self._next_id
        self._send({"type": "tool_call", "id": cid, "method": method, "args": args})
        resp = self._recv()
        assert resp["type"] == "tool_result" and resp["id"] == cid, f"protocol desync: {resp}"
        return resp["result"]

    def batch_profile(self, batch_id):
        return self._call("batch_profile", batch_id=batch_id)

    def contract_diff(self, contract_id, checkpoint_batch_id):
        return self._call("contract_diff", contract_id=contract_id, checkpoint_batch_id=checkpoint_batch_id)

    def lineage_graph_slice(self, run_id):
        return self._call("lineage_graph_slice", run_id=run_id)

    def feature_drift(self, feature_view, ref):
        return self._call("feature_drift", feature_view=feature_view, ref=ref)

    def embedding_drift(self, corpus, ref):
        return self._call("embedding_drift", corpus=corpus, ref=ref)

    def spend_so_far(self):
        return self._call("spend_so_far")

    def budget_remaining(self):
        return self._call("budget_remaining")


class SiegeContext:
    def __init__(self, tools: ToolkitProxy, baseline: dict):
        self._handlers = {}
        self.tools = tools
        self.baseline = baseline
        self.state = {}

    def on(self, event_type, handler):
        self._handlers[event_type] = handler

    def dispatch(self, event):
        h = self._handlers.get(event["type"])
        if h is None:
            return Verdict(alert=False, reason="no handler registered")
        return h(event["payload"], self)
