"""
Spike S-2 bench: ≥3 concurrent DBOS workflows on a shared SQLite system database.

Scenario A — Concurrent monitor-style workflows:
  Spawn N=5 workflows that each run K=20 short steps writing per-step state
  while reading shared per-workflow counters. Measure per-step latency
  (p50, p99, max) and capture any SQLITE_BUSY / OperationalError surfaced.

Scenario B — send/recv ping-pong:
  Pair of workflows ping-ponging messages M times via DBOS.send / DBOS.recv,
  with multiple pairs running concurrently.

Outputs JSON results to stdout for inclusion in the findings doc.
"""
from __future__ import annotations

import json
import statistics
import sys
import tempfile
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from dbos import DBOS, DBOSConfig, Queue, SetWorkflowID

WORKDIR = Path(tempfile.mkdtemp(prefix="s2-bench-"))
DB_PATH = WORKDIR / "dbos.db"

CONFIG: DBOSConfig = {
    "name": "s2-bench",
    "system_database_url": f"sqlite:///{DB_PATH}",
    "run_admin_server": False,
}

# Shared collectors across threads -------------------------------------------------
_step_lat_ms: list[float] = []
_lat_lock = threading.Lock()
_errors: list[str] = []
_err_lock = threading.Lock()


def _record_lat(ms: float) -> None:
    with _lat_lock:
        _step_lat_ms.append(ms)


def _record_err(e: BaseException, context: str) -> None:
    with _err_lock:
        _errors.append(f"{context}: {type(e).__name__}: {e}")


# ---------------------------------------------------------------------------------
# Scenario A: concurrent step-heavy workflows
# ---------------------------------------------------------------------------------
DBOS(config=CONFIG)


@DBOS.step()
def do_unit(wf_id: str, idx: int) -> dict[str, Any]:
    """A single bit of work — write an entry into step state."""
    # Tiny payload; the latency we are measuring is the DBOS checkpoint cost,
    # not user-code cost.
    return {"wf": wf_id, "idx": idx, "ts": time.time()}


@DBOS.workflow()
def monitor_workflow(wf_id: str, steps: int) -> dict[str, Any]:
    """Synthetic monitor: K short DBOS-checkpointed steps."""
    results = []
    for i in range(steps):
        t0 = time.perf_counter()
        out = do_unit(wf_id, i)
        dt_ms = (time.perf_counter() - t0) * 1000.0
        _record_lat(dt_ms)
        results.append(out)
    return {"wf": wf_id, "n": len(results)}


# ---------------------------------------------------------------------------------
# Scenario B: send/recv ping-pong
# ---------------------------------------------------------------------------------
PING_TOPIC = "ping"
PONG_TOPIC = "pong"


@DBOS.workflow()
def pinger(peer_id: str, rounds: int) -> int:
    delivered = 0
    for r in range(rounds):
        DBOS.send(peer_id, {"r": r}, topic=PING_TOPIC)
        msg = DBOS.recv(PONG_TOPIC, timeout_seconds=10)
        if msg is None:
            _record_err(RuntimeError("pong timeout"), f"pinger r={r}")
            break
        delivered += 1
    return delivered


@DBOS.workflow()
def ponger(rounds: int) -> int:
    received = 0
    for r in range(rounds):
        msg = DBOS.recv(PING_TOPIC, timeout_seconds=10)
        if msg is None:
            _record_err(RuntimeError("ping timeout"), f"ponger r={r}")
            break
        # send response back to whoever invoked us via the pinger's workflow id
        # we keep peer id in the message envelope
        peer = msg.get("peer") if isinstance(msg, dict) else None
        if peer is None:
            # fall back: cannot reply
            _record_err(RuntimeError("missing peer"), f"ponger r={r}")
            break
        DBOS.send(peer, {"r": r}, topic=PONG_TOPIC)
        received += 1
    return received


# ---------------------------------------------------------------------------------
# Scenario B driver — pair workflows
# ---------------------------------------------------------------------------------
@DBOS.workflow()
def pinger_v2(peer_id: str, my_id: str, rounds: int) -> int:
    delivered = 0
    for r in range(rounds):
        DBOS.send(peer_id, {"r": r, "peer": my_id}, topic=PING_TOPIC)
        msg = DBOS.recv(PONG_TOPIC, timeout_seconds=10)
        if msg is None:
            _record_err(RuntimeError("pong timeout"), f"pinger r={r}")
            break
        delivered += 1
    return delivered


# ---------------------------------------------------------------------------------
def run_scenario_a(n_workflows: int, steps_per: int) -> dict[str, Any]:
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_workflows) as ex:
        futures = [
            ex.submit(monitor_workflow, f"mw-{i}", steps_per) for i in range(n_workflows)
        ]
        results = []
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except BaseException as e:  # capture lock errors etc
                _record_err(e, "scenario_a workflow")
    wall_ms = (time.perf_counter() - t0) * 1000.0

    lats = sorted(_step_lat_ms)
    n = len(lats)
    out: dict[str, Any] = {
        "n_workflows": n_workflows,
        "steps_per_workflow": steps_per,
        "completed_workflows": len(results),
        "wall_clock_ms": round(wall_ms, 2),
        "total_steps": n,
        "throughput_steps_per_sec": round(n / (wall_ms / 1000.0), 2) if wall_ms else None,
    }
    if lats:
        out["latency_ms"] = {
            "p50": round(lats[n // 2], 3),
            "p95": round(lats[int(n * 0.95)], 3),
            "p99": round(lats[int(n * 0.99)] if n >= 100 else lats[-1], 3),
            "max": round(lats[-1], 3),
            "min": round(lats[0], 3),
            "mean": round(statistics.fmean(lats), 3),
        }
    return out


def run_scenario_b(n_pairs: int, rounds: int) -> dict[str, Any]:
    """Concurrent send/recv ping-pong pairs."""
    t0 = time.perf_counter()
    results: list[dict[str, Any]] = []

    def run_pair(pair_idx: int) -> dict[str, Any]:
        ponger_id = f"ponger-{pair_idx}"
        pinger_id = f"pinger-{pair_idx}"
        # Start ponger first with deterministic workflow id
        with SetWorkflowID(ponger_id):
            ponger_handle = DBOS.start_workflow(ponger, rounds)
        with SetWorkflowID(pinger_id):
            pinger_handle = DBOS.start_workflow(pinger_v2, ponger_id, pinger_id, rounds)
        try:
            sent = pinger_handle.get_result()
            recv = ponger_handle.get_result()
        except BaseException as e:
            _record_err(e, f"scenario_b pair {pair_idx}")
            return {"pair": pair_idx, "error": str(e)}
        return {"pair": pair_idx, "sent": sent, "recv": recv}

    with ThreadPoolExecutor(max_workers=n_pairs) as ex:
        futures = [ex.submit(run_pair, i) for i in range(n_pairs)]
        for fut in as_completed(futures):
            results.append(fut.result())

    wall_ms = (time.perf_counter() - t0) * 1000.0
    total_msgs = sum((r.get("sent") or 0) + (r.get("recv") or 0) for r in results)
    return {
        "n_pairs": n_pairs,
        "rounds_per_pair": rounds,
        "wall_clock_ms": round(wall_ms, 2),
        "total_messages": total_msgs,
        "msgs_per_sec": round(total_msgs / (wall_ms / 1000.0), 2) if wall_ms else None,
        "pair_results": results,
    }


def main() -> int:
    DBOS.launch()
    summary: dict[str, Any] = {"db_path": str(DB_PATH)}
    try:
        # Scenario A: 5 concurrent workflows, 20 steps each (>= 3 required)
        print("Running Scenario A: concurrent step-heavy workflows ...", file=sys.stderr)
        summary["scenario_a"] = run_scenario_a(n_workflows=5, steps_per=20)
        # Reset latency collector for clarity
        _step_lat_ms.clear()

        # Scenario B: 3 concurrent send/recv pairs, 10 rounds
        print("Running Scenario B: send/recv ping-pong ...", file=sys.stderr)
        summary["scenario_b"] = run_scenario_b(n_pairs=3, rounds=10)

        summary["errors"] = list(_errors)
        summary["db_size_bytes"] = DB_PATH.stat().st_size if DB_PATH.exists() else 0
    except BaseException as e:
        summary["fatal_error"] = f"{type(e).__name__}: {e}"
        summary["traceback"] = traceback.format_exc()
    finally:
        DBOS.destroy()

    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
