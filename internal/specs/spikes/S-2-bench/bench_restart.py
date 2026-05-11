"""
Spike S-2 bench: workflow restart while N workflows are mid-step.

Strategy:
  1. Launch DBOS instance 1, start N workflows that each emit K steps with
     a small sleep between steps so we can interrupt mid-flight.
  2. Tear DBOS down (simulating an abrupt restart) while workflows are still
     running. Use os._exit-style abrupt destroy via DBOS.destroy with force.
  3. Re-launch DBOS instance 2 against the SAME dbos.db. Verify the
     framework recovers pending workflows and they progress to completion
     without duplicate side effects.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from dbos import DBOS, DBOSConfig, SetWorkflowID

PHASE = os.environ.get("S2_PHASE", "")
DB_DIR = Path(os.environ.get("S2_DBDIR", ""))
N_WORKFLOWS = 4
STEPS_PER = 8
SLEEP_PER_STEP = 0.25  # seconds; gives a ~2s total runtime per workflow

# Side-effect file: each step appends its (wf_id, idx) so we can detect dupes
SIDE_EFFECT = DB_DIR / "side_effects.log"


def append_side_effect(wf_id: str, idx: int) -> None:
    SIDE_EFFECT.parent.mkdir(parents=True, exist_ok=True)
    with open(SIDE_EFFECT, "a") as f:
        f.write(f"{wf_id}\t{idx}\n")


def make_dbos() -> DBOS:
    config: DBOSConfig = {
        "name": "s2-restart",
        "system_database_url": f"sqlite:///{DB_DIR}/dbos.db",
        "run_admin_server": False,
    }
    return DBOS(config=config)


make_dbos()


@DBOS.step()
def slow_step(wf_id: str, idx: int) -> dict[str, Any]:
    append_side_effect(wf_id, idx)
    time.sleep(SLEEP_PER_STEP)
    return {"wf": wf_id, "idx": idx, "ts": time.time()}


@DBOS.workflow()
def long_monitor(wf_id: str) -> int:
    completed = 0
    for i in range(STEPS_PER):
        slow_step(wf_id, i)
        completed += 1
    return completed


def phase_start() -> None:
    """Launch DBOS, start workflows, exit abruptly after a short delay."""
    DBOS.launch()
    handles = []
    for i in range(N_WORKFLOWS):
        wf_id = f"long-{i}"
        with SetWorkflowID(wf_id):
            h = DBOS.start_workflow(long_monitor, wf_id)
        handles.append((wf_id, h))
    print(json.dumps({"phase": "start", "started": [wf_id for wf_id, _ in handles]}))
    # Wait long enough for several steps but not all steps
    time.sleep(SLEEP_PER_STEP * 3 + 0.1)
    # Abrupt exit — simulate crash. Do NOT call DBOS.destroy gracefully.
    os._exit(0)


def phase_recover() -> None:
    """Re-launch DBOS against the same db; observe recovery."""
    t0 = time.perf_counter()
    DBOS.launch()
    # Allow DBOS auto-recovery to pick up pending workflows and complete them.
    # Poll handles by workflow ID using DBOS.retrieve_workflow.
    completed: dict[str, Any] = {}
    deadline = t0 + 60.0
    pending = [f"long-{i}" for i in range(N_WORKFLOWS)]
    while pending and time.perf_counter() < deadline:
        still_pending = []
        for wf_id in pending:
            try:
                h = DBOS.retrieve_workflow(wf_id)
                # Try to get status without blocking long
                status = h.get_status()
                if status.status in ("SUCCESS", "ERROR", "CANCELLED"):
                    try:
                        result = h.get_result()
                    except Exception as e:  # noqa: BLE001
                        result = f"ERR:{e}"
                    completed[wf_id] = {"status": status.status, "result": result}
                else:
                    still_pending.append(wf_id)
            except Exception as e:  # noqa: BLE001
                still_pending.append(wf_id)
                completed[wf_id + "-poll-err"] = str(e)
        pending = still_pending
        if pending:
            time.sleep(0.5)

    wall = time.perf_counter() - t0
    # Read side-effects log and look for duplicates
    dupes: dict[str, int] = {}
    rows = 0
    if SIDE_EFFECT.exists():
        with open(SIDE_EFFECT) as f:
            seen: set[tuple[str, str]] = set()
            for line in f:
                rows += 1
                wf_id, idx = line.rstrip("\n").split("\t", 1)
                k = (wf_id, idx)
                if k in seen:
                    dupes[f"{wf_id}/{idx}"] = dupes.get(f"{wf_id}/{idx}", 1) + 1
                else:
                    seen.add(k)

    DBOS.destroy()
    print(json.dumps({
        "phase": "recover",
        "wall_sec": round(wall, 3),
        "completed": completed,
        "pending_after_timeout": pending,
        "side_effect_rows": rows,
        "duplicate_side_effects": dupes,
    }, indent=2, default=str))


if __name__ == "__main__":
    if PHASE == "start":
        phase_start()
    elif PHASE == "recover":
        phase_recover()
    else:
        print("set S2_PHASE=start|recover", file=sys.stderr)
        sys.exit(2)
