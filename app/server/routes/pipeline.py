"""Run Pipeline - Notebook execution routes."""
import time
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.config import get_workspace_client

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])

NOTEBOOK_PATHS = {
    "bronze": "/Workspace/Users/ravichandan.cv@databricks.com/tmlpv/lakebase_to_bronze",
    "silver": "/Workspace/Users/ravichandan.cv@databricks.com/tmlpv/bronze_to_silver",
    "gold": "/Workspace/Users/ravichandan.cv@databricks.com/tmlpv/silver_to_gold",
}

# Cluster to use for one-time runs (smallest available)
CLUSTER_SPEC = {
    "spark_version": "15.4.x-scala2.12",
    "num_workers": 0,
    "node_type_id": "Standard_D4ads_v5",
    "driver_node_type_id": "Standard_D4ads_v5",
}

# Track active runs
_active_runs: dict[str, dict] = {}


class RunRequest(BaseModel):
    stage: str  # "bronze", "silver", "gold", or "all"


class RunStatusResponse(BaseModel):
    stage: str
    run_id: Optional[int] = None
    status: str
    message: str = ""


def _submit_notebook_run(stage: str) -> dict:
    """Submit a one-time notebook run via Jobs API."""
    if stage not in NOTEBOOK_PATHS:
        raise ValueError(f"Unknown stage: {stage}")

    w = get_workspace_client()

    # Use submit_run for one-time execution
    from databricks.sdk.service.jobs import SubmitTask, NotebookTask, GitSource

    run = w.jobs.submit(
        run_name=f"tmlpv-{stage}-{int(time.time())}",
        tasks=[
            SubmitTask(
                task_key=f"{stage}_task",
                new_cluster=CLUSTER_SPEC,
                notebook_task=NotebookTask(
                    notebook_path=NOTEBOOK_PATHS[stage],
                ),
            )
        ],
    )

    return {"run_id": run.run_id, "stage": stage, "status": "SUBMITTED"}


@router.post("/run")
async def run_pipeline(request: RunRequest):
    """Trigger a pipeline stage or all stages."""
    try:
        if request.stage == "all":
            results = []
            for stage in ["bronze", "silver", "gold"]:
                result = _submit_notebook_run(stage)
                _active_runs[stage] = result
                results.append(
                    RunStatusResponse(
                        stage=stage,
                        run_id=result["run_id"],
                        status="SUBMITTED",
                        message=f"Notebook {NOTEBOOK_PATHS[stage]} submitted",
                    )
                )
            return results
        else:
            result = _submit_notebook_run(request.stage)
            _active_runs[request.stage] = result
            return RunStatusResponse(
                stage=request.stage,
                run_id=result["run_id"],
                status="SUBMITTED",
                message=f"Notebook {NOTEBOOK_PATHS[request.stage]} submitted",
            )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{run_id}")
async def get_run_status(run_id: int):
    """Get the status of a run."""
    try:
        w = get_workspace_client()
        run = w.jobs.get_run(run_id=run_id)
        state = run.state
        return {
            "run_id": run_id,
            "status": state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN",
            "result": state.result_state.value if state.result_state else None,
            "message": state.state_message or "",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/active-runs")
async def get_active_runs():
    """Get all tracked active runs."""
    results = []
    w = get_workspace_client()
    for stage, run_info in _active_runs.items():
        try:
            run = w.jobs.get_run(run_id=run_info["run_id"])
            state = run.state
            results.append({
                "stage": stage,
                "run_id": run_info["run_id"],
                "status": state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN",
                "result": state.result_state.value if state.result_state else None,
            })
        except Exception:
            results.append({
                "stage": stage,
                "run_id": run_info.get("run_id"),
                "status": "ERROR",
                "result": None,
            })
    return results
