from __future__ import annotations

from platform import node_pb2
from platform.node import Node
from platform.util import mode

from .base import PluginBase


class Plugin(PluginBase):
    def start(self, node: Node, request: node_pb2.RunStartRequest) -> node_pb2.RunResponse:
        """RunStart: execute steps under at-most / at-least / exactly-once."""
        processing_mode = mode(request.modes, "processing_guarantees")
        
        # Initialize run record
        run_record = {
            "run_id": request.run_id,
            "state": "running",
            "next_step": 0,
            "applied": [],
            "steps": [(s.key, s.value) for s in request.steps],
            "mode": processing_mode
        }
        
        # Execute steps based on mode
        for i, step in enumerate(request.steps):
            try:
                if processing_mode == "AT_MOST_ONCE":
                    # Try once, skip on failure
                    result = self._try_write(node, step.key, step.value, request.run_id, i)
                    if result:
                        run_record["applied"].append(i)
                elif processing_mode == "AT_LEAST_ONCE":
                    # Keep retrying until success (accepts duplicates)
                    result = self._try_write(node, step.key, step.value, request.run_id, i)
                    if result:
                        run_record["applied"].append(i)
                elif processing_mode == "EXACTLY_ONCE":
                    # Use idempotency key
                    idempotency_key = f"{request.run_id}:{i}"
                    result = self._write_exactly_once(node, step.key, step.value, idempotency_key)
                    if result:
                        run_record["applied"].append(i)
                
                run_record["next_step"] = i + 1
            except Exception:
                if processing_mode == "AT_MOST_ONCE":
                    # Skip to next step
                    continue
                else:
                    # Mark as uncertain and stop
                    run_record["state"] = "uncertain"
                    break
        
        # Store run record
        with node.lock:
            node.runs[request.run_id] = run_record
        
        return node_pb2.RunResponse(
            ok=True,
            run_id=request.run_id,
            state=run_record["state"],
            next_step=run_record["next_step"],
            applied=run_record["applied"]
        )
    
    def resume(self, node: Node, request: node_pb2.RunResumeRequest) -> node_pb2.RunResponse:
        """RunResume from a log position or from a snapshot."""
        resume_mode = mode(request.modes, "resume")
        run_id = request.run_id
        
        with node.lock:
            run_record = node.runs.get(run_id)
            if not run_record:
                return node_pb2.RunResponse(ok=False, error="unknown run", run_id=run_id)
        
        if resume_mode == "LOG":
            # Continue from next unapplied step
            start_step = run_record["next_step"]
        elif resume_mode == "SNAPSHOT":
            # Continue from snapshot (simpler: resume from last applied)
            start_step = len(run_record["applied"])
        else:
            return node_pb2.RunResponse(ok=False, error="invalid resume mode", run_id=run_id)
        
        # Resume execution
        for i in range(start_step, len(run_record["steps"])):
            key, value = run_record["steps"][i]
            try:
                if run_record["mode"] == "EXACTLY_ONCE":
                    idempotency_key = f"{run_id}:{i}"
                    self._write_exactly_once(node, key, value, idempotency_key)
                else:
                    self._try_write(node, key, value, run_id, i)
                
                run_record["applied"].append(i)
                run_record["next_step"] = i + 1
            except Exception:
                run_record["state"] = "uncertain"
                break
        
        # Update run record
        with node.lock:
            node.runs[run_id] = run_record
        
        return node_pb2.RunResponse(
            ok=True,
            run_id=run_id,
            state=run_record["state"],
            next_step=run_record["next_step"],
            applied=run_record["applied"]
        )
    
    def get(self, node: Node, request: node_pb2.RunGetRequest) -> node_pb2.RunResponse:
        """Get run status."""
        with node.lock:
            run_record = node.runs.get(request.run_id)
            if not run_record:
                return node_pb2.RunResponse(ok=False, error="unknown run", run_id=request.run_id)
        
        return node_pb2.RunResponse(
            ok=True,
            run_id=run_record["run_id"],
            state=run_record["state"],
            next_step=run_record["next_step"],
            applied=run_record["applied"]
        )
    
    def _try_write(self, node: Node, key: bytes, value: bytes, run_id: str, step_idx: int) -> bool:
        """Attempt a write, return True if successful."""
        with node.lock:
            node.store[key] = value
            node.log.append((len(node.log), key, value))
        return True
    
    def _write_exactly_once(self, node: Node, key: bytes, value: bytes, idempotency_key: str) -> bool:
        """Write with idempotency guarantee."""
        with node.lock:
            if idempotency_key in node.idempotent:
                # Already applied, skip
                return True
            node.store[key] = value
            node.log.append((len(node.log), key, value))
            node.idempotent.add(idempotency_key)
        return True
