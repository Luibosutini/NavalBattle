from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporalio.common import RetryPolicy
    from temporalio.exceptions import ActivityError, ApplicationError
    from .activities import (
        artifact_write_activity,
        bedrock_invoke_activity,
        budget_check_activity,
        budget_guard_activity,
        context_append_patch_activity,
        context_build_latest_activity,
        index_update_activity,
        write_comms_log_activity,
        write_event_log_activity,
    )


STATE_ENQUEUED = "ENQUEUED"
STATE_RUNNING = "RUNNING"
STATE_NEED_INPUT = "NEED_INPUT"
STATE_NEED_APPROVAL = "NEED_APPROVAL"
STATE_DONE = "DONE"
STATE_FAILED = "FAILED"
STATE_BUDGET_DENIED = "BUDGET_DENIED"
STATE_CANCELLED = "CANCELLED"


@workflow.defn
class MissionWorkflow:
    def __init__(self) -> None:
        self.mission_id: str = ""
        self.task_id: str = ""
        self.ship: str = "CVL"
        self.trace_id: str = ""
        self.user_state: str = STATE_ENQUEUED
        self.needs_reason: str = ""
        self.next_action: str = ""
        self.last_error: str = ""
        self.last_error_code: str = ""
        self.hitl_mode: str = ""
        self.hitl_timeout_action: str = STATE_FAILED
        self.context_ref: str = ""
        self.context_snapshot: dict[str, Any] = {}
        self.budget_spent_usd: float = 0.0

        self.approval_decision: bool | None = None
        self.approval_note: str = ""
        self.human_input: str = ""

        self.seen_signal_ids: set[str] = set()
        self.signal_log: list[dict[str, Any]] = []
        self.base_payload: dict[str, Any] = {}

    def _meta(self) -> dict[str, Any]:
        return {
            "mission_id": self.mission_id,
            "task_id": self.task_id,
            "ship": self.ship,
            "trace_id": self.trace_id,
        }

    async def _exec_activity(self, fn: Any, payload: dict[str, Any]) -> Any:
        return await workflow.execute_activity(
            fn,
            payload,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                backoff_coefficient=2.0,
            ),
        )

    async def _exec_bedrock_activity(self, payload: dict[str, Any]) -> Any:
        return await workflow.execute_activity(
            bedrock_invoke_activity,
            payload,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=5,
                initial_interval=timedelta(seconds=2),
                maximum_interval=timedelta(seconds=30),
                backoff_coefficient=2.0,
            ),
        )

    async def _write_event(self, event_type: str, detail: dict[str, Any] | None = None) -> None:
        await self._exec_activity(
            write_event_log_activity,
            {**self.base_payload, **self._meta(), "event_type": event_type, "detail": detail or {}},
        )

    async def _write_comms(self, comm_type: str, content: str, to_role: str = "user") -> None:
        await self._exec_activity(
            write_comms_log_activity,
            {
                **self.base_payload,
                **self._meta(),
                "from_role": "system",
                "to_role": to_role,
                "comm_type": comm_type,
                "content": content,
            },
        )

    async def _update_index(self) -> None:
        await self._exec_activity(
            index_update_activity,
            {
                **self.base_payload,
                **self._meta(),
                "user_state": self.user_state,
                "needs_reason": self.needs_reason,
                "next_action": self.next_action,
            },
        )

    async def _context_update(self, patch: dict[str, Any]) -> None:
        await self._exec_activity(
            context_append_patch_activity,
            {
                **self.base_payload,
                **self._meta(),
                "context_ref": self.context_ref,
                "context_patch": patch,
            },
        )
        latest = await self._exec_activity(
            context_build_latest_activity,
            {
                **self.base_payload,
                **self._meta(),
            },
        )
        if str(latest.get("status", "")) == "OK":
            self.context_ref = str(latest.get("context_ref", "")).strip()
            context_data = latest.get("context", {})
            if isinstance(context_data, dict):
                self.context_snapshot = context_data

    async def _budget_guard(self, *, phase: str, call_cost_usd: float = 0.0) -> dict[str, Any]:
        result = await self._exec_activity(
            budget_guard_activity,
            {
                **self.base_payload,
                **self._meta(),
                "phase": phase,
                "budget_limit_usd": self.base_payload.get("budget_limit_usd"),
                "spent_usd": self.budget_spent_usd,
                "call_cost_usd": call_cost_usd,
            },
        )
        try:
            self.budget_spent_usd = float(result.get("total_spent_usd", self.budget_spent_usd))
        except Exception:
            pass
        return result

    def _append_signal_log(self, signal_type: str, signal_id: str, payload: dict[str, Any]) -> None:
        self.signal_log.append({"type": signal_type, "signal_id": signal_id, "payload": payload})
        self.signal_log = self.signal_log[-20:]

    @workflow.signal
    def approve(self, payload: dict[str, Any]) -> None:
        signal_id = str(payload.get("signal_id", "")).strip()
        if signal_id:
            if signal_id in self.seen_signal_ids:
                return
            self.seen_signal_ids.add(signal_id)
        self._append_signal_log("approve", signal_id, payload)

        if self.user_state != STATE_NEED_APPROVAL:
            return
        decision_raw = payload.get("decision")
        if isinstance(decision_raw, bool):
            decision = decision_raw
        else:
            decision = str(decision_raw).strip().lower() in {"yes", "y", "true", "1", "approve", "approved"}
        self.approval_decision = decision
        self.approval_note = str(payload.get("note", "")).strip()

    @workflow.signal
    def provide_input(self, payload: dict[str, Any]) -> None:
        signal_id = str(payload.get("signal_id", "")).strip()
        if signal_id:
            if signal_id in self.seen_signal_ids:
                return
            self.seen_signal_ids.add(signal_id)
        self._append_signal_log("input", signal_id, payload)

        if self.user_state != STATE_NEED_INPUT:
            return
        message = str(payload.get("message", "")).strip()
        if message:
            self.human_input = message

    @workflow.query
    def get_status(self) -> dict[str, Any]:
        return {
            "mission_id": self.mission_id,
            "task_id": self.task_id,
            "ship": self.ship,
            "trace_id": self.trace_id,
            "user_state": self.user_state,
            "needs_reason": self.needs_reason,
            "next_action": self.next_action,
            "hitl_mode": self.hitl_mode,
            "hitl_timeout_action": self.hitl_timeout_action,
            "last_error": self.last_error,
            "last_error_code": self.last_error_code,
            "recent_signals": list(self.signal_log),
        }

    async def _wait_for_approval(self, timeout_hours: int) -> bool:
        self.user_state = STATE_NEED_APPROVAL
        self.needs_reason = "Awaiting human approval signal"
        self.next_action = f"naval approve --mission {self.mission_id} --yes"
        await self._update_index()
        await self._write_event("STATUS_NEED_APPROVAL", {"reason": self.needs_reason})
        await self._write_comms("need_approval", self.needs_reason)
        try:
            await workflow.wait_condition(
                lambda: self.approval_decision is not None,
                timeout=timedelta(hours=timeout_hours),
            )
        except asyncio.TimeoutError:
            return False
        return True

    async def _wait_for_input(self, timeout_hours: int) -> bool:
        self.user_state = STATE_NEED_INPUT
        self.needs_reason = "Awaiting human input signal"
        self.next_action = f"naval input --mission {self.mission_id} \"<message>\""
        await self._update_index()
        await self._write_event("STATUS_NEED_INPUT", {"reason": self.needs_reason})
        await self._write_comms("need_input", self.needs_reason)
        try:
            await workflow.wait_condition(
                lambda: bool(self.human_input),
                timeout=timedelta(hours=timeout_hours),
            )
        except asyncio.TimeoutError:
            return False
        return True

    def _classify_bedrock_activity_error(self, exc: Exception) -> tuple[str, str]:
        reason = "unknown"
        code = "UNKNOWN"
        app_err: ApplicationError | None = None

        if isinstance(exc, ActivityError) and isinstance(exc.cause, ApplicationError):
            app_err = exc.cause
        elif isinstance(exc, ApplicationError):
            app_err = exc

        if app_err is not None:
            err_type = str(app_err.type or "").upper()
            if err_type.startswith("BEDROCK_"):
                code = err_type
                kind = err_type.removeprefix("BEDROCK_").lower()
                if kind in {"throttle", "network", "permission", "validation", "client"}:
                    reason = kind

        if reason == "unknown":
            text = str(exc).lower()
            if "throttl" in text or "too many" in text:
                reason = "throttle"
                code = "BEDROCK_THROTTLE"
            elif "accessdenied" in text or "unauthor" in text or "permission" in text:
                reason = "permission"
                code = "BEDROCK_PERMISSION"
            elif "timeout" in text or "connection" in text or "network" in text:
                reason = "network"
                code = "BEDROCK_NETWORK"

        return reason, code

    def _classify_artifact_error(self, exc: Exception) -> tuple[str, str]:
        if isinstance(exc, ActivityError) and isinstance(exc.cause, ApplicationError):
            err_type = str(exc.cause.type or "").upper()
            if err_type == "ARTIFACT_SCHEMA_INVALID":
                return "artifact_schema_invalid", "ARTIFACT_SCHEMA_INVALID"
        text = str(exc).lower()
        if "artifact" in text and "schema" in text:
            return "artifact_schema_invalid", "ARTIFACT_SCHEMA_INVALID"
        return "artifact_write_failed", "ARTIFACT_WRITE_FAILED"

    async def _finalize_failed(self, state: str, error_msg: str, error_code: str = "") -> dict[str, Any]:
        self.user_state = state
        self.last_error = error_msg
        self.last_error_code = error_code
        self.next_action = ""
        await self._update_index()
        await self._write_event(f"STATUS_{state}", {"error": error_msg, "error_code": error_code})
        await self._write_comms("failure", error_msg)
        return {
            "mission_id": self.mission_id,
            "status": self.user_state,
            "error": self.last_error,
            "error_code": self.last_error_code,
        }

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.base_payload = dict(payload)
        self.mission_id = str(payload.get("mission_id", "")).strip()
        self.task_id = str(payload.get("task_id", "")).strip()
        self.ship = str(payload.get("ship", "CVL")).strip() or "CVL"
        self.trace_id = str(payload.get("trace_id", self.mission_id)).strip() or self.mission_id
        if not self.mission_id:
            raise ValueError("mission_id is required")

        self.user_state = STATE_RUNNING
        self.needs_reason = ""
        self.next_action = ""
        await self._write_event("WORKFLOW_STARTED", {"task_id": self.task_id, "ship": self.ship})
        await self._update_index()
        await self._context_update(
            {
                "workflow": {"state": "RUNNING", "ship": self.ship},
                "inputs": {
                    "ticket": str(payload.get("ticket", "")),
                    "doctrine": str(payload.get("doctrine", "")),
                },
            }
        )

        budget = await self._exec_activity(
            budget_check_activity,
            {
                **self.base_payload,
                **self._meta(),
                "estimated_cost_usd": payload.get("estimated_cost_usd", 0.0),
                "budget_limit_usd": payload.get("budget_limit_usd"),
            },
        )
        if not bool(budget.get("allowed", True)):
            return await self._finalize_failed(STATE_BUDGET_DENIED, "Budget denied by budget_check_activity")

        self.hitl_mode = str(payload.get("hitl_mode", "")).strip().lower()
        timeout_hours = int(payload.get("hitl_timeout_hours", 72) or 72)
        if timeout_hours <= 0:
            timeout_hours = 72

        timeout_action = str(payload.get("hitl_timeout_action", STATE_FAILED)).strip().upper()
        if timeout_action not in {STATE_FAILED, STATE_CANCELLED}:
            timeout_action = STATE_FAILED
        self.hitl_timeout_action = timeout_action

        if self.hitl_mode == "approval":
            ok = await self._wait_for_approval(timeout_hours)
            if not ok:
                return await self._finalize_failed(
                    timeout_action,
                    f"HITL approval timeout ({timeout_hours}h)",
                )
            if not self.approval_decision:
                return await self._finalize_failed(
                    STATE_FAILED,
                    f"Approval denied: {self.approval_note}",
                )
            self.user_state = STATE_RUNNING
            self.needs_reason = ""
            self.next_action = ""
            await self._update_index()
            await self._write_event("APPROVAL_ACCEPTED", {"note": self.approval_note})
            await self._context_update(
                {
                    "hitl": {
                        "mode": "approval",
                        "decision": bool(self.approval_decision),
                        "note": self.approval_note,
                    }
                }
            )

        if self.hitl_mode == "input":
            ok = await self._wait_for_input(timeout_hours)
            if not ok:
                return await self._finalize_failed(
                    timeout_action,
                    f"HITL input timeout ({timeout_hours}h)",
                )
            self.user_state = STATE_RUNNING
            self.needs_reason = ""
            self.next_action = ""
            await self._update_index()
            await self._write_event("INPUT_RECEIVED", {"length": len(self.human_input)})
            await self._context_update(
                {
                    "hitl": {
                        "mode": "input",
                        "message": self.human_input,
                    }
                }
            )

        bedrock_result: dict[str, Any] = {"status": "NOT_USED"}
        llm_text = ""
        llm_cost = 0.0
        if bool(payload.get("use_bedrock", False)):
            budget_pre = await self._budget_guard(phase="pre", call_cost_usd=0.0)
            await self._write_event(
                "BUDGET_GUARD_PRE",
                {
                    "allowed": bool(budget_pre.get("allowed", True)),
                    "reason": str(budget_pre.get("reason", "")),
                    "total_spent_usd": budget_pre.get("total_spent_usd", 0.0),
                    "budget_limit_usd": budget_pre.get("budget_limit_usd"),
                    "source": str(budget_pre.get("source", "")),
                },
            )
            if not bool(budget_pre.get("allowed", True)):
                return await self._finalize_failed(
                    STATE_BUDGET_DENIED,
                    f"Budget guard denied before invoke ({budget_pre.get('reason', 'budget_exceeded')})",
                    "BUDGET_CIRCUIT_OPEN",
                )
            try:
                bedrock_result = await self._exec_bedrock_activity(
                    {
                        **self.base_payload,
                        **self._meta(),
                        "model_id": payload.get("model_id"),
                        "prompt": payload.get("prompt") or payload.get("ticket", ""),
                        "system_prompt": payload.get(
                            "system_prompt",
                            "You are a naval ship AI. Respond concisely in markdown.",
                        ),
                        "max_tokens": payload.get("max_tokens", 256),
                        "temperature": payload.get("temperature", 0.2),
                    }
                )
            except Exception as exc:
                reason, code = self._classify_bedrock_activity_error(exc)
                await self._write_event(
                    "BEDROCK_FAILED",
                    {
                        "reason": reason,
                        "error_code": code,
                        "message": str(exc),
                    },
                )
                return await self._finalize_failed(
                    STATE_FAILED,
                    f"Bedrock invoke failed ({reason})",
                    code,
                )
            llm_text = str(bedrock_result.get("output_text", "")).strip()
            llm_cost = float(bedrock_result.get("estimated_cost_usd", 0.0))
            await self._write_event(
                "BEDROCK_INVOKED",
                {
                    "status": bedrock_result.get("status"),
                    "input_tokens": bedrock_result.get("input_tokens", 0),
                    "output_tokens": bedrock_result.get("output_tokens", 0),
                    "estimated_cost_usd": llm_cost,
                },
            )
            budget_post = await self._budget_guard(phase="post", call_cost_usd=llm_cost)
            await self._write_event(
                "BUDGET_GUARD_POST",
                {
                    "allowed": bool(budget_post.get("allowed", True)),
                    "reason": str(budget_post.get("reason", "")),
                    "total_spent_usd": budget_post.get("total_spent_usd", 0.0),
                    "budget_limit_usd": budget_post.get("budget_limit_usd"),
                    "source": str(budget_post.get("source", "")),
                },
            )
            if not bool(budget_post.get("allowed", True)):
                return await self._finalize_failed(
                    STATE_BUDGET_DENIED,
                    f"Budget guard denied after invoke ({budget_post.get('reason', 'budget_exceeded')})",
                    "BUDGET_CIRCUIT_OPEN",
                )
            await self._context_update(
                {
                    "llm": {
                        "status": str(bedrock_result.get("status", "")),
                        "model_id": str(bedrock_result.get("model_id", "")),
                        "estimated_cost_usd": llm_cost,
                    }
                }
            )

        try:
            inputs = [{"type": "ticket", "value": payload.get("ticket", "")}]
            if self.context_ref:
                inputs.append({"type": "context_ref", "value": self.context_ref})
            artifact = await self._exec_activity(
                artifact_write_activity,
                {
                    **self.base_payload,
                    **self._meta(),
                    "human_input": self.human_input,
                    "approval_note": self.approval_note,
                    "approval_decision": self.approval_decision,
                    "result_status": "OK",
                    "message": llm_text or "Hello from MissionWorkflow",
                    "inputs": inputs,
                    "outputs": (
                        [{"type": "bedrock_output", "value": llm_text}] if llm_text else [{"type": "message", "value": "hello temporal"}]
                    ),
                    "next_actions": [
                        {"action": "review_output", "reason": "workflow completed"},
                        {"action": "cost_review", "reason": f"estimated_cost_usd={llm_cost:.6f}"},
                    ],
                },
            )
        except Exception as exc:
            reason, code = self._classify_artifact_error(exc)
            await self._write_event(
                "ARTIFACT_WRITE_FAILED",
                {
                    "reason": reason,
                    "error_code": code,
                    "message": str(exc),
                },
            )
            return await self._finalize_failed(STATE_FAILED, f"Artifact write failed ({reason})", code)
        self.user_state = STATE_DONE
        self.needs_reason = ""
        self.next_action = ""
        await self._update_index()
        await self._write_event("STATUS_DONE", {"artifact_status": artifact.get("status", "")})
        await self._context_update(
            {
                "workflow": {"state": "DONE"},
                "result": {
                    "artifact_status": str(artifact.get("status", "")),
                    "context_ref": self.context_ref,
                },
            }
        )
        return {
            "mission_id": self.mission_id,
            "status": STATE_DONE,
            "artifact": artifact,
            "bedrock": bedrock_result,
            "context_ref": self.context_ref,
        }
