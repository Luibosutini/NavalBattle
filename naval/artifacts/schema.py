from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

ShipLiteral = Literal["CVL", "DD", "CL", "CVB", "CA", "BB"]
ResultStatusLiteral = Literal["OK", "NG", "BLOCKED"]

SHIP_REQUIRED_PAYLOAD_KEYS: dict[str, tuple[str, ...]] = {
    "CVL": ("requirements", "constraints", "questions", "risks"),
    "DD": ("plan", "files_changed", "commands", "diff_summary"),
    "CL": ("test_plan", "test_results", "repro_steps"),
}

SUMMARY_REQUIRED_BLOCKS: tuple[str, ...] = (
    "## 決定事項",
    "## 未決事項",
    "## 次アクション",
)


class ArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    value: str = Field(min_length=1)


class NextAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    ship: ShipLiteral | None = None


class ResultArtifact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(min_length=1)
    mission_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    ship: ShipLiteral
    status: ResultStatusLiteral
    confidence: float = Field(ge=0.0, le=1.0)
    trace_id: str = Field(min_length=1)
    inputs: list[ArtifactRef] = Field(min_length=1)
    outputs: list[ArtifactRef] = Field(min_length=1)
    next_actions: list[NextAction] = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_ship_payload(self) -> "ResultArtifact":
        required_keys = SHIP_REQUIRED_PAYLOAD_KEYS.get(self.ship, ())
        if not required_keys:
            return self
        missing = [k for k in required_keys if k not in self.payload]
        if missing:
            raise ValueError(f"payload missing required keys for ship={self.ship}: {', '.join(missing)}")
        return self


def validate_result_artifact(data: dict[str, Any]) -> dict[str, Any]:
    return ResultArtifact.model_validate(data).model_dump(mode="json")


def validate_summary_markdown(summary: str) -> str:
    text = str(summary or "").strip()
    if not text:
        raise ValueError("summary.md is empty")
    missing = [block for block in SUMMARY_REQUIRED_BLOCKS if block not in text]
    if missing:
        raise ValueError(f"summary.md missing required sections: {', '.join(missing)}")
    return text
