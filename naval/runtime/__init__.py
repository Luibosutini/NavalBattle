from __future__ import annotations

from .aws_runtime import AwsRuntime
from .base import RuntimeBase, RuntimeContext
from .temporal_runtime import TemporalRuntime


def make_runtime(ctx: RuntimeContext) -> RuntimeBase:
    if ctx.runtime == "aws":
        return AwsRuntime(ctx)
    if ctx.runtime == "temporal":
        return TemporalRuntime(ctx)
    raise ValueError(f"Unsupported runtime: {ctx.runtime}")

