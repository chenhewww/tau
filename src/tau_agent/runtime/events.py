"""Run-scoped envelopes for portable agent events."""

from __future__ import annotations

from pydantic import ConfigDict, Field

from tau_agent.events import AgentEvent
from tau_agent.messages import WireModel


class RunEvent(WireModel):
    """One agent event annotated with run identity and causal order."""

    model_config = ConfigDict(frozen=True)

    run_id: str = Field(min_length=1)
    sequence: int = Field(ge=1)
    timestamp: int
    event: AgentEvent
