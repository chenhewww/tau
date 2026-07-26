"""Portable run lifecycle models for harness executions."""

from __future__ import annotations

from collections.abc import AsyncIterator
from enum import StrEnum
from typing import Protocol, Self

from pydantic import ConfigDict, Field, model_validator

from tau_agent.messages import AgentMessage, WireModel
from tau_agent.runtime.events import RunEvent


class RunStatus(StrEnum):
    """Lifecycle state for one bounded agent execution."""

    CREATED = "created"
    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"


TERMINAL_RUN_STATUSES = frozenset(
    {
        RunStatus.SUCCEEDED,
        RunStatus.FAILED,
        RunStatus.CANCELLED,
        RunStatus.TIMED_OUT,
    }
)

_ALLOWED_RUN_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.CREATED: frozenset({RunStatus.QUEUED, RunStatus.STARTING, RunStatus.CANCELLED}),
    RunStatus.QUEUED: frozenset({RunStatus.STARTING, RunStatus.CANCELLED, RunStatus.TIMED_OUT}),
    RunStatus.STARTING: frozenset(
        {
            RunStatus.RUNNING,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.TIMED_OUT,
        }
    ),
    RunStatus.RUNNING: TERMINAL_RUN_STATUSES,
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.CANCELLED: frozenset(),
    RunStatus.TIMED_OUT: frozenset(),
}


class RunTransitionError(ValueError):
    """Raised when a run attempts an invalid lifecycle transition."""


def transition_run_status(current: RunStatus, target: RunStatus) -> RunStatus:
    """Return ``target`` when the lifecycle transition is valid."""

    if target not in _ALLOWED_RUN_TRANSITIONS[current]:
        raise RunTransitionError(f"Invalid run status transition: {current} -> {target}")
    return target


class RunBudget(WireModel):
    """Portable limits that a future scheduler can assign to a run.

    The first local runtime deliberately does not accept or enforce this model.
    Keeping the schema separate prevents callers from mistaking recorded limits
    for limits that are already enforced.
    """

    model_config = ConfigDict(frozen=True)

    max_turns: int | None = Field(default=None, ge=1)
    max_tool_calls: int | None = Field(default=None, ge=0)
    max_input_tokens: int | None = Field(default=None, ge=0)
    max_output_tokens: int | None = Field(default=None, ge=0)
    max_wall_time_seconds: float | None = Field(default=None, gt=0)


class RunSnapshot(WireModel):
    """Immutable-style point-in-time view of one run."""

    model_config = ConfigDict(frozen=True)

    run_id: str = Field(min_length=1)
    session_id: str | None = None
    parent_run_id: str | None = None
    status: RunStatus
    created_at: int = Field(ge=0)
    started_at: int | None = Field(default=None, ge=0)
    finished_at: int | None = Field(default=None, ge=0)
    event_count: int = Field(default=0, ge=0)
    last_event_sequence: int = Field(default=0, ge=0)
    turn_count: int = Field(default=0, ge=0)
    tool_call_count: int = Field(default=0, ge=0)
    error: str | None = Field(
        default=None,
        description="Local diagnostic text; transports must sanitize it",
    )

    @model_validator(mode="after")
    def _validate_lifecycle(self) -> Self:
        terminal = self.status in TERMINAL_RUN_STATUSES
        if terminal and self.finished_at is None:
            raise ValueError("A terminal run snapshot requires finished_at")
        if not terminal and self.finished_at is not None:
            raise ValueError("A non-terminal run snapshot cannot have finished_at")
        if not terminal and self.error is not None:
            raise ValueError("A non-terminal run snapshot cannot contain an error")
        if self.status in {RunStatus.SUCCEEDED, RunStatus.CANCELLED} and self.error is not None:
            raise ValueError(f"A {self.status} run snapshot cannot contain an error")
        if self.last_event_sequence > self.event_count:
            raise ValueError("last_event_sequence cannot exceed event_count")
        return self

    @property
    def is_terminal(self) -> bool:
        """Return whether this snapshot represents a settled run."""

        return self.status in TERMINAL_RUN_STATUSES


class RunResult(WireModel):
    """Terminal result for a run and the messages it produced."""

    model_config = ConfigDict(frozen=True)

    snapshot: RunSnapshot
    messages: tuple[AgentMessage, ...] = ()

    @model_validator(mode="after")
    def _validate_terminal_snapshot(self) -> Self:
        if not self.snapshot.is_terminal:
            raise ValueError("RunResult requires a terminal snapshot")
        return self


class RunHandle(Protocol):
    """Control and observation surface for one owned run."""

    @property
    def run_id(self) -> str:
        """Return the stable run identifier."""
        ...

    @property
    def snapshot(self) -> RunSnapshot:
        """Return the latest lifecycle snapshot."""
        ...

    def events(self, *, after_sequence: int = 0) -> AsyncIterator[RunEvent]:
        """Replay and follow events after a known sequence cursor."""
        ...

    async def result(self) -> RunResult:
        """Wait for and return the terminal run result."""
        ...

    def cancel(self) -> RunSnapshot:
        """Request cooperative cancellation and return the current snapshot."""
        ...

    def steer(self, message: str) -> None:
        """Queue a steering message for the active run."""
        ...

    def follow_up(self, message: str) -> None:
        """Queue a follow-up message for the active run."""
        ...

    async def aclose(self) -> None:
        """Force-close the owned runner task and wait for cleanup."""
        ...
