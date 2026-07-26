"""Asyncio-backed, single-active-run adapter for ``AgentHarness`` executions."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from uuid import uuid4

from tau_agent.events import (
    AgentEndEvent,
    AgentEvent,
    MessageEndEvent,
    ToolExecutionStartEvent,
    TurnStartEvent,
)
from tau_agent.harness import AgentHarness
from tau_agent.messages import AgentMessage, AssistantMessage, current_timestamp_ms
from tau_agent.runtime.events import RunEvent
from tau_agent.runtime.types import (
    TERMINAL_RUN_STATUSES,
    RunHandle,
    RunResult,
    RunSnapshot,
    RunStatus,
    transition_run_status,
)

EventStreamFactory = Callable[[object], AsyncIterator[AgentEvent]]


class RunConflictError(RuntimeError):
    """Raised when a local runtime already owns an active run."""


class RunNotFoundError(KeyError):
    """Raised when a run id is unknown to a local runtime."""


class _LocalRunHandle:
    """Concrete handle for one background-driven harness execution.

    The runner task, rather than an event consumer, owns the underlying harness
    generator. Consumers may disconnect, reconnect, or stop iterating without
    accidentally abandoning the harness in its running state.
    """

    def __init__(
        self,
        *,
        harness: AgentHarness,
        lease: object,
        stream_factory: Callable[[], AsyncIterator[AgentEvent]],
        session_id: str | None,
        parent_run_id: str | None,
    ) -> None:
        self._harness = harness
        self._lease = lease
        self._lease_released = False
        self._stream_factory = stream_factory
        self._run_id = uuid4().hex
        if parent_run_id == self._run_id:
            raise ValueError("A run cannot be its own parent")
        self._session_id = session_id
        self._parent_run_id = parent_run_id
        self._status = RunStatus.CREATED
        self._created_at = current_timestamp_ms()
        self._started_at: int | None = None
        self._finished_at: int | None = None
        self._turn_count = 0
        self._tool_call_count = 0
        self._error: str | None = None
        self._events: list[RunEvent] = []
        self._change_signal = asyncio.Event()
        self._cancel_requested = False
        self._saw_agent_end = False
        self._terminal_stop_reason: str | None = None
        self._terminal_error: str | None = None
        self._message_start: int | None = None
        self._canonical_messages: tuple[AgentMessage, ...] | None = None
        self._messages: tuple[AgentMessage, ...] = ()
        self._pending_steering: list[str] = []
        self._pending_follow_ups: list[str] = []
        self._completion: asyncio.Future[RunResult] = asyncio.get_running_loop().create_future()
        self._task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def snapshot(self) -> RunSnapshot:
        return RunSnapshot(
            run_id=self._run_id,
            session_id=self._session_id,
            parent_run_id=self._parent_run_id,
            status=self._status,
            created_at=self._created_at,
            started_at=self._started_at,
            finished_at=self._finished_at,
            event_count=len(self._events),
            last_event_sequence=len(self._events),
            turn_count=self._turn_count,
            tool_call_count=self._tool_call_count,
            error=self._error,
        )

    def _launch(self) -> asyncio.Task[None]:
        if self._task is not None:
            raise RuntimeError("Run has already been launched")
        self._transition(RunStatus.STARTING)
        self._task = asyncio.create_task(self._drive(), name=f"tau-run-{self._run_id}")
        return self._task

    async def _drive(self) -> None:
        try:
            if self._cancel_requested:
                self._finish(RunStatus.CANCELLED)
                return

            self._transition(RunStatus.RUNNING)
            self._started_at = current_timestamp_ms()
            self._flush_pending_controls()
            source = self._stream_factory()
            self._message_start = len(self._harness.messages)
            try:
                async for event in source:
                    self._observe(event)
                    self._publish(event)
            finally:
                close = getattr(source, "aclose", None)
                if close is not None:
                    await close()

            if self._cancel_requested:
                self._finish(RunStatus.CANCELLED)
            elif not self._saw_agent_end:
                self._finish(
                    RunStatus.FAILED,
                    error="Harness event stream ended without agent_end",
                )
            elif self._terminal_stop_reason is not None:
                if self._terminal_stop_reason == "aborted":
                    self._finish(RunStatus.CANCELLED)
                elif self._terminal_stop_reason == "error":
                    self._finish(
                        RunStatus.FAILED,
                        error=self._terminal_error or "Agent run failed",
                    )
                else:
                    self._finish(RunStatus.SUCCEEDED)
            else:
                self._finish(RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            self._harness._cancel_with_runtime_lease(self._lease)
            self._finish(RunStatus.CANCELLED)
            raise
        except Exception as exc:  # noqa: BLE001 - runtime is the ownership boundary
            if self._cancel_requested:
                self._finish(RunStatus.CANCELLED)
            else:
                self._finish(RunStatus.FAILED, error=str(exc) or type(exc).__name__)

    def _observe(self, event: AgentEvent) -> None:
        if isinstance(event, TurnStartEvent):
            self._turn_count += 1
        elif isinstance(event, ToolExecutionStartEvent):
            self._tool_call_count += 1
        elif isinstance(event, MessageEndEvent) and isinstance(event.message, AssistantMessage):
            self._terminal_stop_reason = event.message.stop_reason
            self._terminal_error = event.message.error_message
        elif isinstance(event, AgentEndEvent):
            self._saw_agent_end = True
            self._canonical_messages = tuple(
                message.model_copy(deep=True) for message in event.messages
            )

    def _publish(self, event: AgentEvent) -> None:
        envelope = RunEvent(
            run_id=self._run_id,
            sequence=len(self._events) + 1,
            timestamp=current_timestamp_ms(),
            event=event.model_copy(deep=True),
        )
        self._events.append(envelope)
        self._notify_change()

    def _finish(self, status: RunStatus, *, error: str | None = None) -> None:
        if self._status in TERMINAL_RUN_STATUSES:
            return
        self._harness._repair_with_runtime_lease(self._lease)
        self._capture_messages()
        self._clear_controls()
        self._release_lease()
        self._transition(status)
        self._finished_at = current_timestamp_ms()
        self._error = error
        result = RunResult(snapshot=self.snapshot, messages=self._messages)
        if not self._completion.done():
            self._completion.set_result(result)
        self._notify_change()

    def _capture_messages(self) -> None:
        if self._canonical_messages is not None:
            self._messages = self._canonical_messages
            return
        if self._message_start is None:
            self._messages = ()
            return
        self._messages = tuple(
            message.model_copy(deep=True)
            for message in self._harness.messages[self._message_start :]
        )

    def _flush_pending_controls(self) -> None:
        for message in self._pending_steering:
            self._harness._steer_with_runtime_lease(message, self._lease)
        for message in self._pending_follow_ups:
            self._harness._follow_up_with_runtime_lease(message, self._lease)
        self._pending_steering.clear()
        self._pending_follow_ups.clear()

    def _clear_controls(self) -> None:
        self._pending_steering.clear()
        self._pending_follow_ups.clear()
        self._harness._clear_queues_with_runtime_lease(self._lease)

    def _release_lease(self) -> None:
        if self._lease_released:
            return
        self._harness._release_runtime_lease(self._lease)
        self._lease_released = True

    def _notify_change(self) -> None:
        signal = self._change_signal
        self._change_signal = asyncio.Event()
        signal.set()

    def _transition(self, target: RunStatus) -> None:
        self._status = transition_run_status(self._status, target)

    async def events(self, *, after_sequence: int = 0) -> AsyncIterator[RunEvent]:
        """Replay buffered events, then follow the run until it settles."""

        if after_sequence < 0:
            raise ValueError("after_sequence must be at least 0")
        if after_sequence > len(self._events):
            raise ValueError(
                f"after_sequence={after_sequence} is beyond the latest event sequence "
                f"{len(self._events)}"
            )

        cursor = after_sequence
        while True:
            while cursor < len(self._events):
                event = self._events[cursor]
                cursor += 1
                yield event.model_copy(deep=True)

            if self._status in TERMINAL_RUN_STATUSES:
                return
            signal = self._change_signal
            await signal.wait()

    async def result(self) -> RunResult:
        """Wait for terminal state without letting caller cancellation kill the run."""

        result = await asyncio.shield(self._completion)
        return result.model_copy(deep=True)

    def cancel(self) -> RunSnapshot:
        """Request cooperative cancellation; terminal state is asynchronous."""

        if self._status not in TERMINAL_RUN_STATUSES:
            self._cancel_requested = True
            self._harness._cancel_with_runtime_lease(self._lease)
        return self.snapshot

    def steer(self, message: str) -> None:
        if self._status not in {RunStatus.STARTING, RunStatus.RUNNING}:
            raise RuntimeError(f"Cannot steer run in {self._status} state")
        if self._cancel_requested:
            raise RuntimeError("Cannot steer a run after cancellation was requested")
        if self._saw_agent_end:
            raise RuntimeError("Cannot steer a run after agent_end")
        if self._status is RunStatus.STARTING:
            self._pending_steering.append(message)
        else:
            if not self._harness.accepting_controls:
                raise RuntimeError("Cannot steer a run that is settling")
            self._harness._steer_with_runtime_lease(message, self._lease)

    def follow_up(self, message: str) -> None:
        if self._status not in {RunStatus.STARTING, RunStatus.RUNNING}:
            raise RuntimeError(f"Cannot add a follow-up to run in {self._status} state")
        if self._cancel_requested:
            raise RuntimeError("Cannot add a follow-up after cancellation was requested")
        if self._saw_agent_end:
            raise RuntimeError("Cannot add a follow-up after agent_end")
        if self._status is RunStatus.STARTING:
            self._pending_follow_ups.append(message)
        else:
            if not self._harness.accepting_controls:
                raise RuntimeError("Cannot add a follow-up to a run that is settling")
            self._harness._follow_up_with_runtime_lease(message, self._lease)

    async def aclose(self) -> None:
        """Hard-cancel the owned runner and isolate cleanup from caller cancellation."""

        self.cancel()
        task = self._task
        if task is None:
            self._finish(RunStatus.CANCELLED)
            return
        current_task = asyncio.current_task()
        if current_task is not None and task is current_task:
            task.cancel()
            return
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_owned_runner(),
                name=f"tau-run-close-{self._run_id}",
            )
        close_task = self._close_task
        await self._wait_for_close(close_task)

    async def _close_owned_runner(self) -> None:
        task = self._task
        if task is None:
            self._finish(RunStatus.CANCELLED)
            return
        if self._status not in TERMINAL_RUN_STATUSES:
            task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        if self._status not in TERMINAL_RUN_STATUSES:
            self._finish(RunStatus.CANCELLED)

    def _settle_from_finished_task(self, task: asyncio.Task[None]) -> None:
        if self._status in TERMINAL_RUN_STATUSES:
            return
        if task.cancelled() or self._cancel_requested:
            self._finish(RunStatus.CANCELLED)
            return
        exception = task.exception()
        error = (
            str(exception) or type(exception).__name__
            if exception is not None
            else "Runner task ended without settling the run"
        )
        self._finish(RunStatus.FAILED, error=error)

    @staticmethod
    async def _wait_for_close(close_task: asyncio.Task[None]) -> None:
        caller_cancellation: asyncio.CancelledError | None = None
        while not close_task.done():
            try:
                await asyncio.shield(close_task)
            except asyncio.CancelledError as exc:
                if close_task.cancelled():
                    raise
                caller_cancellation = exc
        if close_task.cancelled():
            raise asyncio.CancelledError
        exception = close_task.exception()
        if exception is not None:
            raise exception
        if caller_cancellation is not None:
            raise caller_cancellation


class LocalAgentRuntime:
    """Asyncio-only in-memory runtime with one active harness run at a time.

    Start and control each run from the same running asyncio event loop.
    """

    def __init__(self) -> None:
        self._runs: dict[str, _LocalRunHandle] = {}
        self._active_run_id: str | None = None

    @property
    def active_run_id(self) -> str | None:
        handle = self._active_handle()
        return handle.run_id if handle is not None else None

    def prompt(
        self,
        harness: AgentHarness,
        content: str,
        *,
        session_id: str | None = None,
        parent_run_id: str | None = None,
    ) -> RunHandle:
        """Start and own one ``AgentHarness.prompt`` execution."""

        return self._start(
            harness,
            lambda lease: harness._prompt_with_runtime_lease(content, lease),
            session_id=session_id,
            parent_run_id=parent_run_id,
        )

    def continue_(
        self,
        harness: AgentHarness,
        *,
        session_id: str | None = None,
        parent_run_id: str | None = None,
    ) -> RunHandle:
        """Start and own one ``AgentHarness.continue_`` execution."""

        return self._start(
            harness,
            harness._continue_with_runtime_lease,
            session_id=session_id,
            parent_run_id=parent_run_id,
        )

    def _start(
        self,
        harness: AgentHarness,
        stream_factory: EventStreamFactory,
        *,
        session_id: str | None,
        parent_run_id: str | None,
    ) -> RunHandle:
        active = self._active_handle()
        if active is not None:
            raise RunConflictError(f"Run {active.run_id} is already active")
        try:
            lease = harness._claim_runtime_lease()
        except RuntimeError as exc:
            raise RunConflictError(str(exc)) from exc
        try:
            if harness.has_queued_messages():
                raise RunConflictError(
                    "AgentHarness has queued messages; clear them before transferring "
                    "exclusive ownership to LocalAgentRuntime"
                )

            handle = _LocalRunHandle(
                harness=harness,
                lease=lease,
                stream_factory=lambda: stream_factory(lease),
                session_id=session_id,
                parent_run_id=parent_run_id,
            )
            self._runs[handle.run_id] = handle
            self._active_run_id = handle.run_id
            task = handle._launch()
            task.add_done_callback(lambda finished: self._on_run_done(handle, finished))
            return handle
        except Exception:
            harness._release_runtime_lease(lease)
            raise

    def inspect(self, run_id: str) -> RunSnapshot:
        return self._handle(run_id).snapshot

    def get(self, run_id: str) -> RunHandle:
        """Return a retained run handle for replay or result lookup."""

        return self._handle(run_id)

    def cancel(self, run_id: str) -> RunSnapshot:
        return self._handle(run_id).cancel()

    def steer(self, run_id: str, message: str) -> None:
        self._handle(run_id).steer(message)

    def follow_up(self, run_id: str, message: str) -> None:
        self._handle(run_id).follow_up(message)

    async def shutdown(self) -> None:
        """Force-close the active run, if any, and release runtime ownership."""

        active = self._active_handle()
        if active is not None:
            await active.aclose()
            if active.snapshot.is_terminal and self._active_run_id == active.run_id:
                self._active_run_id = None

    def _handle(self, run_id: str) -> _LocalRunHandle:
        try:
            return self._runs[run_id]
        except KeyError as exc:
            raise RunNotFoundError(run_id) from exc

    def _active_handle(self) -> _LocalRunHandle | None:
        if self._active_run_id is None:
            return None
        handle = self._runs[self._active_run_id]
        if handle.snapshot.is_terminal:
            self._active_run_id = None
            return None
        return handle

    def _on_run_done(
        self,
        handle: _LocalRunHandle,
        task: asyncio.Task[None],
    ) -> None:
        handle._settle_from_finished_task(task)
        if self._active_run_id == handle.run_id:
            self._active_run_id = None
        with suppress(asyncio.CancelledError):
            task.exception()
