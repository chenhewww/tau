"""Local run ownership for complete :class:`CodingSession` executions.

This module deliberately lives in ``tau_coding``.  A coding-session run spans
more than one portable harness invocation: persistence, compaction, overflow
recovery, automatic retry, and extension events are all inside its settled
boundary.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from typing import Literal, Protocol
from uuid import uuid4

from pydantic import ConfigDict, Field

from tau_agent.events import MessageEndEvent, ToolExecutionStartEvent, TurnStartEvent
from tau_agent.messages import AgentMessage, AssistantMessage, WireModel, current_timestamp_ms
from tau_agent.runtime import (
    TERMINAL_RUN_STATUSES,
    RunConflictError,
    RunNotFoundError,
    RunResult,
    RunSnapshot,
    RunStatus,
    transition_run_status,
)
from tau_agent.types import JSONValue
from tau_coding.events import (
    AgentSettledEvent,
    AutoRetryStartEvent,
    CodingSessionEvent,
    QueueUpdateEvent,
    SessionAgentEndEvent,
)


class CodingRunCleanupError(RuntimeError):
    """A cancellation cleanup failed after the provider had been stopped."""


class CodingRunEvent(WireModel):
    """One coding-session event annotated with run identity and causal order."""

    model_config = ConfigDict(frozen=True)

    run_id: str = Field(min_length=1)
    sequence: int = Field(ge=1)
    timestamp: int = Field(ge=0)
    event: CodingSessionEvent


class CodingRunHandle(Protocol):
    """Control and observation surface for one complete coding-session run."""

    @property
    def run_id(self) -> str:
        """Return the stable run identifier."""
        ...

    @property
    def snapshot(self) -> RunSnapshot:
        """Return the latest lifecycle snapshot."""
        ...

    def events(self, *, after_sequence: int = 0) -> AsyncIterator[CodingRunEvent]:
        """Replay and follow events after a known sequence cursor."""
        ...

    async def result(self) -> RunResult:
        """Wait for and return the terminal result."""
        ...

    def cancel(self) -> RunSnapshot:
        """Request cooperative cancellation and return the current snapshot."""
        ...

    async def steer(self, message: str) -> None:
        """Queue a steering message through the coding-session input policy."""
        ...

    async def follow_up(self, message: str) -> None:
        """Queue a follow-up message through the coding-session input policy."""
        ...

    async def aclose(self) -> None:
        """Force-close the owned runner task and wait for cleanup."""
        ...


class _CodingSessionDriver(Protocol):
    """Private structural seam implemented by ``CodingSession``."""

    @property
    def session_id(self) -> str | None: ...

    def _claim_runtime_lease(self, run_id: str) -> object: ...

    def _bind_runtime_callbacks(
        self,
        lease: object,
        *,
        cancel: Callable[[], RunSnapshot],
        close: Callable[[], Awaitable[None]],
        publish: Callable[[CodingSessionEvent], None],
        accepting_controls: Callable[[], bool],
        cancellation_requested: Callable[[], bool],
        control_started: Callable[[], None],
        control_finished: Callable[[], None],
    ) -> None: ...

    def _cancel_with_runtime_lease(self, lease: object) -> None: ...

    def _clear_queues_with_runtime_lease(self, lease: object) -> QueueUpdateEvent: ...

    def _result_messages_with_runtime_lease(
        self,
        lease: object,
        observed: tuple[AgentMessage, ...],
    ) -> tuple[AgentMessage, ...]: ...

    def _durability_failure_with_runtime_lease(self, lease: object) -> str | None: ...

    def _release_runtime_lease(self, lease: object) -> None: ...

    def _prompt_with_runtime_lease(
        self,
        content: str,
        lease: object,
        *,
        run_id: str,
        source: Literal["interactive", "extension"] = "interactive",
        custom_type: str | None = None,
        details: dict[str, JSONValue] | None = None,
    ) -> AsyncIterator[CodingSessionEvent]: ...

    def _continue_with_runtime_lease(
        self,
        lease: object,
        *,
        run_id: str,
    ) -> AsyncIterator[CodingSessionEvent]: ...

    def _queue_control_with_runtime_lease(
        self,
        content: str,
        lease: object,
        *,
        streaming_behavior: Literal["steer", "follow_up"],
        run_id: str,
    ) -> AsyncIterator[CodingSessionEvent]: ...


class _LocalCodingRunHandle:
    """Background owner for one complete coding-session event stream."""

    def __init__(
        self,
        *,
        run_id: str,
        session: _CodingSessionDriver,
        lease: object,
        stream_factory: Callable[[], AsyncIterator[CodingSessionEvent]],
        parent_run_id: str | None,
    ) -> None:
        if parent_run_id == run_id:
            raise ValueError("A run cannot be its own parent")
        self._run_id = run_id
        self._session = session
        self._session_id = session.session_id
        self._lease = lease
        self._lease_released = False
        self._stream_factory = stream_factory
        self._parent_run_id = parent_run_id
        self._status = RunStatus.CREATED
        self._created_at = current_timestamp_ms()
        self._started_at: int | None = None
        self._finished_at: int | None = None
        self._turn_count = 0
        self._tool_call_count = 0
        self._error: str | None = None
        self._events: list[CodingRunEvent] = []
        self._messages: list[AgentMessage] = []
        self._change_signal = asyncio.Event()
        self._cancel_requested = False
        self._saw_any_event = False
        self._saw_settled = False
        self._accepting_controls = True
        self._inflight_controls = 0
        self._control_change = asyncio.Event()
        self._last_assistant: AssistantMessage | None = None
        self._last_queue_update: QueueUpdateEvent | None = None
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
        self._task = asyncio.create_task(self._drive(), name=f"tau-coding-run-{self._run_id}")
        return self._task

    async def _drive(self) -> None:
        try:
            if self._cancel_requested:
                self._finish(RunStatus.CANCELLED)
                return

            self._started_at = current_timestamp_ms()
            self._transition(RunStatus.RUNNING)
            source = self._stream_factory()
            try:
                async for event in source:
                    self._observe(event)
                    self._publish(event)
            finally:
                close = getattr(source, "aclose", None)
                if close is not None:
                    await close()

            await self._wait_for_controls()

            if self._cancel_requested:
                self._finish(RunStatus.CANCELLED)
            elif not self._saw_any_event:
                # An input hook may consume a prompt without starting the agent.
                self._finish(RunStatus.SUCCEEDED)
            elif not self._saw_settled:
                self._finish(
                    RunStatus.FAILED,
                    error="CodingSession event stream ended without agent_settled",
                )
            elif self._last_assistant is not None:
                if self._last_assistant.stop_reason == "aborted":
                    self._finish(RunStatus.CANCELLED)
                elif self._last_assistant.stop_reason == "error":
                    self._finish(
                        RunStatus.FAILED,
                        error=self._last_assistant.error_message or "Agent run failed",
                    )
                else:
                    self._finish(RunStatus.SUCCEEDED)
            else:
                self._finish(RunStatus.SUCCEEDED)
        except asyncio.CancelledError:
            self._session._cancel_with_runtime_lease(self._lease)
            durability_error = self._session._durability_failure_with_runtime_lease(self._lease)
            if durability_error is not None:
                self._finish(RunStatus.FAILED, error=durability_error)
            elif self._cancel_requested:
                self._finish(RunStatus.CANCELLED)
            else:
                self._finish(
                    RunStatus.FAILED,
                    error="Runner cancelled without a run cancellation request",
                )
            raise
        except Exception as exc:  # noqa: BLE001 - runtime is the ownership boundary
            durability_error = self._session._durability_failure_with_runtime_lease(self._lease)
            if (
                self._cancel_requested
                and not isinstance(exc, CodingRunCleanupError)
                and durability_error is None
            ):
                self._finish(RunStatus.CANCELLED)
            else:
                self._finish(RunStatus.FAILED, error=str(exc) or type(exc).__name__)

    def _observe(self, event: CodingSessionEvent) -> None:
        self._saw_any_event = True
        if isinstance(event, TurnStartEvent):
            self._turn_count += 1
        elif isinstance(event, ToolExecutionStartEvent):
            self._tool_call_count += 1
        elif isinstance(event, MessageEndEvent):
            self._messages.append(event.message.model_copy(deep=True))
            if isinstance(event.message, AssistantMessage):
                self._last_assistant = event.message.model_copy(deep=True)
        elif isinstance(event, SessionAgentEndEvent):
            self._accepting_controls = False
        elif isinstance(event, AutoRetryStartEvent):
            self._accepting_controls = True
        elif isinstance(event, AgentSettledEvent):
            self._saw_settled = True
            self._accepting_controls = False
        elif isinstance(event, QueueUpdateEvent):
            self._last_queue_update = event.model_copy(deep=True)

    def _publish(self, event: CodingSessionEvent) -> None:
        if isinstance(event, QueueUpdateEvent):
            self._last_queue_update = event.model_copy(deep=True)
        envelope = CodingRunEvent(
            run_id=self._run_id,
            sequence=len(self._events) + 1,
            timestamp=current_timestamp_ms(),
            event=event.model_copy(deep=True),
        )
        self._events.append(envelope)
        self._notify_change()

    def _publish_control_event(self, event: CodingSessionEvent) -> None:
        if self._status in TERMINAL_RUN_STATUSES:
            raise RuntimeError(f"Cannot publish controls to a run in {self._status} state")
        self._publish(event)

    def _finish(self, status: RunStatus, *, error: str | None = None) -> None:
        if self._status in TERMINAL_RUN_STATUSES:
            return
        self._accepting_controls = False
        cleanup_error: Exception | None = None
        result_messages = tuple(self._messages)
        try:
            if self._saw_any_event or self._last_queue_update is not None:
                empty_queue = self._session._clear_queues_with_runtime_lease(self._lease)
                if self._last_queue_update is not None and (
                    self._last_queue_update.steering or self._last_queue_update.follow_up
                ):
                    self._publish(empty_queue)
            result_messages = self._session._result_messages_with_runtime_lease(
                self._lease,
                tuple(self._messages),
            )
        except Exception as exc:  # noqa: BLE001 - cleanup failure changes the terminal result
            cleanup_error = exc
        try:
            self._release_lease()
        except Exception as exc:  # noqa: BLE001 - lease cleanup is part of settlement
            cleanup_error = cleanup_error or exc

        if cleanup_error is not None:
            status = RunStatus.FAILED
            error = str(cleanup_error) or type(cleanup_error).__name__
        self._transition(status)
        self._finished_at = current_timestamp_ms()
        self._error = error
        result = RunResult(snapshot=self.snapshot, messages=result_messages)
        if not self._completion.done():
            self._completion.set_result(result)
        self._notify_change()

    def _release_lease(self) -> None:
        if self._lease_released:
            return
        self._session._release_runtime_lease(self._lease)
        self._lease_released = True

    def _notify_change(self) -> None:
        signal = self._change_signal
        self._change_signal = asyncio.Event()
        signal.set()

    def _transition(self, target: RunStatus) -> None:
        self._status = transition_run_status(self._status, target)

    def _controls_are_open(self) -> bool:
        return (
            self._accepting_controls
            and not self._cancel_requested
            and self._status in {RunStatus.STARTING, RunStatus.RUNNING}
        )

    def _cancellation_was_requested(self) -> bool:
        return self._cancel_requested

    async def events(self, *, after_sequence: int = 0) -> AsyncIterator[CodingRunEvent]:
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
        """Wait for terminal state without letting waiter cancellation kill the run."""

        result = await asyncio.shield(self._completion)
        return result.model_copy(deep=True)

    def cancel(self) -> RunSnapshot:
        """Request cancellation and interrupt session-owned awaits."""

        if self._status in TERMINAL_RUN_STATUSES or self._cancel_requested:
            return self.snapshot
        self._cancel_requested = True
        self._accepting_controls = False
        self._session._cancel_with_runtime_lease(self._lease)
        if self._task is not None and not self._task.done():
            self._task.cancel()
        return self.snapshot

    async def steer(self, message: str) -> None:
        await self._queue_control(message, streaming_behavior="steer")

    async def follow_up(self, message: str) -> None:
        await self._queue_control(message, streaming_behavior="follow_up")

    async def _queue_control(
        self,
        message: str,
        *,
        streaming_behavior: Literal["steer", "follow_up"],
    ) -> None:
        self._control_started()
        try:
            async for _event in self._session._queue_control_with_runtime_lease(
                message,
                self._lease,
                streaming_behavior=streaming_behavior,
                run_id=self._run_id,
            ):
                # The session publishes the event through the callback bound by the
                # runtime, so every watcher observes one canonical copy.
                pass
        finally:
            self._control_finished()

    def _control_started(self) -> None:
        if not self._controls_are_open():
            raise RuntimeError(f"Cannot queue controls to a run in {self._status} state")
        self._inflight_controls += 1

    def _control_finished(self) -> None:
        if self._inflight_controls <= 0:
            raise RuntimeError("Coding run control accounting is unbalanced")
        self._inflight_controls -= 1
        signal = self._control_change
        self._control_change = asyncio.Event()
        signal.set()

    async def _wait_for_controls(self) -> None:
        while self._inflight_controls:
            signal = self._control_change
            await signal.wait()

    async def aclose(self) -> None:
        """Hard-cancel the owned runner and isolate cleanup from caller cancellation."""

        self.cancel()
        task = self._task
        if task is None:
            self._finish(RunStatus.CANCELLED)
            return
        current_task = asyncio.current_task()
        if current_task is not None and task is current_task:
            return
        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_owned_runner(),
                name=f"tau-coding-run-close-{self._run_id}",
            )
        await self._wait_for_close(self._close_task)

    async def _close_owned_runner(self) -> None:
        task = self._task
        if task is None:
            self._finish(RunStatus.CANCELLED)
            return
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


class LocalCodingSessionRuntime:
    """Asyncio-only in-memory runtime for complete ``CodingSession`` runs."""

    def __init__(self) -> None:
        self._runs: dict[str, _LocalCodingRunHandle] = {}
        self._active_run_id: str | None = None

    @property
    def active_run_id(self) -> str | None:
        handle = self._active_handle()
        return handle.run_id if handle is not None else None

    def prompt(
        self,
        session: _CodingSessionDriver,
        content: str,
        *,
        parent_run_id: str | None = None,
        source: Literal["interactive", "extension"] = "interactive",
        custom_type: str | None = None,
        details: dict[str, JSONValue] | None = None,
    ) -> CodingRunHandle:
        """Start and own one complete ``CodingSession.prompt`` execution."""

        return self._start(
            session,
            lambda lease, run_id: session._prompt_with_runtime_lease(
                content,
                lease,
                run_id=run_id,
                source=source,
                custom_type=custom_type,
                details=details,
            ),
            parent_run_id=parent_run_id,
        )

    def continue_(
        self,
        session: _CodingSessionDriver,
        *,
        parent_run_id: str | None = None,
    ) -> CodingRunHandle:
        """Start and own one complete ``CodingSession.continue_`` execution."""

        return self._start(
            session,
            lambda lease, run_id: session._continue_with_runtime_lease(
                lease,
                run_id=run_id,
            ),
            parent_run_id=parent_run_id,
        )

    def _start(
        self,
        session: _CodingSessionDriver,
        stream_factory: Callable[[object, str], AsyncIterator[CodingSessionEvent]],
        *,
        parent_run_id: str | None,
    ) -> CodingRunHandle:
        active = self._active_handle()
        if active is not None:
            raise RunConflictError(f"Run {active.run_id} is already active")

        run_id = uuid4().hex
        try:
            lease = session._claim_runtime_lease(run_id)
        except RuntimeError as exc:
            raise RunConflictError(str(exc)) from exc
        try:
            handle = _LocalCodingRunHandle(
                run_id=run_id,
                session=session,
                lease=lease,
                stream_factory=lambda: stream_factory(lease, run_id),
                parent_run_id=parent_run_id,
            )
            session._bind_runtime_callbacks(
                lease,
                cancel=handle.cancel,
                close=handle.aclose,
                publish=handle._publish_control_event,
                accepting_controls=handle._controls_are_open,
                cancellation_requested=handle._cancellation_was_requested,
                control_started=handle._control_started,
                control_finished=handle._control_finished,
            )
            self._runs[run_id] = handle
            self._active_run_id = run_id
            task = handle._launch()
            task.add_done_callback(lambda finished: self._on_run_done(handle, finished))
            return handle
        except Exception:
            session._release_runtime_lease(lease)
            raise

    def inspect(self, run_id: str) -> RunSnapshot:
        return self._handle(run_id).snapshot

    def get(self, run_id: str) -> CodingRunHandle:
        """Return a retained handle for replay or result lookup."""

        return self._handle(run_id)

    def cancel(self, run_id: str) -> RunSnapshot:
        return self._handle(run_id).cancel()

    async def steer(self, run_id: str, message: str) -> None:
        await self._handle(run_id).steer(message)

    async def follow_up(self, run_id: str, message: str) -> None:
        await self._handle(run_id).follow_up(message)

    async def shutdown(self) -> None:
        """Force-close the active run, if any, and release session ownership."""

        active = self._active_handle()
        if active is not None:
            await active.aclose()
            if active.snapshot.is_terminal and self._active_run_id == active.run_id:
                self._active_run_id = None

    def _handle(self, run_id: str) -> _LocalCodingRunHandle:
        try:
            return self._runs[run_id]
        except KeyError as exc:
            raise RunNotFoundError(run_id) from exc

    def _active_handle(self) -> _LocalCodingRunHandle | None:
        if self._active_run_id is None:
            return None
        handle = self._runs[self._active_run_id]
        if handle.snapshot.is_terminal:
            self._active_run_id = None
            return None
        return handle

    def _on_run_done(
        self,
        handle: _LocalCodingRunHandle,
        task: asyncio.Task[None],
    ) -> None:
        handle._settle_from_finished_task(task)
        if self._active_run_id == handle.run_id:
            self._active_run_id = None
        with suppress(asyncio.CancelledError):
            task.exception()


__all__ = [
    "CodingRunCleanupError",
    "CodingRunEvent",
    "CodingRunHandle",
    "LocalCodingSessionRuntime",
]
