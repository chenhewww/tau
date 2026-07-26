"""Stateful reusable agent harness built on the Pi-compatible loop."""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from inspect import isawaitable
from typing import Literal

from tau_agent.events import AgentEndEvent, AgentEvent
from tau_agent.loop import AfterToolCall, BeforeToolCall, run_agent_loop
from tau_agent.messages import (
    AgentMessage,
    AssistantMessage,
    TextContent,
    ToolResultMessage,
    UserMessage,
)
from tau_agent.provider import ModelProvider
from tau_agent.tools import AgentTool

EventListener = Callable[[AgentEvent], Awaitable[None] | None]
QueueMode = Literal["one_at_a_time", "all"]


@dataclass(frozen=True, slots=True)
class QueuedMessages:
    steering: tuple[AgentMessage, ...] = ()
    follow_up: tuple[AgentMessage, ...] = ()

    @property
    def count(self) -> int:
        return len(self.steering) + len(self.follow_up)


@dataclass(slots=True)
class AgentHarnessConfig:
    provider: ModelProvider
    model: str
    system: str
    tools: list[AgentTool] = field(default_factory=list)
    max_turns: int | None = None
    queue_mode: QueueMode = "one_at_a_time"
    before_tool_call: BeforeToolCall | None = None
    after_tool_call: AfterToolCall | None = None


class SimpleCancellationToken:
    def __init__(self) -> None:
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def is_cancelled(self) -> bool:
        return self._cancelled


class AgentHarness:
    """Reusable stateful agent brain independent of coding/UI policy."""

    def __init__(
        self,
        config: AgentHarnessConfig,
        *,
        messages: Sequence[AgentMessage] = (),
    ) -> None:
        self._config = config
        self._messages = list(messages)
        self._listeners: list[EventListener] = []
        self._current_signal: SimpleCancellationToken | None = None
        self._running = False
        self._accepting_controls = False
        self._runtime_lease: object | None = None
        self._steering_queue: deque[AgentMessage] = deque()
        self._follow_up_queue: deque[AgentMessage] = deque()

    @property
    def messages(self) -> tuple[AgentMessage, ...]:
        return tuple(self._messages)

    @property
    def config(self) -> AgentHarnessConfig:
        return self._config

    @property
    def is_running(self) -> bool:
        return self._running or self._runtime_lease is not None

    @property
    def accepting_controls(self) -> bool:
        """Return whether an active run can still consume queued controls."""

        return self._accepting_controls

    @property
    def queued_messages(self) -> QueuedMessages:
        return QueuedMessages(tuple(self._steering_queue), tuple(self._follow_up_queue))

    @property
    def pending_message_count(self) -> int:
        return self.queued_messages.count

    def has_queued_messages(self) -> bool:
        return bool(self._steering_queue or self._follow_up_queue)

    def append_message(self, message: AgentMessage) -> None:
        self._ensure_not_runtime_owned()
        self._messages.append(message)

    def replace_messages(self, messages: Sequence[AgentMessage]) -> None:
        self._ensure_not_runtime_owned()
        self._messages = list(messages)

    def subscribe(self, listener: EventListener) -> Callable[[], None]:
        self._listeners.append(listener)

        def unsubscribe() -> None:
            with suppress(ValueError):
                self._listeners.remove(listener)

        return unsubscribe

    def cancel(self) -> None:
        self._ensure_not_runtime_owned()
        self._cancel()

    def _cancel(self) -> None:
        if self._current_signal is not None:
            self._current_signal.cancel()

    def steer(self, content: str) -> QueuedMessages:
        return self.steer_message(UserMessage(content=content))

    def steer_message(self, message: AgentMessage) -> QueuedMessages:
        self._ensure_not_runtime_owned()
        self._ensure_accepting_controls()
        return self._steer_message(message)

    def _steer_message(self, message: AgentMessage) -> QueuedMessages:
        self._steering_queue.append(message)
        return self.queued_messages

    def follow_up(self, content: str) -> QueuedMessages:
        return self.follow_up_message(UserMessage(content=content))

    def follow_up_message(self, message: AgentMessage) -> QueuedMessages:
        self._ensure_not_runtime_owned()
        self._ensure_accepting_controls()
        return self._follow_up_message(message)

    def _follow_up_message(self, message: AgentMessage) -> QueuedMessages:
        self._follow_up_queue.append(message)
        return self.queued_messages

    def clear_queues(self) -> QueuedMessages:
        self._ensure_not_runtime_owned()
        return self._clear_queues()

    def _clear_queues(self) -> QueuedMessages:
        snapshot = self.queued_messages
        self._steering_queue.clear()
        self._follow_up_queue.clear()
        return snapshot

    def pop_latest_follow_up(self) -> AgentMessage | None:
        self._ensure_not_runtime_owned()
        return self._follow_up_queue.pop() if self._follow_up_queue else None

    def pop_latest_steering(self) -> AgentMessage | None:
        self._ensure_not_runtime_owned()
        return self._steering_queue.pop() if self._steering_queue else None

    def prompt_message(self, message: AgentMessage) -> AsyncIterator[AgentEvent]:
        self._ensure_not_running()
        return self._start_run(prompts=(message,))

    def _prompt_with_runtime_lease(
        self,
        content: str,
        lease: object,
    ) -> AsyncIterator[AgentEvent]:
        self._ensure_runtime_lease(lease)
        return self._start_run(prompts=(UserMessage(content=content),))

    def _start_run(
        self,
        *,
        prompts: Sequence[AgentMessage] = (),
    ) -> AsyncIterator[AgentEvent]:
        if self._running:
            raise RuntimeError(
                "AgentHarness is already running; use steer() or follow_up() to queue messages."
            )
        self._append_interrupted_tool_results()
        self._running = True
        self._accepting_controls = True
        return self._run(prompts=prompts)

    def prompt(self, content: str) -> AsyncIterator[AgentEvent]:
        return self.prompt_message(UserMessage(content=content))

    def continue_(self) -> AsyncIterator[AgentEvent]:
        self._ensure_not_running()
        return self._start_run()

    def _continue_with_runtime_lease(self, lease: object) -> AsyncIterator[AgentEvent]:
        self._ensure_runtime_lease(lease)
        return self._start_run()

    async def _run(
        self,
        *,
        prompts: Sequence[AgentMessage] = (),
    ) -> AsyncIterator[AgentEvent]:
        signal = SimpleCancellationToken()
        self._current_signal = signal
        try:
            async for event in run_agent_loop(
                provider=self._config.provider,
                model=self._config.model,
                system=self._config.system,
                messages=self._messages,
                prompts=prompts,
                tools=self._config.tools,
                max_turns=self._config.max_turns,
                signal=signal,
                get_steering_messages=self._drain_steering_messages,
                get_follow_up_messages=self._drain_follow_up_messages,
                before_tool_call=self._config.before_tool_call,
                after_tool_call=self._config.after_tool_call,
            ):
                if isinstance(event, AgentEndEvent):
                    self._accepting_controls = False
                await self._notify(event)
                yield event
        finally:
            if signal.is_cancelled():
                self._append_interrupted_tool_results()
            if self._current_signal is signal:
                self._current_signal = None
            self._accepting_controls = False
            self._running = False

    async def _notify(self, event: AgentEvent) -> None:
        for listener in list(self._listeners):
            result = listener(event)
            if isawaitable(result):
                await result

    def _ensure_not_running(self) -> None:
        if self._runtime_lease is not None:
            raise RuntimeError("AgentHarness is reserved by LocalAgentRuntime")
        if self._running:
            raise RuntimeError(
                "AgentHarness is already running; use steer() or follow_up() to queue messages."
            )

    def _ensure_not_runtime_owned(self) -> None:
        if self._runtime_lease is not None:
            raise RuntimeError("AgentHarness is owned by LocalAgentRuntime; use its run handle")

    def _ensure_accepting_controls(self) -> None:
        if self._running and not self._accepting_controls:
            raise RuntimeError("AgentHarness is settling and no longer accepts controls")

    def _claim_runtime_lease(self) -> object:
        self._ensure_not_running()
        lease = object()
        self._runtime_lease = lease
        return lease

    def _ensure_runtime_lease(self, lease: object) -> None:
        if self._runtime_lease is not lease:
            raise RuntimeError("AgentHarness runtime lease is not held by this run")

    def _release_runtime_lease(self, lease: object) -> None:
        self._ensure_runtime_lease(lease)
        if self._running:
            raise RuntimeError("Cannot release an AgentHarness lease while it is running")
        self._runtime_lease = None

    def _cancel_with_runtime_lease(self, lease: object) -> None:
        self._ensure_runtime_lease(lease)
        self._cancel()

    def _steer_with_runtime_lease(self, content: str, lease: object) -> None:
        self._ensure_runtime_lease(lease)
        self._ensure_accepting_controls()
        self._steer_message(UserMessage(content=content))

    def _follow_up_with_runtime_lease(self, content: str, lease: object) -> None:
        self._ensure_runtime_lease(lease)
        self._ensure_accepting_controls()
        self._follow_up_message(UserMessage(content=content))

    def _clear_queues_with_runtime_lease(self, lease: object) -> QueuedMessages:
        self._ensure_runtime_lease(lease)
        return self._clear_queues()

    def _repair_with_runtime_lease(self, lease: object) -> int:
        self._ensure_runtime_lease(lease)
        before = len(self._messages)
        self._append_interrupted_tool_results()
        return len(self._messages) - before

    def _drain_steering_messages(self) -> tuple[AgentMessage, ...]:
        return self._drain_queue(self._steering_queue)

    def _drain_follow_up_messages(self) -> tuple[AgentMessage, ...]:
        return self._drain_queue(self._follow_up_queue)

    def _drain_queue(self, queue: deque[AgentMessage]) -> tuple[AgentMessage, ...]:
        if not queue:
            return ()
        if self._config.queue_mode == "all":
            messages = tuple(queue)
            queue.clear()
            return messages
        return (queue.popleft(),)

    def append_interrupted_tool_results(self) -> int:
        self._ensure_not_runtime_owned()
        before = len(self._messages)
        self._append_interrupted_tool_results()
        return len(self._messages) - before

    def _append_interrupted_tool_results(self) -> None:
        returned_ids = {
            message.tool_call_id
            for message in self._messages
            if isinstance(message, ToolResultMessage)
        }
        for message in tuple(self._messages):
            if not isinstance(message, AssistantMessage):
                continue
            for call in message.tool_calls:
                if call.id in returned_ids:
                    continue
                returned_ids.add(call.id)
                self._messages.append(
                    ToolResultMessage(
                        tool_call_id=call.id,
                        tool_name=call.name,
                        content=[TextContent(text="Tool call interrupted by user")],
                        is_error=True,
                    )
                )
