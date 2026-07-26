"""Pi-compatible JSON event stream renderer."""

import typer

from tau_agent.events import MessageEndEvent
from tau_agent.messages import AssistantMessage
from tau_coding.events import CodingSessionEvent, SessionAgentEndEvent


class JsonEventRenderer:
    def __init__(self) -> None:
        self._failed = False

    def render(self, event: CodingSessionEvent) -> None:
        if isinstance(event, MessageEndEvent) and isinstance(event.message, AssistantMessage):
            self._failed = event.message.stop_reason == "error"
        elif isinstance(event, SessionAgentEndEvent) and event.will_retry:
            self._failed = False
        typer.echo(event.model_dump_json(by_alias=True, exclude_none=True))

    def finish(self) -> bool:
        return not self._failed
