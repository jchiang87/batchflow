import os
import asyncio
import json
from pathlib import Path
import logging
from smolagents import CodeAgent, Tool, OpenAIServerModel
from .agent import AgentNotification, InterventionActions

log = logging.getLogger(__name__)


def _get_model() -> OpenAIServerModel:
    settings = json.loads(
        (Path("~/.claude/settings.json").expanduser()).read_text()
    )
    env = settings["env"]
    default_model_id = os.environ.get("ANTHROPIC_DEFAULT_MODEL",
                                      "claude-sonnet-4-6")
    model_id = env.get("ANTHROPIC_DEFAULT_SONNET_MODEL", default_model_id)
    return OpenAIServerModel(
        model_id=model_id,
        api_base=env["ANTHROPIC_BASE_URL"],
        api_key=env["ANTHROPIC_AUTH_TOKEN"],
    )


class BatchflowTool(Tool):
    """Base class for batchflow smolagents tools. Handles event loop injection."""

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop


class RestartNodeTool(BatchflowTool):
    name = "restart_node"
    description = (
        "Restart a node from the workflow graph.  This restarts "
        "the node cluster from where it left off.  This is the "
        "appropriate intervention for handling transient infrastructure "
        "failures."
    )
    inputs = {
        "node_id": {
            "type": "string",
            "description": ("The ID of the node in the workflow graph, "
                            "e.g., 'stage_1a'."),
            "nullable": True,
        },
        "reason": {
            "type": "string",
            "description": ("The reason for the restart intervention, e.g., "
                            "'transient disk failure', etc.."),
            "nullable": True,
        },
        "actor": {
            "type": "string",
            "description": "The actor who issued this intervention.",
            "nullable": True,
        },
    }
    output_type = "null"

    def __init__(self, interventions: InterventionActions):
        super().__init__()
        self._interventions = interventions
        self._loop: asyncio.AbstractEventLoop | None = None

    def forward(
            self,
            node_id: str = "",
            reason: str = "",
            actor: str = "smolagents.CodeAgent",
    ):
        log.debug("restart_tool.forward: node_id=%r", node_id)
        future = asyncio.run_coroutine_threadsafe(
            self._interventions.restart_node(node_id, reason, actor),
            self._loop,
        )
        log.debug("restart_tool.forward: submitted coroutine, awaiting result")
        future.result()
        log.debug("restart_tool.forward: done")


class CodeAgentRunner:
    """
    Drives smolagents tools in response to AgentNotifications.

    Owns the concurrency semaphore and injects the event loop into tools
    at first call time.  Pass ``runner.run`` to ``CallbackTransport``.

    Parameters
    ----------
    tools : list[BatchflowTool]
        Tools to make available.  Each must be fully constructed (with its
        dependencies) before being passed here.
    max_concurrent : int
        Maximum number of tool invocations running simultaneously.
    use_agent : bool
        If True (default), route notifications through a ``smolagents.CodeAgent``
        that decides which tool to call.  If False, call ``RestartNodeTool``
        directly — useful for testing without a live model.
    agent_timeout : float | None
        Seconds to wait for the agent to finish before cancelling it and
        logging a timeout error.  None means no limit.
    """

    def __init__(self, tools: list[BatchflowTool], max_concurrent: int = 3,
                 use_agent: bool = True, agent_timeout: float | None = 120.0):
        self._tools = tools
        self._max_concurrent = max_concurrent
        self._use_agent = use_agent
        self._agent_timeout = agent_timeout
        self._semaphore: asyncio.Semaphore | None = None

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrent)
        return self._semaphore

    def _build_agent(self) -> CodeAgent:
        return CodeAgent(
            tools=self._tools,
            model=_get_model(),
            additional_authorized_imports=["json"],
            name="smolagents_CodeAgent",
            description="Handles blocked nodes in the workflow graph.",
            instructions=(
                "Based on the notification.hold_reasons and the "
                "notification.classification, perform specific "
                "interventions to recover the workflow.  For now, "
                "handle all interventions the same way: running the "
                "restart_node tool."
            ),
            verbosity_level=1,
        )

    async def run(self, notification: AgentNotification) -> None:
        log.debug("CodeAgentRunner.run: node_id=%r", notification.node_id)
        if notification.node_id == "__workflow__":
            log.debug("CodeAgentRunner: skipping workflow-level notification %r",
                      notification.event_type)
            return
        loop = asyncio.get_running_loop()
        for tool in self._tools:
            tool.set_loop(loop)
        os.environ["IGNORE_SIGNAL"] = "True"

        async def _run_and_catch():
            try:
                async with self._get_semaphore():
                    if self._use_agent:
                        agent = self._build_agent()
                        await asyncio.wait_for(
                            asyncio.to_thread(
                                agent.run,
                                f"Please handle {notification.to_json()}",
                            ),
                            timeout=self._agent_timeout,
                        )
                    else:
                        restart_tool = next(
                            (t for t in self._tools if isinstance(t, RestartNodeTool)),
                            None,
                        )
                        if restart_tool is None:
                            log.error("CodeAgentRunner: no RestartNodeTool registered")
                            return
                        await asyncio.to_thread(restart_tool.forward, notification.node_id)
            except asyncio.TimeoutError:
                log.error(
                    "CodeAgentRunner timed out after %.0fs for node %r",
                    self._agent_timeout, notification.node_id,
                )
            except Exception as exc:
                log.error("CodeAgentRunner restart failed: %s", exc, exc_info=exc)

        asyncio.create_task(_run_and_catch(), name=f"agent-{notification.node_id}")


def make_code_agent_callback(interventions: InterventionActions):
    """Convenience shim: returns a CodeAgentRunner.run bound to interventions."""
    use_agent = not "DONT_USE_AGENT" in os.environ
    return CodeAgentRunner([RestartNodeTool(interventions)], use_agent=use_agent).run
