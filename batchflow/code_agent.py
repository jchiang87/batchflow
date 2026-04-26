import os
import asyncio
import json
from pathlib import Path
from smolagents import CodeAgent, Tool, OpenAIServerModel
from .agent import AgentNotification, InterventionActions


_agent_semaphore = asyncio.Semaphore(3)  # At most 3 simultaneous LLMs.


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


class RestartNodeTool(Tool):
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
            "nullable": False,
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

    async def forward(
            self,
            node_id: str,
            reason: str = "",
            actor: str = "smolagents.CodeAgent",
    ):
        await self._interventions.restart_node(node_id, reason, actor)


async def _run_agent(
        agent: CodeAgent,
        notification: AgentNotification,
) -> None:
    async with _agent_semaphore:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            agent.run,
            f"Handle the following notification:\n{notification.to_json()}"
        )


def make_code_agent_callback(interventions: InterventionActions):
    """
    Factory to create a callback function for handling AgentNotifications.
    """
    def my_callback(notification: AgentNotification) -> None:
        restart_tool = RestartNodeTool(interventions)
        my_agent = CodeAgent(
            model=_get_model(),
            tools=[restart_tool],
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
            verbosity_level=0,
        )
        asyncio.create_task(
            _run_agent(my_agent, notification),
            name=f"agent-{notification.node_id}",
        )
    return my_callback
