import os
import asyncio
import json
from pathlib import Path
from smolagents import CodeAgent, Tool, OpenAIServerModel
from .agent import AgentNotification, InterventionActions


_interventions: InterventionActions | None = None


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

    async def forward(
            self,
            node_id: str,
            reason: str = "",
            actor: str = "smolagents.CodeAgent",
    ):
        await _interventions.restart_node(node_id, reason, actor)


async def code_agent_callback(
        notification: AgentNotification,
        interventions: InterventionActions
) -> None:
    global _interventions
    _interventions = interventions
    my_agent = CodeAgent(
        model=_get_model(),
        tools=[RestartNodeTool()],
        additional_authorized_imports=["json"],
        name="smolagents_CodeAgent",
        description="Handles blocked nodes in the workflow graph.",
        instructions=(
            "Based on the notification.hold_reasons and the "
            "notification.classification, perform specific "
            "interventions to recover the workflow.  For now, handle "
            "all interventions the same way: running the restart_node tool."
        ),
        verbosity_level=0,
    )

    asyncio.create_task(my_agent.run(f"Handle the {notification}"))
