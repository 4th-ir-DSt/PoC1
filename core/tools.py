from datetime import datetime
from core.data_model_service import generate_logical_data, extract_json_from_string
from core.config import settings
from openai import Client
from core.logging_config import get_logger

logger = get_logger(__name__)

# Example tool functions
def get_current_time():
    return {"current_time": datetime.utcnow().isoformat()}

def echo(text: str):
    return {"echo": text}

def tool_generate_logical_data(messages, query):
    return {"result": generate_logical_data(messages, query)}

def tool_extract_json_from_string(s):
    return {"result": extract_json_from_string(s)}

def classify_intent(query: str) -> dict:
    """
    Use the LLM to classify the user's intent as 'MODEL', 'CLARIFY', 'CONVO', or 'UNKNOWN'.
    """
    client = Client(
        base_url=settings.llm_base_url,
        api_key=settings.llm_api_key,
        timeout=settings.llm_timeout
    )
    system_prompt = (
        "You are an intent-classification assistant for a logical data modeling system. "
        "Given a user query, respond ONLY with one of the following intents: "
        "'MODEL' (if the user is requesting a logical data model and you have enough information), "
        "'CLARIFY' (if you need to ask a clarifying question to get more details), "
        "'CONVO' (if the user wants a general conversation), or "
        "'UNKNOWN' (if the intent is unclear or unsupported). "
        "Do not add any extra text. "
        "If you choose 'CLARIFY', you will be prompted to generate a clarifying question in a follow-up step."
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": query}
    ]
    chat_completion = client.beta.chat.completions.create(
        model=settings.llm_model,
        messages=messages,
        max_tokens=10
    )
    response = chat_completion.choices[0].message.content.strip().upper()
    if response not in ("MODEL", "CLARIFY", "CONVO", "UNKNOWN"):
        return {"intent": "UNKNOWN", "raw": response}
    return {"intent": response}

tool_registry = {
    "get_current_time": get_current_time,
    "echo": echo,
    "generate_logical_data": tool_generate_logical_data,
    "extract_json_from_string": tool_extract_json_from_string,
    "classify_intent": classify_intent,
}

def handle_tool_call(response):
    if isinstance(response, dict) and "tool_call" in response:
        tool_info = response["tool_call"]
        tool_name = tool_info["name"]
        tool_args = tool_info.get("args", {})
        logger.info(f"Tool call detected: {tool_name} with args: {tool_args} | Full tool_call: {tool_info}")
        if tool_name in tool_registry:
            try:
                return tool_registry[tool_name](**tool_args)
            except Exception as e:
                logger.error(f"Tool execution failed for {tool_name}: {e}")
                return {"error": f"Tool execution failed: {str(e)}"}
        else:
            logger.error(f"Unknown tool: {tool_name}")
            return {"error": f"Unknown tool: {tool_name}"}
    return response 