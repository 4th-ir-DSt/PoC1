"""Model-related schemas for the logical data modeling assistant."""

from pydantic import BaseModel, Field
from typing import List, Dict, Any, Literal

class Message(BaseModel):
    """A single message in the chat between user and assistant."""
    role: Literal["user", "assistant"] = Field(..., description="The role of the message sender: 'user' or 'assistant'.")
    content: Any = Field(..., description="The message content. For 'user', this is a string. For 'assistant', this is the logical data model as a JSON object.")
    timestamp: str = Field(..., description="The ISO 8601 UTC timestamp when the message was created.")

class QueryRequest(BaseModel):
    """Request body for the /model-chat endpoint. Only the user's query is required."""
    query: str = Field(..., description="The user's request or instruction for the data modeling assistant.")

class QueryResponse(BaseModel):
    """Response body for the /model-chat endpoint, containing the user query and the assistant's response."""
    messages: List[Message] = Field(..., description="The user query and the assistant's response (logical data model).") 