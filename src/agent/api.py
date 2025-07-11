# from fastapi import FastAPI
# from pydantic import BaseModel, Field
# from typing import List, Dict, Any, Literal
# from src.agent.main import generate_logical_data
# from src.prompts.main import SYSTEM_PROMPT

# app = FastAPI(title="Logical Data Modeling Assistant API", description="Generate and iteratively refine logical data models via chat.")

# # In-memory chat history (single session, not for production)
# chat_history: List[Dict[str, Any]] = []

# class Message(BaseModel):
#     """A single message in the chat between user and assistant."""
#     role: Literal["user", "assistant"] = Field(..., description="The role of the message sender: 'user' or 'assistant'.")
#     content: Any = Field(..., description="The message content. For 'user', this is a string. For 'assistant', this is the logical data model as a JSON object.")

# class QueryRequest(BaseModel):
#     """Request body for the /model-chat endpoint. Only the user's query is required."""
#     query: str = Field(..., description="The user's request or instruction for the data modeling assistant.")

# class QueryResponse(BaseModel):
#     """Response body for the /model-chat endpoint, containing the user query and the assistant's response."""
#     messages: List[Message] = Field(..., description="The user query and the assistant's response (logical data model).")

# @app.post("/model-chat", response_model=QueryResponse, summary="Chat with the logical data modeling assistant", tags=["Model Chat"])
# def model_chat(request: QueryRequest) -> QueryResponse:
#     """
#     Generate a logical data model based on a single user query.
#     The assistant will always return the full logical data model as a JSON object.
#     The response contains only the user query and the assistant's response (no system prompt).
#     """
#     global chat_history
#     # Build the chat with system prompt and user query
#     messages = [
#         {"role": "system", "content": SYSTEM_PROMPT},
#         {"role": "user", "content": request.query}
#     ]
#     generate_logical_data(messages, request.query)
#     # Only return user and assistant messages (omit system prompt)
#     filtered_messages = [m for m in messages if m["role"] != "system"]
#     # Append to in-memory chat history
#     chat_history.extend(filtered_messages)
#     return QueryResponse(messages=filtered_messages)

# @app.post("/model-chat/reset", summary="Reset the chat history", tags=["Model Chat"])
# def reset_chat() -> Dict[str, str]:
#     """
#     Reset (clear) the in-memory chat history.
#     """
#     global chat_history
#     chat_history.clear()
#     return {"message": "Chat history has been reset."}

# @app.get("/model-chat/history", response_model=QueryResponse, summary="Get the current chat history", tags=["Model Chat"])
# def get_chat_history() -> QueryResponse:
#     """
#     Retrieve the current in-memory chat history (user and assistant messages only).
#     """
#     return QueryResponse(messages=chat_history) 