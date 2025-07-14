"""Model chat router for logical data modeling assistant."""

import time
from fastapi import APIRouter, Header, HTTPException, Request
from typing import List, Dict, Any, Optional
from schemas.api import Message, QueryRequest, QueryResponse
from core.config import settings
from core.storage import (
    get_chat_history, save_chat_history, add_message_to_history, 
    clear_chat_history, get_utc_timestamp, is_greeting, is_casual_query
)
from core.data_model_service import generate_logical_data, order_chat_history, extract_json_from_string
from core.prompts.main import SYSTEM_PROMPT
from core.logging_config import get_logger
from core.exceptions import LLMServiceException, ValidationException, StorageException
import json
from core.tools import handle_tool_call

logger = get_logger(__name__)
router = APIRouter(tags=["Model Chat"])

@router.post("/model-chat", response_model=QueryResponse, summary="Chat with the logical data modeling assistant")
async def model_chat(
    request: QueryRequest, 
    user_id: Optional[str] = Header(settings.default_user_id, include_in_schema=False),
    http_request: Request = None
) -> QueryResponse:
    """
    Main chat endpoint for logical data modeling assistant.
    
    Args:
        request: User's query request
        user_id: User identifier (from header)
        http_request: FastAPI request object for logging
        
    Returns:
        QueryResponse with conversation messages
        
    Raises:
        HTTPException: For various error conditions
    """
    start_time = time.time()
    logger.info(f"Chat request from user {user_id}: {request.query[:100]}...")
    
    try:
        # Input validation
        if not request.query or not request.query.strip():
            raise ValidationException("Query cannot be empty")
        
        if len(request.query) > 10000:  # 10KB limit
            raise ValidationException("Query too long (max 10KB)")
        
        # Handle greetings
        if is_greeting(request.query):
            logger.debug(f"Handling greeting from user {user_id}")
            response = QueryResponse(messages=[
                Message(role="user", content=request.query, timestamp=get_utc_timestamp()),
                Message(role="assistant", content="Hello! How can I help you with your data modeling today?", timestamp=get_utc_timestamp())
            ])
            processing_time = time.time() - start_time
            logger.info(f"Greeting handled in {processing_time:.3f}s for user {user_id}")
            return response
        
        # Handle casual queries
        if is_casual_query(request.query):
            logger.debug(f"Handling casual query from user {user_id}")
            response = QueryResponse(messages=[
                Message(role="user", content=request.query, timestamp=get_utc_timestamp()),
                Message(
                    role="assistant",
                    content="I'm a logical data modeling assistant. I can help you design, refine, and explain logical data models for your business or software needs. Just describe your requirements or ask for a data model, and I'll generate one for you!",
                    timestamp=get_utc_timestamp()
                )
            ])
            processing_time = time.time() - start_time
            logger.info(f"Casual query handled in {processing_time:.3f}s for user {user_id}")
            return response
        
        # Get or create chat history for this user
        history = get_chat_history(user_id)
        
        # Add the new user message to the history
        user_message = {"role": "user", "content": request.query, "timestamp": get_utc_timestamp()}
        add_message_to_history(user_id, user_message)
        
        # Prepare messages for the LLM (system prompt + full history)
        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        for m in history:
            if m["role"] == "assistant" and isinstance(m["content"], dict):
                # Convert dict to JSON string for LLM
                messages.append({"role": m["role"], "content": json.dumps(m["content"])})
            else:
                messages.append({"role": m["role"], "content": m["content"]})
        
        # Add current user message
        messages.append({"role": "user", "content": request.query})
        
        logger.debug(f"Prepared {len(messages)} messages for LLM (user {user_id})")
        
        # Generate assistant response
        response = generate_logical_data(messages, request.query)
        logger.info(f"Raw LLM response: {response}")
        # Check for tool call and handle if present
        tool_result = handle_tool_call(response)
        if tool_result != response:
            processed_response = tool_result
        else:
            # Post-process: ensure response is a JSON object if possible
            processed_response = extract_json_from_string(response)
        
        # Add the assistant's response to the history
        assistant_message = {"role": "assistant", "content": processed_response, "timestamp": get_utc_timestamp()}
        add_message_to_history(user_id, assistant_message)
        
        # Get updated history and order it
        updated_history = get_chat_history(user_id)
        ordered_history = order_chat_history(updated_history)
        
        # Convert history to Message objects with timestamps
        response_messages = [Message(**msg) for msg in ordered_history]
        
        processing_time = time.time() - start_time
        logger.info(f"Chat completed in {processing_time:.3f}s for user {user_id}. Messages: {len(response_messages)}")
        
        return QueryResponse(messages=response_messages)
        
    except ValidationException as e:
        logger.warning(f"Validation error for user {user_id}: {e}")
        raise HTTPException(status_code=400, detail=str(e))
        
    except LLMServiceException as e:
        logger.error(f"LLM service error for user {user_id}: {e}")
        raise HTTPException(status_code=503, detail="Service temporarily unavailable")
        
    except StorageException as e:
        logger.error(f"Storage error for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Unexpected error for user {user_id} after {processing_time:.3f}s: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/model-chat/reset", summary="Reset the chat history")
async def reset_chat(
    user_id: Optional[str] = Header(settings.default_user_id, include_in_schema=False)
) -> Dict[str, str]:
    """
    Reset chat history for a user.
    
    Args:
        user_id: User identifier (from header)
        
    Returns:
        Confirmation message
    """
    try:
        logger.info(f"Resetting chat history for user {user_id}")
        clear_chat_history(user_id)
        return {"message": "Chat history has been reset."}
        
    except Exception as e:
        logger.error(f"Error resetting chat history for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to reset chat history")

@router.get("/model-chat/history", response_model=QueryResponse, summary="Get the current chat history")
async def get_chat_history_endpoint(
    user_id: Optional[str] = Header(settings.default_user_id, include_in_schema=False)
) -> QueryResponse:
    """
    Get chat history for a user.
    
    Args:
        user_id: User identifier (from header)
        
    Returns:
        QueryResponse with chat history
    """
    try:
        logger.debug(f"Retrieving chat history for user {user_id}")
        history = get_chat_history(user_id)
        ordered_history = order_chat_history(history)
        
        # Convert history to Message objects with timestamps
        response_messages = [Message(**msg) for msg in ordered_history]
        
        logger.debug(f"Retrieved {len(response_messages)} messages for user {user_id}")
        return QueryResponse(messages=response_messages)
        
    except Exception as e:
        logger.error(f"Error retrieving chat history for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve chat history") 