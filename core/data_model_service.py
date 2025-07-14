"""Model service for logical data generation."""

import json
import re
import time
from typing import List, Dict, Any, Optional
from openai import Client
from openai.types.chat import ChatCompletion

from core.config import settings
from core.prompts.main import SYSTEM_PROMPT
from core.exceptions import LLMServiceException, ValidationException
from core.logging_config import get_logger

logger = get_logger(__name__)

def extract_json_from_string(s: str) -> Any:
    """
    Extract JSON from a string, handling code blocks and plain JSON.
    
    Args:
        s: Input string that may contain JSON
        
    Returns:
        Parsed JSON object or original string if not JSON
    """
    try:
        # Try to extract JSON from a code block
        match = re.search(r"```(?:json)?\n(.*?)```", s, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            logger.debug(f"Extracting JSON from code block: {json_str[:100]}...")
            return json.loads(json_str)
        
        # Try to parse the whole string as JSON
        logger.debug(f"Attempting to parse entire string as JSON: {s[:100]}...")
        return json.loads(s)
        
    except json.JSONDecodeError as e:
        logger.debug(f"JSON parsing failed: {e}. Returning original string.")
        return s
    except Exception as e:
        logger.error(f"Unexpected error in JSON extraction: {e}")
        return s

def generate_logical_data(messages: List[Dict[str, Any]], query: str) -> str:
    """
    Generate logical data using the LLM.
    
    Args:
        messages: Conversation history for context
        query: Current user query
        
    Returns:
        LLM response as string
        
    Raises:
        LLMServiceException: If LLM service fails
        ValidationException: If input validation fails
    """
    start_time = time.time()
    logger.info(f"Generating logical data for query: {query[:100]}...")
    
    try:
        # Input validation
        if not messages:
            raise ValidationException("Messages list cannot be empty")
        
        if not query or not query.strip():
            raise ValidationException("Query cannot be empty")
        
        # Create client only when needed (lazy initialization)
        client = Client(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key,
            timeout=settings.llm_timeout
        )
        
        logger.debug(f"LLM request - Model: {settings.llm_model}, Max tokens: {settings.llm_max_tokens}")
        
        # Call LLM API
        chat_completion: ChatCompletion = client.beta.chat.completions.create(
            model=settings.llm_model,
            messages=messages,
            max_tokens=settings.llm_max_tokens
        )
        
        response = chat_completion.choices[0].message.content
        
        if not response:
            raise LLMServiceException("Empty response from LLM service")
        
        processing_time = time.time() - start_time
        logger.info(f"LLM response generated in {processing_time:.2f}s. Response length: {len(response)}")
        logger.debug(f"LLM response preview: {response[:200]}...")
        
        return response
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"LLM service failed after {processing_time:.2f}s: {str(e)}")
        
        if isinstance(e, (LLMServiceException, ValidationException)):
            raise
        
        # Wrap unexpected errors
        raise LLMServiceException(f"LLM service error: {str(e)}") from e

def order_chat_history(history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Order chat history with most recent user+assistant pair first.
    
    Args:
        history: List of chat messages
        
    Returns:
        Ordered list of messages
    """
    try:
        if not history:
            logger.debug("Empty history provided, returning empty list")
            return []
        
        # Group into pairs: [user, assistant]
        pairs = []
        i = 0
        while i < len(history):
            if i + 1 < len(history) and history[i]["role"] == "user" and history[i+1]["role"] == "assistant":
                pairs.append([history[i], history[i+1]])
                i += 2
            else:
                pairs.append([history[i]])
                i += 1
        
        # Reverse pairs if more than one user query
        if len(pairs) > 1:
            pairs = list(reversed(pairs))
        
        # Flatten back to a single list
        ordered = [msg for pair in pairs for msg in pair]
        
        logger.debug(f"Ordered {len(history)} messages into {len(ordered)} messages")
        return ordered
        
    except Exception as e:
        logger.error(f"Error ordering chat history: {e}")
        # Return original history if ordering fails
        return history

def validate_message_structure(message: Dict[str, Any]) -> bool:
    """
    Validate that a message has the required structure.
    
    Args:
        message: Message dictionary to validate
        
    Returns:
        True if valid, False otherwise
    """
    required_fields = ["role", "content"]
    
    if not isinstance(message, dict):
        logger.warning("Message is not a dictionary")
        return False
    
    for field in required_fields:
        if field not in message:
            logger.warning(f"Message missing required field: {field}")
            return False
    
    if message["role"] not in ["user", "assistant"]:
        logger.warning(f"Invalid message role: {message['role']}")
        return False
    
    return True 