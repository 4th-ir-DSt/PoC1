"""Storage and utility functions for the application."""

import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from core.logging_config import get_logger
from core.exceptions import StorageException

logger = get_logger(__name__)

# In-memory chat histories per user (no login, user_id required in header)
chat_histories: Dict[str, List[Dict[str, Any]]] = {}

# Greeting detection
GREETINGS = ["hello", "hi", "hey", "good morning", "good afternoon", "good evening"]

def is_greeting(text: str) -> bool:
    """
    Check if the text is a greeting.
    
    Args:
        text: Text to check
        
    Returns:
        True if text is a greeting, False otherwise
    """
    try:
        if not text or not isinstance(text, str):
            return False
        
        normalized_text = text.strip().lower()
        is_greeting_result = normalized_text in GREETINGS
        
        if is_greeting_result:
            logger.debug(f"Detected greeting: '{text}'")
        
        return is_greeting_result
        
    except Exception as e:
        logger.error(f"Error checking if text is greeting: {e}")
        return False

# Casual query detection
CASUAL_QUERIES = [
    "what can you do", "who are you", "help", "what is this", 
    "what do you do", "how can you help", "your capabilities"
]

def is_casual_query(text: str) -> bool:
    """
    Check if the text is a casual query.
    
    Args:
        text: Text to check
        
    Returns:
        True if text is a casual query, False otherwise
    """
    try:
        if not text or not isinstance(text, str):
            return False
        
        normalized_text = text.strip().lower()
        is_casual_result = normalized_text in CASUAL_QUERIES
        
        if is_casual_result:
            logger.debug(f"Detected casual query: '{text}'")
        
        return is_casual_result
        
    except Exception as e:
        logger.error(f"Error checking if text is casual query: {e}")
        return False

def get_utc_timestamp() -> str:
    """
    Get current UTC timestamp in ISO format.
    
    Returns:
        ISO format UTC timestamp string
    """
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        return timestamp
    except Exception as e:
        logger.error(f"Error generating UTC timestamp: {e}")
        # Fallback to simple timestamp
        return str(int(time.time()))

def get_chat_history(user_id: str) -> List[Dict[str, Any]]:
    """
    Get chat history for a specific user.
    
    Args:
        user_id: User identifier
        
    Returns:
        List of chat messages for the user
    """
    try:
        if not user_id:
            logger.warning("Empty user_id provided to get_chat_history")
            return []
        
        history = chat_histories.get(user_id, [])
        logger.debug(f"Retrieved {len(history)} messages for user {user_id}")
        return history
        
    except Exception as e:
        logger.error(f"Error retrieving chat history for user {user_id}: {e}")
        return []

def save_chat_history(user_id: str, history: List[Dict[str, Any]]) -> None:
    """
    Save chat history for a specific user.
    
    Args:
        user_id: User identifier
        history: List of chat messages to save
        
    Raises:
        StorageException: If saving fails
    """
    try:
        if not user_id:
            raise StorageException("User ID cannot be empty")
        
        if not isinstance(history, list):
            raise StorageException("History must be a list")
        
        chat_histories[user_id] = history
        logger.debug(f"Saved {len(history)} messages for user {user_id}")
        
    except Exception as e:
        logger.error(f"Error saving chat history for user {user_id}: {e}")
        if isinstance(e, StorageException):
            raise
        raise StorageException(f"Failed to save chat history: {str(e)}") from e

def add_message_to_history(user_id: str, message: Dict[str, Any]) -> None:
    """
    Add a single message to user's chat history.
    
    Args:
        user_id: User identifier
        message: Message to add
        
    Raises:
        StorageException: If adding message fails
    """
    try:
        if not user_id:
            raise StorageException("User ID cannot be empty")
        
        if not isinstance(message, dict):
            raise StorageException("Message must be a dictionary")
        
        # Get or create history for user
        history = chat_histories.setdefault(user_id, [])
        history.append(message)
        
        logger.debug(f"Added message to history for user {user_id}. Total messages: {len(history)}")
        
    except Exception as e:
        logger.error(f"Error adding message to history for user {user_id}: {e}")
        if isinstance(e, StorageException):
            raise
        raise StorageException(f"Failed to add message to history: {str(e)}") from e

def clear_chat_history(user_id: str) -> None:
    """
    Clear chat history for a specific user.
    
    Args:
        user_id: User identifier
    """
    try:
        if not user_id:
            logger.warning("Empty user_id provided to clear_chat_history")
            return
        
        if user_id in chat_histories:
            del chat_histories[user_id]
            logger.info(f"Cleared chat history for user {user_id}")
        else:
            logger.debug(f"No chat history found for user {user_id}")
            
    except Exception as e:
        logger.error(f"Error clearing chat history for user {user_id}: {e}")

def get_total_users() -> int:
    """
    Get total number of users with chat history.
    
    Returns:
        Number of users
    """
    try:
        count = len(chat_histories)
        logger.debug(f"Total users with chat history: {count}")
        return count
    except Exception as e:
        logger.error(f"Error getting total users: {e}")
        return 0

def get_total_messages() -> int:
    """
    Get total number of messages across all users.
    
    Returns:
        Total number of messages
    """
    try:
        total = sum(len(history) for history in chat_histories.values())
        logger.debug(f"Total messages across all users: {total}")
        return total
    except Exception as e:
        logger.error(f"Error getting total messages: {e}")
        return 0 