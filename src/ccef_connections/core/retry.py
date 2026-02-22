"""
Retry logic with exponential backoff for CCEF connections.

This module provides decorators and utilities for retrying failed operations
with intelligent backoff strategies tailored to different API rate limits.
"""

import logging
from typing import Callable, Type, Tuple

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from ..exceptions import RateLimitError, ConnectionError

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_attempts: int = 3,
    min_wait: float = 1.0,
    max_wait: float = 60.0,
    multiplier: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (ConnectionError, RateLimitError),
) -> Callable:
    """
    Decorator for retrying operations with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time in seconds
        max_wait: Maximum wait time in seconds
        multiplier: Exponential backoff multiplier
        exceptions: Tuple of exception types to retry on

    Returns:
        Decorated function with retry logic

    Examples:
        >>> @retry_with_backoff(max_attempts=5, min_wait=0.5)
        ... def fetch_data():
        ...     # Some operation that might fail
        ...     pass
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(exceptions),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


def retry_airtable_operation(func: Callable) -> Callable:
    """
    Decorator for Airtable operations with rate limit handling.

    Airtable has a rate limit of 5 requests per second per base.
    This decorator implements exponential backoff with appropriate timing.

    Args:
        func: The function to decorate

    Returns:
        Decorated function with Airtable-specific retry logic

    Examples:
        >>> @retry_airtable_operation
        ... def update_records(table, records):
        ...     return table.batch_update(records)
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1.5, min=0.2, max=10.0),
        retry=retry_if_exception_type((ConnectionError, RateLimitError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def retry_openai_operation(func: Callable) -> Callable:
    """
    Decorator for OpenAI API operations with rate limit handling.

    Handles 429 (rate limit) errors with exponential backoff.

    Args:
        func: The function to decorate

    Returns:
        Decorated function with OpenAI-specific retry logic

    Examples:
        >>> @retry_openai_operation
        ... def call_openai(prompt):
        ...     return llm.invoke(prompt)
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2.0, min=1.0, max=60.0),
        retry=retry_if_exception_type((ConnectionError, RateLimitError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def retry_google_operation(func: Callable) -> Callable:
    """
    Decorator for Google API operations (Sheets, BigQuery) with retry logic.

    Google APIs have various rate limits depending on the service.
    This implements a conservative retry strategy.

    Args:
        func: The function to decorate

    Returns:
        Decorated function with Google API retry logic

    Examples:
        >>> @retry_google_operation
        ... def query_bigquery(sql):
        ...     return client.query(sql)
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2.0, min=1.0, max=60.0),
        retry=retry_if_exception_type((ConnectionError, RateLimitError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def retry_helpscout_operation(func: Callable) -> Callable:
    """
    Decorator for HelpScout API operations with retry logic.

    HelpScout API has rate limits; this implements exponential backoff
    with 5 attempts, matching the pattern used by OpenAI/Google decorators.

    Args:
        func: The function to decorate

    Returns:
        Decorated function with HelpScout-specific retry logic

    Examples:
        >>> @retry_helpscout_operation
        ... def list_conversations(mailbox_id):
        ...     return client.get(f"/mailboxes/{mailbox_id}/conversations")
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2.0, min=1.0, max=60.0),
        retry=retry_if_exception_type((ConnectionError, RateLimitError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def retry_zoom_operation(func: Callable) -> Callable:
    """
    Decorator for Zoom API operations with retry logic.

    Zoom API has rate limits; this implements exponential backoff
    with 5 attempts, matching the pattern used by other API decorators.

    Args:
        func: The function to decorate

    Returns:
        Decorated function with Zoom-specific retry logic

    Examples:
        >>> @retry_zoom_operation
        ... def list_meetings(user_id):
        ...     return client.get(f"/users/{user_id}/meetings")
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2.0, min=1.0, max=60.0),
        retry=retry_if_exception_type((ConnectionError, RateLimitError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def retry_action_network_operation(func: Callable) -> Callable:
    """
    Decorator for Action Network API operations with retry logic.

    Action Network has a rate limit of 4 requests per second.
    This implements exponential backoff with 5 attempts.

    Args:
        func: The function to decorate

    Returns:
        Decorated function with Action Network-specific retry logic

    Examples:
        >>> @retry_action_network_operation
        ... def list_people():
        ...     return client.get("/people")
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2.0, min=1.0, max=60.0),
        retry=retry_if_exception_type((ConnectionError, RateLimitError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)


def retry_action_builder_operation(func: Callable) -> Callable:
    """
    Decorator for Action Builder API operations with retry logic.

    Action Builder has a rate limit of 4 requests per second.
    This implements exponential backoff with 5 attempts.

    Args:
        func: The function to decorate

    Returns:
        Decorated function with Action Builder-specific retry logic

    Examples:
        >>> @retry_action_builder_operation
        ... def list_campaigns():
        ...     return client.get("/campaigns")
    """
    return retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2.0, min=1.0, max=60.0),
        retry=retry_if_exception_type((ConnectionError, RateLimitError, Exception)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(func)
