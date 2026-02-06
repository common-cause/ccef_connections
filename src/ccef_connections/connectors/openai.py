"""
OpenAI/ChatGPT connector for CCEF connections library.

This module provides a wrapper around langchain-openai with automatic credential
management and support for structured outputs.
"""

import logging
import os
from typing import Any, Optional, Type

from langchain_core.prompts import ChatPromptTemplate
from langchain.chat_models import init_chat_model
from pydantic import BaseModel

from ..core.base import BaseConnection
from ..core.retry import retry_openai_operation
from ..exceptions import ConnectionError, CredentialError

logger = logging.getLogger(__name__)


class OpenAIConnector(BaseConnection):
    """
    OpenAI/ChatGPT connector with langchain integration.

    This connector provides an interface to OpenAI models using langchain-openai,
    with automatic credential management and support for structured outputs.

    Examples:
        >>> connector = OpenAIConnector()
        >>> llm = connector.get_chat_model("gpt-4o")
        >>>
        >>> # With structured output
        >>> class Response(BaseModel):
        ...     answer: str
        ...     confidence: float
        >>>
        >>> structured_llm = llm.with_structured_output(Response)
        >>> result = structured_llm.invoke("What is 2+2?")
    """

    def __init__(self) -> None:
        """Initialize the OpenAI connector."""
        super().__init__()
        self._api_key: Optional[str] = None

    def connect(self) -> None:
        """
        Establish connection to OpenAI by setting up credentials.

        Raises:
            CredentialError: If OpenAI API key is missing
            ConnectionError: If connection setup fails
        """
        try:
            api_key = self._credential_manager.get_openai_key()
            # Set the API key in environment for langchain to use
            os.environ["OPENAI_API_KEY"] = api_key
            self._api_key = api_key
            self._is_connected = True
            logger.info("Successfully connected to OpenAI")
        except CredentialError:
            logger.error("Failed to connect to OpenAI: missing credentials")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to OpenAI: {str(e)}")
            raise ConnectionError(f"Failed to connect to OpenAI: {str(e)}") from e

    def disconnect(self) -> None:
        """Clean up OpenAI connection."""
        self._api_key = None
        self._is_connected = False
        logger.debug("Disconnected from OpenAI")

    def health_check(self) -> bool:
        """
        Check if the OpenAI connection is healthy.

        Returns:
            True if connected and API key is set, False otherwise
        """
        return self._is_connected and self._api_key is not None

    def get_chat_model(
        self,
        model: str = "gpt-4o",
        temperature: float = 0.1,
        **kwargs: Any,
    ) -> Any:
        """
        Get a configured chat model instance.

        Args:
            model: Model name (default: "gpt-4o")
            temperature: Temperature for generation (default: 0.1)
            **kwargs: Additional arguments to pass to init_chat_model

        Returns:
            Configured ChatOpenAI instance

        Raises:
            ConnectionError: If not connected

        Examples:
            >>> connector = OpenAIConnector()
            >>> llm = connector.get_chat_model("gpt-4o", temperature=0.2)
            >>> response = llm.invoke("Hello, how are you?")
        """
        if not self._is_connected:
            self.connect()

        logger.debug(f"Creating chat model: {model} (temperature={temperature})")
        return init_chat_model(
            model, model_provider="openai", temperature=temperature, **kwargs
        )

    @retry_openai_operation
    def invoke_with_structured_output(
        self,
        model: str,
        system_prompt: str,
        user_content: str,
        response_model: Type[BaseModel],
        temperature: float = 0.1,
    ) -> BaseModel:
        """
        Invoke the model with structured output.

        This is a convenience method that wraps the common pattern of using
        structured outputs with Pydantic models.

        Args:
            model: Model name (e.g., "gpt-4o")
            system_prompt: System prompt
            user_content: User message content
            response_model: Pydantic model class for structured output
            temperature: Temperature for generation

        Returns:
            Instance of the response_model with parsed output

        Examples:
            >>> from pydantic import BaseModel
            >>> class Analysis(BaseModel):
            ...     sentiment: str
            ...     summary: str
            >>>
            >>> connector = OpenAIConnector()
            >>> result = connector.invoke_with_structured_output(
            ...     "gpt-4o",
            ...     "You are a helpful assistant.",
            ...     "Analyze this text: I love this product!",
            ...     Analysis
            ... )
            >>> print(result.sentiment, result.summary)
        """
        llm = self.get_chat_model(model, temperature=temperature)
        structured_llm = llm.with_structured_output(response_model)

        prompt_template = ChatPromptTemplate.from_messages(
            [("system", "{system_prompt}"), ("human", "{user_content}")]
        )

        chain = prompt_template | structured_llm
        result = chain.invoke({"system_prompt": system_prompt, "user_content": user_content})

        logger.debug(f"Received structured output: {type(result).__name__}")
        return result

    def create_prompt_template(self, messages: list) -> ChatPromptTemplate:
        """
        Create a chat prompt template.

        Args:
            messages: List of (role, content) tuples

        Returns:
            ChatPromptTemplate instance

        Examples:
            >>> connector = OpenAIConnector()
            >>> template = connector.create_prompt_template([
            ...     ("system", "You are a helpful assistant."),
            ...     ("human", "{user_input}")
            ... ])
        """
        return ChatPromptTemplate.from_messages(messages)
