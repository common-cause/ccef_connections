"""Tests for the OpenAI connector."""

import os
from unittest.mock import MagicMock, patch, call

import pytest
from pydantic import BaseModel

from ccef_connections.connectors.openai import OpenAIConnector
from ccef_connections.exceptions import ConnectionError, CredentialError


# ── Test response model ──────────────────────────────────────────────


class MockResponseModel(BaseModel):
    """A simple Pydantic model used as a structured output target in tests."""

    answer: str
    confidence: float


# ── Fixtures ─────────────────────────────────────────────────────────


FAKE_API_KEY = "sk-test-fake-key-1234567890"


@pytest.fixture
def connector():
    """Create an OpenAIConnector with a mocked credential manager."""
    with patch.object(
        OpenAIConnector, "_credential_manager", create=True
    ) as mock_cm:
        mock_cm.get_openai_key.return_value = FAKE_API_KEY
        c = OpenAIConnector()
        c._credential_manager = mock_cm
        yield c


@pytest.fixture
def connected_connector(connector):
    """Create a connector that is already connected with a fake API key."""
    connector._api_key = FAKE_API_KEY
    connector._is_connected = True
    return connector


@pytest.fixture(autouse=True)
def _clean_env():
    """Remove OPENAI_API_KEY from env before/after each test to avoid leaks."""
    old = os.environ.pop("OPENAI_API_KEY", None)
    yield
    if old is not None:
        os.environ["OPENAI_API_KEY"] = old
    else:
        os.environ.pop("OPENAI_API_KEY", None)


# ── Initialization ───────────────────────────────────────────────────


class TestInit:
    def test_initial_state(self):
        """A freshly created connector should be disconnected with no API key."""
        connector = OpenAIConnector()
        assert connector._api_key is None
        assert not connector.is_connected()
        assert connector._is_connected is False

    def test_initial_client_is_none(self):
        """The underlying _client attribute from BaseConnection should be None."""
        connector = OpenAIConnector()
        assert connector._client is None

    def test_has_credential_manager(self):
        """The connector should have a credential manager from BaseConnection."""
        connector = OpenAIConnector()
        assert connector._credential_manager is not None

    def test_repr_disconnected(self):
        connector = OpenAIConnector()
        assert repr(connector) == "<OpenAIConnector status=disconnected>"

    def test_repr_connected(self, connected_connector):
        assert repr(connected_connector) == "<OpenAIConnector status=connected>"


# ── Connect ──────────────────────────────────────────────────────────


class TestConnect:
    def test_connect_success(self, connector):
        """connect() should retrieve the API key and set environment variable."""
        connector.connect()

        assert connector.is_connected()
        assert connector._api_key == FAKE_API_KEY
        assert os.environ.get("OPENAI_API_KEY") == FAKE_API_KEY
        connector._credential_manager.get_openai_key.assert_called_once()

    def test_connect_sets_is_connected(self, connector):
        """connect() should set _is_connected to True."""
        assert connector._is_connected is False
        connector.connect()
        assert connector._is_connected is True

    def test_connect_missing_credentials_raises_credential_error(self):
        """connect() should re-raise CredentialError when key is missing."""
        connector = OpenAIConnector()
        connector._credential_manager.get_openai_key = MagicMock(
            side_effect=CredentialError("Required credential not found")
        )

        with pytest.raises(CredentialError, match="Required credential not found"):
            connector.connect()

        assert not connector.is_connected()
        assert connector._api_key is None

    def test_connect_unexpected_error_raises_connection_error(self):
        """connect() should wrap unexpected exceptions in ConnectionError."""
        connector = OpenAIConnector()
        connector._credential_manager.get_openai_key = MagicMock(
            side_effect=RuntimeError("something went wrong")
        )

        with pytest.raises(ConnectionError, match="Failed to connect to OpenAI"):
            connector.connect()

        assert not connector.is_connected()

    def test_connect_unexpected_error_chains_original(self):
        """The wrapped ConnectionError should chain to the original exception."""
        connector = OpenAIConnector()
        original = RuntimeError("original error")
        connector._credential_manager.get_openai_key = MagicMock(
            side_effect=original
        )

        with pytest.raises(ConnectionError) as exc_info:
            connector.connect()

        assert exc_info.value.__cause__ is original


# ── Disconnect ───────────────────────────────────────────────────────


class TestDisconnect:
    def test_disconnect_clears_state(self, connected_connector):
        """disconnect() should clear the API key and set _is_connected to False."""
        connected_connector.disconnect()

        assert not connected_connector.is_connected()
        assert connected_connector._api_key is None
        assert connected_connector._is_connected is False

    def test_disconnect_from_never_connected(self):
        """disconnect() on a fresh connector should not raise."""
        connector = OpenAIConnector()
        connector.disconnect()

        assert not connector.is_connected()
        assert connector._api_key is None

    def test_disconnect_idempotent(self, connected_connector):
        """Calling disconnect() twice should not raise."""
        connected_connector.disconnect()
        connected_connector.disconnect()

        assert not connected_connector.is_connected()
        assert connected_connector._api_key is None


# ── Health Check ─────────────────────────────────────────────────────


class TestHealthCheck:
    def test_health_check_when_connected(self, connected_connector):
        """health_check() should return True when connected with a valid key."""
        assert connected_connector.health_check() is True

    def test_health_check_when_not_connected(self, connector):
        """health_check() should return False when not connected."""
        assert connector.health_check() is False

    def test_health_check_connected_but_no_key(self, connector):
        """health_check() returns False if _is_connected but _api_key is None."""
        connector._is_connected = True
        connector._api_key = None
        assert connector.health_check() is False

    def test_health_check_has_key_but_not_connected(self, connector):
        """health_check() returns False if _api_key is set but _is_connected is False."""
        connector._is_connected = False
        connector._api_key = FAKE_API_KEY
        assert connector.health_check() is False

    def test_health_check_after_disconnect(self, connected_connector):
        """health_check() returns False after disconnecting."""
        assert connected_connector.health_check() is True
        connected_connector.disconnect()
        assert connected_connector.health_check() is False


# ── get_chat_model ───────────────────────────────────────────────────


class TestGetChatModel:
    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_get_chat_model_default_params(self, mock_init, connected_connector):
        """get_chat_model() with defaults calls init_chat_model correctly."""
        mock_model = MagicMock()
        mock_init.return_value = mock_model

        result = connected_connector.get_chat_model()

        mock_init.assert_called_once_with(
            "gpt-4o", model_provider="openai", temperature=0.1
        )
        assert result is mock_model

    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_get_chat_model_custom_params(self, mock_init, connected_connector):
        """get_chat_model() passes custom model name and temperature."""
        mock_init.return_value = MagicMock()

        connected_connector.get_chat_model("gpt-4o-mini", temperature=0.7)

        mock_init.assert_called_once_with(
            "gpt-4o-mini", model_provider="openai", temperature=0.7
        )

    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_get_chat_model_passes_kwargs(self, mock_init, connected_connector):
        """get_chat_model() forwards extra keyword arguments to init_chat_model."""
        mock_init.return_value = MagicMock()

        connected_connector.get_chat_model(
            "gpt-4o", temperature=0.2, max_tokens=500, top_p=0.9
        )

        mock_init.assert_called_once_with(
            "gpt-4o",
            model_provider="openai",
            temperature=0.2,
            max_tokens=500,
            top_p=0.9,
        )

    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_get_chat_model_auto_connects(self, mock_init, connector):
        """get_chat_model() should call connect() if not already connected."""
        mock_init.return_value = MagicMock()
        assert not connector.is_connected()

        connector.get_chat_model()

        assert connector.is_connected()
        assert connector._api_key == FAKE_API_KEY
        mock_init.assert_called_once()

    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_get_chat_model_does_not_reconnect(self, mock_init, connected_connector):
        """get_chat_model() should not call connect() if already connected."""
        mock_init.return_value = MagicMock()

        with patch.object(connected_connector, "connect") as mock_connect:
            connected_connector.get_chat_model()

        mock_connect.assert_not_called()
        mock_init.assert_called_once()

    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_get_chat_model_returns_init_chat_model_result(
        self, mock_init, connected_connector
    ):
        """get_chat_model() should return exactly what init_chat_model returns."""
        sentinel = object()
        mock_init.return_value = sentinel

        result = connected_connector.get_chat_model()

        assert result is sentinel


# ── invoke_with_structured_output ────────────────────────────────────


class TestInvokeWithStructuredOutput:
    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_invoke_structured_output_success(
        self, mock_init, mock_prompt_cls, connected_connector
    ):
        """invoke_with_structured_output() should chain prompt | structured_llm."""
        # Set up the mock LLM and structured LLM
        mock_llm = MagicMock()
        mock_init.return_value = mock_llm

        expected_result = MockResponseModel(answer="42", confidence=0.95)
        mock_structured_llm = MagicMock()
        mock_llm.with_structured_output.return_value = mock_structured_llm

        # Set up the mock prompt template and chain
        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template

        mock_chain = MagicMock()
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        mock_chain.invoke.return_value = expected_result

        result = connected_connector.invoke_with_structured_output(
            model="gpt-4o",
            system_prompt="You are a math tutor.",
            user_content="What is the answer to life?",
            response_model=MockResponseModel,
            temperature=0.1,
        )

        assert result is expected_result

        # Verify init_chat_model was called with the right model
        mock_init.assert_called_once_with(
            "gpt-4o", model_provider="openai", temperature=0.1
        )

        # Verify with_structured_output was called with the response model
        mock_llm.with_structured_output.assert_called_once_with(MockResponseModel)

        # Verify prompt template was created with system and human messages
        mock_prompt_cls.from_messages.assert_called_once_with(
            [("system", "{system_prompt}"), ("human", "{user_content}")]
        )

        # Verify chain.invoke was called with correct variables
        mock_chain.invoke.assert_called_once_with(
            {
                "system_prompt": "You are a math tutor.",
                "user_content": "What is the answer to life?",
            }
        )

    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_invoke_structured_output_custom_temperature(
        self, mock_init, mock_prompt_cls, connected_connector
    ):
        """invoke_with_structured_output() should pass custom temperature."""
        mock_llm = MagicMock()
        mock_init.return_value = mock_llm
        mock_structured_llm = MagicMock()
        mock_llm.with_structured_output.return_value = mock_structured_llm

        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template
        mock_chain = MagicMock()
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        mock_chain.invoke.return_value = MockResponseModel(
            answer="test", confidence=0.5
        )

        connected_connector.invoke_with_structured_output(
            model="gpt-4o-mini",
            system_prompt="System",
            user_content="User",
            response_model=MockResponseModel,
            temperature=0.8,
        )

        mock_init.assert_called_once_with(
            "gpt-4o-mini", model_provider="openai", temperature=0.8
        )

    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_invoke_structured_output_auto_connects(
        self, mock_init, mock_prompt_cls, connector
    ):
        """invoke_with_structured_output() should auto-connect via get_chat_model()."""
        mock_llm = MagicMock()
        mock_init.return_value = mock_llm
        mock_structured_llm = MagicMock()
        mock_llm.with_structured_output.return_value = mock_structured_llm

        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template
        mock_chain = MagicMock()
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        mock_chain.invoke.return_value = MockResponseModel(
            answer="auto", confidence=1.0
        )

        assert not connector.is_connected()

        connector.invoke_with_structured_output(
            model="gpt-4o",
            system_prompt="S",
            user_content="U",
            response_model=MockResponseModel,
        )

        assert connector.is_connected()

    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_invoke_structured_output_default_temperature(
        self, mock_init, mock_prompt_cls, connected_connector
    ):
        """invoke_with_structured_output() should use 0.1 as default temperature."""
        mock_llm = MagicMock()
        mock_init.return_value = mock_llm
        mock_structured_llm = MagicMock()
        mock_llm.with_structured_output.return_value = mock_structured_llm

        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template
        mock_chain = MagicMock()
        mock_template.__or__ = MagicMock(return_value=mock_chain)
        mock_chain.invoke.return_value = MockResponseModel(
            answer="default", confidence=0.9
        )

        connected_connector.invoke_with_structured_output(
            model="gpt-4o",
            system_prompt="System",
            user_content="User",
            response_model=MockResponseModel,
            # temperature not specified; should default to 0.1
        )

        mock_init.assert_called_once_with(
            "gpt-4o", model_provider="openai", temperature=0.1
        )


# ── create_prompt_template ───────────────────────────────────────────


class TestCreatePromptTemplate:
    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    def test_create_prompt_template_calls_from_messages(
        self, mock_prompt_cls, connected_connector
    ):
        """create_prompt_template() should delegate to ChatPromptTemplate.from_messages."""
        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template

        messages = [
            ("system", "You are a helpful assistant."),
            ("human", "{user_input}"),
        ]

        result = connected_connector.create_prompt_template(messages)

        mock_prompt_cls.from_messages.assert_called_once_with(messages)
        assert result is mock_template

    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    def test_create_prompt_template_single_message(
        self, mock_prompt_cls, connected_connector
    ):
        """create_prompt_template() should work with a single message."""
        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template

        messages = [("human", "Hello")]

        result = connected_connector.create_prompt_template(messages)

        mock_prompt_cls.from_messages.assert_called_once_with(messages)
        assert result is mock_template

    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    def test_create_prompt_template_empty_messages(
        self, mock_prompt_cls, connected_connector
    ):
        """create_prompt_template() with empty list should still call from_messages."""
        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template

        result = connected_connector.create_prompt_template([])

        mock_prompt_cls.from_messages.assert_called_once_with([])
        assert result is mock_template

    @patch("ccef_connections.connectors.openai.ChatPromptTemplate")
    def test_create_prompt_template_multi_turn(
        self, mock_prompt_cls, connected_connector
    ):
        """create_prompt_template() supports multi-turn conversation templates."""
        mock_template = MagicMock()
        mock_prompt_cls.from_messages.return_value = mock_template

        messages = [
            ("system", "You are a chatbot."),
            ("human", "Hi there!"),
            ("ai", "Hello! How can I help?"),
            ("human", "{follow_up}"),
        ]

        result = connected_connector.create_prompt_template(messages)

        mock_prompt_cls.from_messages.assert_called_once_with(messages)
        assert result is mock_template

    def test_create_prompt_template_does_not_require_connection(self):
        """create_prompt_template() should work even if not connected."""
        connector = OpenAIConnector()

        with patch(
            "ccef_connections.connectors.openai.ChatPromptTemplate"
        ) as mock_prompt_cls:
            mock_prompt_cls.from_messages.return_value = MagicMock()
            result = connector.create_prompt_template([("human", "test")])

        assert result is not None


# ── Context Manager ──────────────────────────────────────────────────


class TestContextManager:
    def test_context_manager_connects_and_disconnects(self, connector):
        """Using the connector as a context manager should connect on enter and
        disconnect on exit."""
        with connector as c:
            assert c is connector
            assert c.is_connected()
            assert c._api_key == FAKE_API_KEY

        assert not connector.is_connected()
        assert connector._api_key is None

    def test_context_manager_disconnects_on_exception(self, connector):
        """The context manager should disconnect even if an exception occurs."""
        with pytest.raises(ValueError, match="intentional"):
            with connector as c:
                assert c.is_connected()
                raise ValueError("intentional error")

        assert not connector.is_connected()
        assert connector._api_key is None

    def test_context_manager_sets_env_var(self, connector):
        """The context manager should set OPENAI_API_KEY on entry."""
        with connector as c:
            assert os.environ.get("OPENAI_API_KEY") == FAKE_API_KEY


# ── Retry Decorator ──────────────────────────────────────────────────


class TestRetryDecorator:
    def test_invoke_with_structured_output_is_decorated(self):
        """invoke_with_structured_output should have retry behavior from the
        @retry_openai_operation decorator (tenacity wrapping)."""
        # The tenacity retry decorator wraps the function and adds a 'retry'
        # attribute. Check that the method on the *class* has retry metadata.
        method = OpenAIConnector.invoke_with_structured_output
        assert hasattr(method, "retry"), (
            "invoke_with_structured_output should be decorated with "
            "@retry_openai_operation (tenacity)"
        )


# ── Edge Cases ───────────────────────────────────────────────────────


class TestEdgeCases:
    def test_connect_then_disconnect_then_reconnect(self, connector):
        """A connector should be able to reconnect after disconnecting."""
        connector.connect()
        assert connector.is_connected()

        connector.disconnect()
        assert not connector.is_connected()

        connector.connect()
        assert connector.is_connected()
        assert connector._api_key == FAKE_API_KEY

    @patch("ccef_connections.connectors.openai.init_chat_model")
    def test_get_chat_model_multiple_calls(self, mock_init, connected_connector):
        """Calling get_chat_model() multiple times should call init_chat_model each time."""
        mock_init.return_value = MagicMock()

        connected_connector.get_chat_model("gpt-4o")
        connected_connector.get_chat_model("gpt-4o-mini", temperature=0.5)

        assert mock_init.call_count == 2
        mock_init.assert_any_call(
            "gpt-4o", model_provider="openai", temperature=0.1
        )
        mock_init.assert_any_call(
            "gpt-4o-mini", model_provider="openai", temperature=0.5
        )

    def test_is_connected_mirrors_internal_flag(self, connector):
        """is_connected() should return the same value as _is_connected."""
        assert connector.is_connected() == connector._is_connected
        connector.connect()
        assert connector.is_connected() == connector._is_connected
        connector.disconnect()
        assert connector.is_connected() == connector._is_connected
