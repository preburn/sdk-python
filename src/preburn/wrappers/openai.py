"""OpenAI client wrapper that checks each call with Preburn and reports its token usage.

Needs the `openai` extra: `pip install "preburn[openai]"`.
"""

import asyncio
import json
import logging
import math
from collections.abc import Callable, Iterable, Mapping
from types import TracebackType
from typing import Any, Generic, Literal, TypeVar, overload

import openai
from openai.resources.chat import AsyncChat, AsyncCompletions, Chat, Completions
from openai.resources.responses import AsyncResponses, Responses
from openai.types import CompletionUsage
from openai.types.chat import ChatCompletion, ChatCompletionChunk
from openai.types.completion_usage import PromptTokensDetails
from openai.types.responses import (
    Response,
    ResponseCompletedEvent,
    ResponseFailedEvent,
    ResponseIncompleteEvent,
    ResponseStreamEvent,
    ResponseUsage,
)
from openai.types.responses.response_usage import InputTokensDetails

from preburn._async_client import AsyncPreburn
from preburn._client import Preburn
from preburn._errors import DecisionNotApplicableError, PreburnError
from preburn._models import CallContext, Decision

__all__ = [
    "DEFAULT_OUTPUT_TOKEN_ESTIMATE",
    "AsyncWrappedChat",
    "AsyncWrappedChatCompletions",
    "AsyncWrappedOpenAI",
    "AsyncWrappedResponses",
    "AsyncWrappedStream",
    "WrappedChat",
    "WrappedChatCompletions",
    "WrappedOpenAI",
    "WrappedResponses",
    "WrappedStream",
    "wrap_openai",
]

StreamItem = TypeVar("StreamItem")
UsageSource = TypeVar("UsageSource")
TokenUsage = dict[str, int]
Parameters = dict[str, Any]

PROVIDER = "openai"
CHARACTERS_PER_TOKEN = 4
DEFAULT_OUTPUT_TOKEN_ESTIMATE = 1024
"""Output tokens estimated for a call without a token limit parameter."""
RESPONSES_TOKEN_LIMIT_PARAMETER = "max_output_tokens"
TOKEN_LIMIT_OVERRIDES = frozenset({"max_tokens", "max_completion_tokens", "max_output_tokens"})
TEXT_PART_TYPES = frozenset({"text", "input_text", "output_text"})
REPORT_FAILED_EVENT = "wrapper.report_failed"
RELEASE_FAILED_EVENT = "wrapper.release_failed"
MODEL_MISSING_CODE = "model_missing"
CHAT_OPERATION = "chat.completions.create"
RESPONSES_OPERATION = "responses.create"

logger = logging.getLogger("preburn")


class _CheckedCall:
    def __init__(self, preburn: Preburn, decision: Decision) -> None:
        self._preburn = preburn
        self._decision = decision
        self._settled = False

    @classmethod
    def start(
        cls, preburn: Preburn, context: CallContext, model: str, usage_estimate: TokenUsage
    ) -> "_CheckedCall":
        decision = preburn.check(
            context.customer_id,
            context.feature,
            PROVIDER,
            model,
            usage_estimate=usage_estimate,
            customer_user_id=context.customer_user_id,
        )
        return cls(preburn, decision)

    def apply(self, parameters: Parameters, token_limit_parameter: str) -> None:
        try:
            _apply_decision(self._decision, parameters, token_limit_parameter)
        except DecisionNotApplicableError:
            self.release()
            raise

    def observe(
        self, read_usage: Callable[[UsageSource], TokenUsage | None], source: UsageSource
    ) -> None:
        if self._settled:
            return
        try:
            usage = read_usage(source)
        except Exception as error:
            _log_settle_failure(REPORT_FAILED_EVENT, self._decision, error)
            self.release()
            return
        if usage is None:
            return
        self._settled = True
        try:
            self._preburn.report(self._decision, usage)
        except Exception as error:
            _log_settle_failure(REPORT_FAILED_EVENT, self._decision, error)

    def settle(
        self, read_usage: Callable[[UsageSource], TokenUsage | None], source: UsageSource
    ) -> None:
        self.observe(read_usage, source)
        self.release()

    def release(self) -> None:
        if self._settled:
            return
        self._settled = True
        try:
            self._preburn.release(self._decision)
        except Exception as error:
            _log_settle_failure(RELEASE_FAILED_EVENT, self._decision, error)


class _AsyncCheckedCall:
    def __init__(self, preburn: AsyncPreburn, decision: Decision) -> None:
        self._preburn = preburn
        self._decision = decision
        self._settled = False

    @classmethod
    async def start(
        cls, preburn: AsyncPreburn, context: CallContext, model: str, usage_estimate: TokenUsage
    ) -> "_AsyncCheckedCall":
        decision = await preburn.check(
            context.customer_id,
            context.feature,
            PROVIDER,
            model,
            usage_estimate=usage_estimate,
            customer_user_id=context.customer_user_id,
        )
        return cls(preburn, decision)

    async def apply(self, parameters: Parameters, token_limit_parameter: str) -> None:
        try:
            _apply_decision(self._decision, parameters, token_limit_parameter)
        except DecisionNotApplicableError:
            await self.release()
            raise

    async def observe(
        self, read_usage: Callable[[UsageSource], TokenUsage | None], source: UsageSource
    ) -> None:
        if self._settled:
            return
        try:
            usage = read_usage(source)
        except Exception as error:
            _log_settle_failure(REPORT_FAILED_EVENT, self._decision, error)
            await self.release()
            return
        if usage is None:
            return
        self._settled = True
        try:
            await self._preburn.report(self._decision, usage)
        except Exception as error:
            _log_settle_failure(REPORT_FAILED_EVENT, self._decision, error)

    async def settle(
        self, read_usage: Callable[[UsageSource], TokenUsage | None], source: UsageSource
    ) -> None:
        await self.observe(read_usage, source)
        await self.release()

    async def release(self) -> None:
        if self._settled:
            return
        self._settled = True
        try:
            await self._preburn.release(self._decision)
        except Exception as error:
            _log_settle_failure(RELEASE_FAILED_EVENT, self._decision, error)

    async def release_after_cancellation(self) -> None:
        try:
            await asyncio.wait_for(self.release(), self._preburn.check_timeout)
        except asyncio.TimeoutError as error:
            _log_settle_failure(RELEASE_FAILED_EVENT, self._decision, error)


class WrappedStream(Generic[StreamItem]):
    """Stream of a wrapped call that reports the call's usage when the usage arrives.

    The usage arrives with the last chunk of a chat completion stream, and with the completed,
    incomplete or failed event of a responses stream. When the stream ends, fails or is closed
    before that, the decision is released. A report or release that fails is logged and never
    interrupts the stream. Close the stream, or use it as a context manager, when you stop
    reading early. Every other attribute is the OpenAI stream's.
    """

    def __init__(
        self,
        stream: openai.Stream[StreamItem],
        read_usage: Callable[[StreamItem], TokenUsage | None],
        call: _CheckedCall,
    ) -> None:
        """Wraps an OpenAI stream with the checked call it settles."""
        self._stream = stream
        self._read_usage = read_usage
        self._call = call

    def __iter__(self) -> "WrappedStream[StreamItem]":
        """Returns the stream."""
        return self

    def __next__(self) -> StreamItem:
        """Returns the next item, reporting the usage when the item carries it."""
        try:
            item = next(self._stream)
        except Exception:
            self._call.release()
            raise
        self._call.observe(self._read_usage, item)
        return item

    def __enter__(self) -> "WrappedStream[StreamItem]":
        """Returns the stream."""
        return self

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Closes the stream."""
        self.close()

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI stream."""
        return getattr(self._stream, name)

    def close(self) -> None:
        """Closes the OpenAI stream and releases the decision unless the usage was reported."""
        self._stream.close()
        self._call.release()


class AsyncWrappedStream(Generic[StreamItem]):
    """Async stream of a wrapped call that reports the call's usage when the usage arrives.

    The usage arrives with the last chunk of a chat completion stream, and with the completed,
    incomplete or failed event of a responses stream. When the stream ends, fails or is closed
    before that, the decision is released, and a cancelled read releases it before the
    cancellation propagates. A report or release that fails is logged and never interrupts the
    stream. Close the stream, or use it with `async with`, when you stop reading early. Every
    other attribute is the OpenAI stream's.
    """

    def __init__(
        self,
        stream: openai.AsyncStream[StreamItem],
        read_usage: Callable[[StreamItem], TokenUsage | None],
        call: _AsyncCheckedCall,
    ) -> None:
        """Wraps an OpenAI async stream with the checked call it settles."""
        self._stream = stream
        self._read_usage = read_usage
        self._call = call

    def __aiter__(self) -> "AsyncWrappedStream[StreamItem]":
        """Returns the stream."""
        return self

    async def __anext__(self) -> StreamItem:
        """Returns the next item, reporting the usage when the item carries it."""
        try:
            item = await anext(self._stream)
        except asyncio.CancelledError:
            await self._call.release_after_cancellation()
            raise
        except Exception:
            await self._call.release()
            raise
        await self._call.observe(self._read_usage, item)
        return item

    async def __aenter__(self) -> "AsyncWrappedStream[StreamItem]":
        """Returns the stream."""
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Closes the stream."""
        await self.close()

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI stream."""
        return getattr(self._stream, name)

    async def close(self) -> None:
        """Closes the OpenAI stream and releases the decision unless the usage was reported."""
        await self._stream.close()
        await self._call.release()

    async def aclose(self) -> None:
        """Closes the stream like `close`, so OpenAI's `aclose` alias also settles the call."""
        await self.close()


class WrappedChatCompletions:
    """Chat completions of a wrapped `OpenAI` client, with `create` checked by Preburn.

    Every other attribute is the OpenAI resource's, and calls through it are not checked.
    """

    def __init__(self, completions: Completions, preburn: Preburn) -> None:
        """Wraps the chat completions resource of an OpenAI client."""
        self._completions = completions
        self._preburn = preburn

    @overload
    def create(
        self, *, preburn: CallContext, stream: Literal[True], **parameters: Any
    ) -> WrappedStream[ChatCompletionChunk]: ...

    @overload
    def create(
        self, *, preburn: CallContext, stream: Literal[False] | None = None, **parameters: Any
    ) -> ChatCompletion: ...

    @overload
    def create(
        self, *, preburn: CallContext, stream: bool, **parameters: Any
    ) -> ChatCompletion | WrappedStream[ChatCompletionChunk]: ...

    def create(
        self, *, preburn: CallContext, **parameters: Any
    ) -> ChatCompletion | WrappedStream[ChatCompletionChunk]:
        """Checks the call with Preburn, applies the decision, calls OpenAI and reports usage.

        A streamed call asks OpenAI for a final usage chunk and reports from it.

        Args:
            preburn: Customer and feature the call is for.
            **parameters: Parameters of OpenAI's `chat.completions.create`.

        Returns:
            The chat completion, or a `WrappedStream` of its chunks when `stream` is true.

        Raises:
            DecisionDenied: The decision is deny. OpenAI is not called.
            DecisionNotApplicableError: The decision routes to another provider or sets a
                parameter the wrapper cannot apply. The decision is released and OpenAI is
                not called.
            PreburnError: `model` is missing (code `model_missing`), or the check was rejected
                with a status other than 502, 503 or 504.
            openai.OpenAIError: The OpenAI call failed. The decision is released.
        """
        model = _read_model(parameters, CHAT_OPERATION)
        _collect_messages(parameters, "messages")
        _collect_tools(parameters)
        token_limit_parameter = _resolve_chat_token_limit_parameter(parameters)
        usage_estimate = _estimate_usage(
            _count_chat_input_characters(parameters),
            _read_parameter(parameters, token_limit_parameter),
        )
        call = _CheckedCall.start(self._preburn, preburn, model, usage_estimate)
        call.apply(parameters, token_limit_parameter)
        _request_stream_usage(parameters)
        result: ChatCompletion | openai.Stream[ChatCompletionChunk]
        try:
            result = self._completions.create(**parameters)
        except Exception:
            call.release()
            raise
        if isinstance(result, openai.Stream):
            return WrappedStream(result, _read_chat_usage, call)
        call.settle(_read_chat_usage, result)
        return result

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI chat completions resource."""
        return getattr(self._completions, name)


class AsyncWrappedChatCompletions:
    """Chat completions of a wrapped `AsyncOpenAI` client, with `create` checked by Preburn.

    Every other attribute is the OpenAI resource's, and calls through it are not checked.
    """

    def __init__(self, completions: AsyncCompletions, preburn: AsyncPreburn) -> None:
        """Wraps the chat completions resource of an async OpenAI client."""
        self._completions = completions
        self._preburn = preburn

    @overload
    async def create(
        self, *, preburn: CallContext, stream: Literal[True], **parameters: Any
    ) -> AsyncWrappedStream[ChatCompletionChunk]: ...

    @overload
    async def create(
        self, *, preburn: CallContext, stream: Literal[False] | None = None, **parameters: Any
    ) -> ChatCompletion: ...

    @overload
    async def create(
        self, *, preburn: CallContext, stream: bool, **parameters: Any
    ) -> ChatCompletion | AsyncWrappedStream[ChatCompletionChunk]: ...

    async def create(
        self, *, preburn: CallContext, **parameters: Any
    ) -> ChatCompletion | AsyncWrappedStream[ChatCompletionChunk]:
        """Checks the call with Preburn, applies the decision, calls OpenAI and reports usage.

        A streamed call asks OpenAI for a final usage chunk and reports from it.

        Args:
            preburn: Customer and feature the call is for.
            **parameters: Parameters of OpenAI's `chat.completions.create`.

        Returns:
            The chat completion, or an `AsyncWrappedStream` of its chunks when `stream` is
            true.

        Raises:
            DecisionDenied: The decision is deny. OpenAI is not called.
            DecisionNotApplicableError: The decision routes to another provider or sets a
                parameter the wrapper cannot apply. The decision is released and OpenAI is
                not called.
            PreburnError: `model` is missing (code `model_missing`), or the check was rejected
                with a status other than 502, 503 or 504.
            openai.OpenAIError: The OpenAI call failed. The decision is released.
        """
        model = _read_model(parameters, CHAT_OPERATION)
        _collect_messages(parameters, "messages")
        _collect_tools(parameters)
        token_limit_parameter = _resolve_chat_token_limit_parameter(parameters)
        usage_estimate = _estimate_usage(
            _count_chat_input_characters(parameters),
            _read_parameter(parameters, token_limit_parameter),
        )
        call = await _AsyncCheckedCall.start(self._preburn, preburn, model, usage_estimate)
        await call.apply(parameters, token_limit_parameter)
        _request_stream_usage(parameters)
        result: ChatCompletion | openai.AsyncStream[ChatCompletionChunk]
        try:
            result = await self._completions.create(**parameters)
        except asyncio.CancelledError:
            await call.release_after_cancellation()
            raise
        except Exception:
            await call.release()
            raise
        if isinstance(result, openai.AsyncStream):
            return AsyncWrappedStream(result, _read_chat_usage, call)
        await call.settle(_read_chat_usage, result)
        return result

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI chat completions resource."""
        return getattr(self._completions, name)


class WrappedResponses:
    """Responses of a wrapped `OpenAI` client, with `create` checked by Preburn.

    Every other attribute is the OpenAI resource's, and calls through it are not checked.
    """

    def __init__(self, responses: Responses, preburn: Preburn) -> None:
        """Wraps the responses resource of an OpenAI client."""
        self._responses = responses
        self._preburn = preburn

    @overload
    def create(
        self, *, preburn: CallContext, stream: Literal[True], **parameters: Any
    ) -> WrappedStream[ResponseStreamEvent]: ...

    @overload
    def create(
        self, *, preburn: CallContext, stream: Literal[False] | None = None, **parameters: Any
    ) -> Response: ...

    @overload
    def create(
        self, *, preburn: CallContext, stream: bool, **parameters: Any
    ) -> Response | WrappedStream[ResponseStreamEvent]: ...

    def create(
        self, *, preburn: CallContext, **parameters: Any
    ) -> Response | WrappedStream[ResponseStreamEvent]:
        """Checks the call with Preburn, applies the decision, calls OpenAI and reports usage.

        Args:
            preburn: Customer and feature the call is for.
            **parameters: Parameters of OpenAI's `responses.create`.

        Returns:
            The response, or a `WrappedStream` of its events when `stream` is true.

        Raises:
            DecisionDenied: The decision is deny. OpenAI is not called.
            DecisionNotApplicableError: The decision routes to another provider or sets a
                parameter the wrapper cannot apply. The decision is released and OpenAI is
                not called.
            PreburnError: `model` is missing (code `model_missing`), or the check was rejected
                with a status other than 502, 503 or 504.
            openai.OpenAIError: The OpenAI call failed. The decision is released.
        """
        model = _read_model(parameters, RESPONSES_OPERATION)
        _collect_messages(parameters, "input")
        _collect_tools(parameters)
        usage_estimate = _estimate_usage(
            _count_responses_input_characters(parameters),
            _read_parameter(parameters, RESPONSES_TOKEN_LIMIT_PARAMETER),
        )
        call = _CheckedCall.start(self._preburn, preburn, model, usage_estimate)
        call.apply(parameters, RESPONSES_TOKEN_LIMIT_PARAMETER)
        result: Response | openai.Stream[ResponseStreamEvent]
        try:
            result = self._responses.create(**parameters)
        except Exception:
            call.release()
            raise
        if isinstance(result, openai.Stream):
            return WrappedStream(result, _read_event_usage, call)
        call.settle(_read_result_usage, result)
        return result

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI responses resource."""
        return getattr(self._responses, name)


class AsyncWrappedResponses:
    """Responses of a wrapped `AsyncOpenAI` client, with `create` checked by Preburn.

    Every other attribute is the OpenAI resource's, and calls through it are not checked.
    """

    def __init__(self, responses: AsyncResponses, preburn: AsyncPreburn) -> None:
        """Wraps the responses resource of an async OpenAI client."""
        self._responses = responses
        self._preburn = preburn

    @overload
    async def create(
        self, *, preburn: CallContext, stream: Literal[True], **parameters: Any
    ) -> AsyncWrappedStream[ResponseStreamEvent]: ...

    @overload
    async def create(
        self, *, preburn: CallContext, stream: Literal[False] | None = None, **parameters: Any
    ) -> Response: ...

    @overload
    async def create(
        self, *, preburn: CallContext, stream: bool, **parameters: Any
    ) -> Response | AsyncWrappedStream[ResponseStreamEvent]: ...

    async def create(
        self, *, preburn: CallContext, **parameters: Any
    ) -> Response | AsyncWrappedStream[ResponseStreamEvent]:
        """Checks the call with Preburn, applies the decision, calls OpenAI and reports usage.

        Args:
            preburn: Customer and feature the call is for.
            **parameters: Parameters of OpenAI's `responses.create`.

        Returns:
            The response, or an `AsyncWrappedStream` of its events when `stream` is true.

        Raises:
            DecisionDenied: The decision is deny. OpenAI is not called.
            DecisionNotApplicableError: The decision routes to another provider or sets a
                parameter the wrapper cannot apply. The decision is released and OpenAI is
                not called.
            PreburnError: `model` is missing (code `model_missing`), or the check was rejected
                with a status other than 502, 503 or 504.
            openai.OpenAIError: The OpenAI call failed. The decision is released.
        """
        model = _read_model(parameters, RESPONSES_OPERATION)
        _collect_messages(parameters, "input")
        _collect_tools(parameters)
        usage_estimate = _estimate_usage(
            _count_responses_input_characters(parameters),
            _read_parameter(parameters, RESPONSES_TOKEN_LIMIT_PARAMETER),
        )
        call = await _AsyncCheckedCall.start(self._preburn, preburn, model, usage_estimate)
        await call.apply(parameters, RESPONSES_TOKEN_LIMIT_PARAMETER)
        result: Response | openai.AsyncStream[ResponseStreamEvent]
        try:
            result = await self._responses.create(**parameters)
        except asyncio.CancelledError:
            await call.release_after_cancellation()
            raise
        except Exception:
            await call.release()
            raise
        if isinstance(result, openai.AsyncStream):
            return AsyncWrappedStream(result, _read_event_usage, call)
        await call.settle(_read_result_usage, result)
        return result

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI responses resource."""
        return getattr(self._responses, name)


class WrappedChat:
    """Chat resource of a wrapped `OpenAI` client.

    Attributes:
        completions: Chat completions with `create` checked by Preburn. Every other attribute
            is the OpenAI resource's.
    """

    def __init__(self, chat: Chat, preburn: Preburn) -> None:
        """Wraps the chat resource of an OpenAI client."""
        self._chat = chat
        self.completions = WrappedChatCompletions(chat.completions, preburn)

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI chat resource."""
        return getattr(self._chat, name)


class AsyncWrappedChat:
    """Chat resource of a wrapped `AsyncOpenAI` client.

    Attributes:
        completions: Chat completions with `create` checked by Preburn. Every other attribute
            is the OpenAI resource's.
    """

    def __init__(self, chat: AsyncChat, preburn: AsyncPreburn) -> None:
        """Wraps the chat resource of an async OpenAI client."""
        self._chat = chat
        self.completions = AsyncWrappedChatCompletions(chat.completions, preburn)

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI chat resource."""
        return getattr(self._chat, name)


class WrappedOpenAI:
    """`OpenAI` client whose `chat.completions.create` and `responses.create` Preburn checks.

    Every other attribute is the wrapped client's, and calls through it are not checked.

    Attributes:
        chat: Chat resource with checked completions.
        responses: Responses resource with a checked `create`.
    """

    def __init__(self, client: openai.OpenAI, preburn: Preburn) -> None:
        """Wraps an OpenAI client with the Preburn client that checks its calls."""
        self._client = client
        self.chat = WrappedChat(client.chat, preburn)
        self.responses = WrappedResponses(client.responses, preburn)

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI client."""
        return getattr(self._client, name)


class AsyncWrappedOpenAI:
    """`AsyncOpenAI` client whose `chat.completions.create` and `responses.create` Preburn checks.

    Every other attribute is the wrapped client's, and calls through it are not checked.

    Attributes:
        chat: Chat resource with checked completions.
        responses: Responses resource with a checked `create`.
    """

    def __init__(self, client: openai.AsyncOpenAI, preburn: AsyncPreburn) -> None:
        """Wraps an async OpenAI client with the Preburn client that checks its calls."""
        self._client = client
        self.chat = AsyncWrappedChat(client.chat, preburn)
        self.responses = AsyncWrappedResponses(client.responses, preburn)

    def __getattr__(self, name: str) -> Any:
        """Returns the attribute of the OpenAI client."""
        return getattr(self._client, name)


@overload
def wrap_openai(client: openai.OpenAI, preburn: Preburn) -> WrappedOpenAI: ...


@overload
def wrap_openai(client: openai.AsyncOpenAI, preburn: AsyncPreburn) -> AsyncWrappedOpenAI: ...


def wrap_openai(
    client: openai.OpenAI | openai.AsyncOpenAI, preburn: Preburn | AsyncPreburn
) -> WrappedOpenAI | AsyncWrappedOpenAI:
    """Wraps an OpenAI client so Preburn checks each chat completion and response call.

    A wrapped `chat.completions.create` or `responses.create` takes the extra keyword
    `preburn=CallContext(...)` and runs these steps:

    1. Estimates input tokens as `ceil(characters / 4)` over message text, text content
       parts, instructions and tool definitions, and output tokens from `max_tokens`,
       `max_completion_tokens` or `max_output_tokens`, else `DEFAULT_OUTPUT_TOKEN_ESTIMATE`.
    2. Checks the call. Deny raises `DecisionDenied`. Route swaps the model, or raises
       `DecisionNotApplicableError` when the target provider is not `openai`. A token limit
       override lowers the token limit parameter the call uses.
    3. Calls OpenAI. When the call raises, the decision is released and the error re-raised.
       A cancelled async call releases the decision before the cancellation propagates,
       waiting at most the `AsyncPreburn` client's `check_timeout`.
    4. Reports the usage: prompt or input tokens minus cached tokens as `input_tokens`,
       cached tokens as `cached_input_tokens`, completion or output tokens as
       `output_tokens`. Missing cached token details count as 0. A streamed call reports
       when the usage arrives and releases the decision when the stream ends without it.

    When Preburn cannot be reached the check falls back as the Preburn client does. A report
    or release that fails, or usage that cannot be read, such as more cached tokens than
    input tokens, is logged and never replaces the OpenAI result, stream or error. Usage that
    cannot be read releases the decision.

    Args:
        client: `OpenAI` or `AsyncOpenAI` client.
        preburn: `Preburn` client for an `OpenAI` client, `AsyncPreburn` for an
            `AsyncOpenAI` client.

    Returns:
        `WrappedOpenAI` for an `OpenAI` client, `AsyncWrappedOpenAI` for an `AsyncOpenAI`
        client.

    Raises:
        TypeError: The clients are not both sync or both async.
    """
    if isinstance(client, openai.OpenAI) and isinstance(preburn, Preburn):
        return WrappedOpenAI(client, preburn)
    if isinstance(client, openai.AsyncOpenAI) and isinstance(preburn, AsyncPreburn):
        return AsyncWrappedOpenAI(client, preburn)
    raise TypeError(
        "wrap_openai needs OpenAI with Preburn or AsyncOpenAI with AsyncPreburn "
        f"client={type(client).__name__} preburn={type(preburn).__name__}"
    )


def _apply_decision(decision: Decision, parameters: Parameters, token_limit_parameter: str) -> None:
    decision.raise_for_denial()
    if decision.outcome == "route":
        if decision.provider != PROVIDER:
            raise DecisionNotApplicableError(
                decision,
                f"route target not on openai provider={decision.provider} model={decision.model}",
            )
        parameters["model"] = decision.model
    for name, value in decision.overrides.items():
        if name not in TOKEN_LIMIT_OVERRIDES or type(value) is not int:
            raise DecisionNotApplicableError(
                decision, f"override not applicable to openai calls name={name}"
            )
        requested_limit = _read_parameter(parameters, token_limit_parameter)
        parameters[token_limit_parameter] = (
            value if requested_limit is None else min(requested_limit, value)
        )


def _request_stream_usage(parameters: Parameters) -> None:
    if _read_parameter(parameters, "stream"):
        stream_options = _read_parameter(parameters, "stream_options") or {}
        parameters["stream_options"] = {**stream_options, "include_usage": True}


def _resolve_chat_token_limit_parameter(parameters: Parameters) -> str:
    if _read_parameter(parameters, "max_tokens") is not None:
        return "max_tokens"
    return "max_completion_tokens"


def _estimate_usage(input_characters: int, output_token_limit: int | None) -> TokenUsage:
    return {
        "input_tokens": math.ceil(input_characters / CHARACTERS_PER_TOKEN),
        "output_tokens": (
            DEFAULT_OUTPUT_TOKEN_ESTIMATE if output_token_limit is None else output_token_limit
        ),
    }


def _count_chat_input_characters(parameters: Parameters) -> int:
    return _count_message_characters(parameters["messages"]) + _count_tool_characters(parameters)


def _count_responses_input_characters(parameters: Parameters) -> int:
    return (
        _count_content_characters(_read_parameter(parameters, "instructions"))
        + _count_message_characters(_read_parameter(parameters, "input"))
        + _count_tool_characters(parameters)
    )


def _count_message_characters(messages: str | Iterable[object] | None) -> int:
    if messages is None or isinstance(messages, str):
        return _count_content_characters(messages)
    return sum(
        _count_content_characters(_convert_to_mapping(message).get("content"))
        for message in messages
    )


def _count_content_characters(content: str | Iterable[object] | None) -> int:
    if content is None:
        return 0
    if isinstance(content, str):
        return len(content)
    parts = [_convert_to_mapping(part) for part in content]
    return sum(len(part["text"]) for part in parts if part["type"] in TEXT_PART_TYPES)


def _collect_messages(parameters: Parameters, name: str) -> None:
    messages = _read_parameter(parameters, name)
    if messages is not None and not isinstance(messages, str):
        parameters[name] = [_collect_content(message) for message in messages]


def _collect_content(message: object) -> object:
    if not isinstance(message, Mapping):
        return message
    content = message.get("content")
    if content is None or isinstance(content, (str, list)):
        return message
    return {**message, "content": list(content)}


def _collect_tools(parameters: Parameters) -> None:
    tools = _read_parameter(parameters, "tools")
    if tools is not None:
        parameters["tools"] = list(tools)


def _count_tool_characters(parameters: Parameters) -> int:
    tools = _read_parameter(parameters, "tools")
    if tools is None:
        return 0
    return len(
        json.dumps(tools, separators=(",", ":"), ensure_ascii=False, default=_convert_to_dict)
    )


def _read_model(parameters: Parameters, operation: str) -> str:
    model = _read_parameter(parameters, "model")
    if not isinstance(model, str):
        raise PreburnError(
            MODEL_MISSING_CODE,
            f"model missing, pass it to {operation} so Preburn can check the call",
        )
    return model


def _read_parameter(parameters: Parameters, name: str) -> Any:
    value = parameters.get(name)
    if isinstance(value, (openai.Omit, openai.NotGiven)):
        return None
    return value


def _convert_to_mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, openai.BaseModel):
        return value.to_dict()
    if isinstance(value, Mapping):
        return value
    raise TypeError(f"openai parameter entry not a mapping type={type(value).__name__}")


def _convert_to_dict(value: object) -> dict[str, Any]:
    return dict(_convert_to_mapping(value))


def _read_chat_usage(result: ChatCompletion | ChatCompletionChunk) -> TokenUsage | None:
    return _read_completion_usage(result.usage)


def _read_result_usage(response: Response) -> TokenUsage | None:
    return _read_response_usage(response.usage)


def _read_event_usage(event: ResponseStreamEvent) -> TokenUsage | None:
    if isinstance(event, (ResponseCompletedEvent, ResponseIncompleteEvent, ResponseFailedEvent)):
        return _read_response_usage(event.response.usage)
    return None


def _read_completion_usage(usage: CompletionUsage | None) -> TokenUsage | None:
    if usage is None:
        return None
    return _build_token_usage(
        usage.prompt_tokens,
        _count_cached_tokens(usage.prompt_tokens_details),
        usage.completion_tokens,
    )


def _read_response_usage(usage: ResponseUsage | None) -> TokenUsage | None:
    if usage is None:
        return None
    return _build_token_usage(
        usage.input_tokens,
        _count_cached_tokens(usage.input_tokens_details),
        usage.output_tokens,
    )


def _count_cached_tokens(details: PromptTokensDetails | InputTokensDetails | None) -> int:
    if details is None or details.cached_tokens is None:
        return 0
    return details.cached_tokens


def _build_token_usage(input_tokens: int, cached_tokens: int, output_tokens: int) -> TokenUsage:
    if cached_tokens > input_tokens:
        raise ValueError(
            f"cached tokens exceed input tokens cached_tokens={cached_tokens} "
            f"input_tokens={input_tokens}"
        )
    return {
        "input_tokens": input_tokens - cached_tokens,
        "cached_input_tokens": cached_tokens,
        "output_tokens": output_tokens,
    }


def _log_settle_failure(event: str, decision: Decision, error: Exception) -> None:
    logger.warning(
        f"{event} decision_id={decision.decision_id} feature={decision.feature} "
        f"error={type(error).__name__}"
    )
