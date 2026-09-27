import asyncio
import json
import logging
import math
import secrets
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from typing import Any, NoReturn

import httpx
import httpx2
import openai
import pytest
from openai.types.chat import ChatCompletion, ChatCompletionChunk, ChatCompletionMessage

from preburn import (
    AsyncPreburn,
    CallContext,
    DecisionDenied,
    DecisionNotApplicableError,
    Preburn,
    PreburnError,
    ReportMode,
)
from preburn._transport import CHECK_PATH, RELEASE_PATH, REPORT_PATH, REPORTS_PATH
from preburn.wrappers.openai import (
    AsyncWrappedOpenAI,
    WrappedOpenAI,
    wrap_openai,
)

PREBURN = "preburn"
OPENAI = "openai"
PREBURN_BASE_URL = "http://preburn.test"
OPENAI_BASE_URL = "http://openai.test/v1"
CHAT_PATH = "/v1/chat/completions"
RESPONSES_PATH = "/v1/responses"
DECISION_ID = "dec_01jbvagescfn78y0938nkrkayd"
MODEL = "gpt-6-sol"
ROUTE_MODEL = "gpt-6-luna"
CHAT_STREAM_LENGTH = 3
SHORT_CHECK_TIMEOUT_SECONDS = 0.05
IDLE_INTERVAL_SECONDS = 3600.0
CANCELLATION_DEADLINE_SECONDS = 2.0
EVENT_STREAM_HEADERS = {"content-type": "text/event-stream"}
CONTEXT = CallContext("customer_1", "chat", customer_user_id="user_7")
SYSTEM_TEXT = "You answer in one line."
USER_TEXT = "Describe this image."
EARLIER_ANSWER_TEXT = "A cat on a mat."
INSTRUCTIONS_TEXT = "Be brief."
INPUT_TEXT = "Summarize the quarterly report."
TOOLS: list[dict[str, object]] = [
    {
        "type": "function",
        "function": {
            "name": "lookup_order",
            "parameters": {"type": "object", "properties": {"order_id": {"type": "string"}}},
        },
    }
]
MESSAGES: list[dict[str, object]] = [
    {"role": "system", "content": SYSTEM_TEXT},
    {"role": "user", "content": USER_TEXT},
]
CHAT_USAGE = {
    "prompt_tokens": 120,
    "completion_tokens": 30,
    "total_tokens": 150,
    "prompt_tokens_details": {"cached_tokens": 100},
}
RESPONSE_USAGE = {
    "input_tokens": 240,
    "input_tokens_details": {"cached_tokens": 200, "cache_write_tokens": 0},
    "output_tokens": 45,
    "output_tokens_details": {"reasoning_tokens": 0},
    "total_tokens": 285,
}
SENT_CHAT_USAGE = {"input_tokens": "20", "cached_input_tokens": "100", "output_tokens": "30"}
SENT_RESPONSE_USAGE = {"input_tokens": "40", "cached_input_tokens": "200", "output_tokens": "45"}
CHAT_COMPLETION: dict[str, object] = {
    "id": "chatcmpl-1",
    "object": "chat.completion",
    "created": 1790000000,
    "model": MODEL,
    "choices": [
        {
            "index": 0,
            "message": {"role": "assistant", "content": "Hello"},
            "finish_reason": "stop",
            "logprobs": None,
        }
    ],
    "usage": CHAT_USAGE,
}
RESPONSE: dict[str, object] = {
    "id": "resp_1",
    "object": "response",
    "created_at": 1790000000,
    "model": MODEL,
    "status": "completed",
    "output": [],
    "parallel_tool_calls": True,
    "tool_choice": "auto",
    "tools": [],
    "usage": RESPONSE_USAGE,
}
REPORT_RESPONSE: dict[str, object] = {
    "ledger_entry_id": "led_01jbvagescfn78y0938nkrkayd",
    "cost": "0.001200000",
    "cost_status": "costed",
    "duplicate": False,
}
BATCH_RESPONSE: dict[str, object] = {"results": [{"status": 202, "result": REPORT_RESPONSE}]}


@dataclass(frozen=True)
class RecordedRequest:
    service: str
    path: str
    body: Any


@dataclass(frozen=True)
class PlannedAnswer:
    status: int
    body: object = None
    events: tuple[dict[str, object], ...] | None = None
    held: bool = False


class HeldEventStream(httpx2.AsyncByteStream):
    def __init__(self, content: bytes, hold_reached: asyncio.Event) -> None:
        self._content = content
        self._hold_reached = hold_reached

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self._content
        self._hold_reached.set()
        await asyncio.Event().wait()


class FakeServices:
    def __init__(self) -> None:
        self.requests: list[RecordedRequest] = []
        self.hold_reached = asyncio.Event()
        self._answers: dict[tuple[str, str], PlannedAnswer] = {}

    def plan(self, service: str, path: str, answer: PlannedAnswer) -> None:
        self._answers[(service, path)] = answer

    def handle_preburn(self, request: httpx.Request) -> httpx.Response:
        answer = self._record(PREBURN, request.url.path, request.content)
        if answer.body is None:
            return httpx.Response(answer.status)
        return httpx.Response(answer.status, json=answer.body)

    async def handle_preburn_async(self, request: httpx.Request) -> httpx.Response:
        response = self.handle_preburn(request)
        if self._answers[(PREBURN, request.url.path)].held:
            await self.hold()
        return response

    def handle_openai(self, request: httpx2.Request) -> httpx2.Response:
        answer = self._record(OPENAI, request.url.path, request.content)
        if answer.events is None:
            return httpx2.Response(answer.status, json=answer.body)
        return httpx2.Response(
            answer.status, headers=EVENT_STREAM_HEADERS, content=format_events(answer.events)
        )

    async def handle_openai_async(self, request: httpx2.Request) -> httpx2.Response:
        response = self.handle_openai(request)
        answer = self._answers[(OPENAI, request.url.path)]
        if not answer.held:
            return response
        if answer.events is None:
            await self.hold()
        held_stream = HeldEventStream(format_events(answer.events), self.hold_reached)
        return httpx2.Response(answer.status, headers=EVENT_STREAM_HEADERS, stream=held_stream)

    async def hold(self) -> NoReturn:
        self.hold_reached.set()
        await asyncio.Event().wait()
        pytest.fail("held request answered")

    def paths(self) -> list[tuple[str, str]]:
        return [(request.service, request.path) for request in self.requests]

    def bodies(self, service: str, path: str) -> list[Any]:
        return [
            request.body
            for request in self.requests
            if (request.service, request.path) == (service, path)
        ]

    def _record(self, service: str, path: str, content: bytes) -> PlannedAnswer:
        self.requests.append(RecordedRequest(service, path, json.loads(content or b"null")))
        return self._answers[(service, path)]


def make_check_body(
    outcome: str = "allow",
    provider: str = OPENAI,
    model: str = MODEL,
    overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "decision_id": DECISION_ID,
        "outcome": outcome,
        "reason": "no_policy_matched" if outcome == "allow" else "policy_matched",
        "provider": provider,
        "model": model,
        "overrides": {} if overrides is None else overrides,
        "estimated_cost": "0.012000000",
        "reserved_amount": "0.012000000",
        "estimate_basis": "request_estimate",
        "cost_status": "costed",
        "matched_policy_id": None,
        "fallback_outcome": "allow",
        "expires_at": "2026-09-26T10:10:00Z",
        "signals": {
            "period_revenue_net": "30.000000000",
            "cost_allowance": "18.000000000",
            "cost_to_date": "4.250000000",
            "reserved": "0.012000000",
            "elapsed_fraction": "0.2500",
            "allowance_remaining": "13.738000000",
            "pace": "0.9444",
            "projected_margin": "0.4333",
            "request_estimated_cost": "0.012000000",
            "period_decision_count": 41,
            "features": {},
        },
    }


def make_chat_chunk(content: str | None, usage: dict[str, object] | None) -> dict[str, object]:
    choices = [] if content is None else [{"index": 0, "delta": {"content": content}}]
    return {
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 1790000000,
        "model": MODEL,
        "choices": choices,
        "usage": usage,
    }


def make_response_event(event_type: str, sequence_number: int) -> dict[str, object]:
    usage = RESPONSE_USAGE if event_type == "response.completed" else None
    return {
        "type": event_type,
        "sequence_number": sequence_number,
        "response": {**RESPONSE, "usage": usage},
    }


def make_chat_stream() -> tuple[dict[str, object], ...]:
    return (
        make_chat_chunk("Hel", None),
        make_chat_chunk("lo", None),
        make_chat_chunk(None, CHAT_USAGE),
    )


def make_response_stream() -> tuple[dict[str, object], ...]:
    return (
        make_response_event("response.created", 0),
        make_response_event("response.in_progress", 1),
        make_response_event("response.completed", 2),
    )


def make_preburn_api_key() -> str:
    return f"pb_test_runtime_{secrets.token_hex(16)}"


def make_preburn(
    services: FakeServices, report_mode: ReportMode = "sync", api_key: str | None = None
) -> Preburn:
    return Preburn(
        api_key=make_preburn_api_key() if api_key is None else api_key,
        base_url=PREBURN_BASE_URL,
        report_mode=report_mode,
        flush_interval=IDLE_INTERVAL_SECONDS,
        transport=httpx.MockTransport(services.handle_preburn),
    )


def make_async_preburn(
    services: FakeServices, check_timeout: float = 0.25, report_mode: ReportMode = "sync"
) -> AsyncPreburn:
    return AsyncPreburn(
        api_key=make_preburn_api_key(),
        base_url=PREBURN_BASE_URL,
        check_timeout=check_timeout,
        report_mode=report_mode,
        flush_interval=IDLE_INTERVAL_SECONDS,
        transport=httpx.MockTransport(services.handle_preburn_async),
    )


def make_openai(
    services: FakeServices, handle: Callable[[httpx2.Request], httpx2.Response] | None = None
) -> openai.OpenAI:
    transport = httpx2.MockTransport(services.handle_openai if handle is None else handle)
    return openai.OpenAI(
        api_key=secrets.token_hex(16),
        base_url=OPENAI_BASE_URL,
        max_retries=0,
        http_client=httpx2.Client(transport=transport),
    )


def make_async_openai(services: FakeServices) -> openai.AsyncOpenAI:
    return openai.AsyncOpenAI(
        api_key=secrets.token_hex(16),
        base_url=OPENAI_BASE_URL,
        max_retries=0,
        http_client=httpx2.AsyncClient(
            transport=httpx2.MockTransport(services.handle_openai_async)
        ),
    )


def build_services() -> FakeServices:
    services = FakeServices()
    services.plan(PREBURN, CHECK_PATH, PlannedAnswer(200, make_check_body()))
    services.plan(PREBURN, REPORT_PATH, PlannedAnswer(202, REPORT_RESPONSE))
    services.plan(PREBURN, RELEASE_PATH, PlannedAnswer(204))
    services.plan(PREBURN, REPORTS_PATH, PlannedAnswer(202, BATCH_RESPONSE))
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, CHAT_COMPLETION))
    services.plan(OPENAI, RESPONSES_PATH, PlannedAnswer(200, RESPONSE))
    return services


def format_events(events: tuple[dict[str, object], ...]) -> bytes:
    return "".join(f"data: {json.dumps(event)}\n\n" for event in events).encode()


def estimate_tokens(*texts: str) -> int:
    return math.ceil(sum(len(text) for text in texts) / 4)


def format_tools(tools: list[dict[str, object]]) -> str:
    return json.dumps(tools, separators=(",", ":"))


@pytest.fixture(name="services")
def make_services() -> FakeServices:
    return build_services()


@pytest.fixture(name="wrapped")
def make_wrapped(services: FakeServices) -> Iterator[WrappedOpenAI]:
    with make_preburn(services) as preburn, make_openai(services) as client:
        yield wrap_openai(client, preburn)


def test_chat_completion_checks_calls_and_reports_usage(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    completion = wrapped.chat.completions.create(
        model=MODEL, messages=MESSAGES, max_completion_tokens=400, preburn=CONTEXT
    )
    if not isinstance(completion, ChatCompletion) or completion.id != "chatcmpl-1":
        pytest.fail(f"completion={completion!r}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, REPORT_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    expected_check = {
        "customer_id": "customer_1",
        "feature": "chat",
        "provider": "openai",
        "model": MODEL,
        "usage_estimate": {
            "input_tokens": str(estimate_tokens(SYSTEM_TEXT, USER_TEXT)),
            "output_tokens": "400",
        },
        "customer_user_id": "user_7",
    }
    if services.bodies(PREBURN, CHECK_PATH) != [expected_check]:
        pytest.fail(f"check={services.bodies(PREBURN, CHECK_PATH)}")
    expected_call = {"model": MODEL, "messages": MESSAGES, "max_completion_tokens": 400}
    if services.bodies(OPENAI, CHAT_PATH) != [expected_call]:
        pytest.fail(f"call={services.bodies(OPENAI, CHAT_PATH)}")
    report = services.bodies(PREBURN, REPORT_PATH)[0]
    if (report["decision_id"], report["usage"]) != (DECISION_ID, SENT_CHAT_USAGE):
        pytest.fail(f"report={report}")


def test_chat_estimate_counts_text_parts_and_tool_definitions(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    messages: list[object] = [
        {"role": "system", "content": SYSTEM_TEXT},
        {
            "role": "user",
            "content": [
                {"type": "text", "text": USER_TEXT},
                {"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}},
            ],
        },
        ChatCompletionMessage(role="assistant", content=EARLIER_ANSWER_TEXT),
    ]
    wrapped.chat.completions.create(
        model=MODEL, messages=iter(messages), tools=iter(TOOLS), preburn=CONTEXT
    )
    estimate = services.bodies(PREBURN, CHECK_PATH)[0]["usage_estimate"]
    expected_input = estimate_tokens(
        SYSTEM_TEXT, USER_TEXT, EARLIER_ANSWER_TEXT, format_tools(TOOLS)
    )
    expected = {"input_tokens": str(expected_input), "output_tokens": "1024"}
    if estimate != expected:
        pytest.fail(f"usage_estimate={estimate}")
    call = services.bodies(OPENAI, CHAT_PATH)[0]
    if len(call["messages"]) != len(messages) or call["tools"] != TOOLS:
        pytest.fail(f"call={call}")


def test_responses_create_reports_cached_input_tokens(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    response_input = [{"role": "user", "content": [{"type": "input_text", "text": INPUT_TEXT}]}]
    response_tools: list[dict[str, object]] = [
        {"type": "function", "name": "lookup_order", "parameters": {"type": "object"}}
    ]
    response = wrapped.responses.create(
        model=MODEL,
        instructions=INSTRUCTIONS_TEXT,
        input=response_input,
        tools=response_tools,
        max_output_tokens=300,
        preburn=CONTEXT,
    )
    if not isinstance(response, openai.types.responses.Response) or response.id != "resp_1":
        pytest.fail(f"response={response!r}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, RESPONSES_PATH), (PREBURN, REPORT_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    estimate = services.bodies(PREBURN, CHECK_PATH)[0]["usage_estimate"]
    expected_input = estimate_tokens(INSTRUCTIONS_TEXT, INPUT_TEXT, format_tools(response_tools))
    if estimate != {"input_tokens": str(expected_input), "output_tokens": "300"}:
        pytest.fail(f"usage_estimate={estimate}")
    report = services.bodies(PREBURN, REPORT_PATH)[0]
    if (report["decision_id"], report["usage"]) != (DECISION_ID, SENT_RESPONSE_USAGE):
        pytest.fail(f"report={report}")


def test_generator_content_parts_reach_openai(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    chat_parts: list[dict[str, object]] = [{"type": "text", "text": USER_TEXT}]
    response_parts: list[dict[str, object]] = [{"type": "input_text", "text": INPUT_TEXT}]
    wrapped.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": (part for part in chat_parts)}],
        preburn=CONTEXT,
    )
    wrapped.responses.create(
        model=MODEL,
        input=[{"role": "user", "content": (part for part in response_parts)}],
        preburn=CONTEXT,
    )
    chat_call = services.bodies(OPENAI, CHAT_PATH)[0]
    if chat_call["messages"] != [{"role": "user", "content": chat_parts}]:
        pytest.fail(f"messages={chat_call['messages']}")
    response_call = services.bodies(OPENAI, RESPONSES_PATH)[0]
    if response_call["input"] != [{"role": "user", "content": response_parts}]:
        pytest.fail(f"input={response_call['input']}")
    estimates = [
        body["usage_estimate"]["input_tokens"] for body in services.bodies(PREBURN, CHECK_PATH)
    ]
    if estimates != [str(estimate_tokens(USER_TEXT)), str(estimate_tokens(INPUT_TEXT))]:
        pytest.fail(f"estimates={estimates}")


def test_responses_estimate_counts_plain_text_input(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    wrapped.responses.create(model=MODEL, input=INPUT_TEXT, preburn=CONTEXT)
    estimate = services.bodies(PREBURN, CHECK_PATH)[0]["usage_estimate"]
    expected = {"input_tokens": str(estimate_tokens(INPUT_TEXT)), "output_tokens": "1024"}
    if estimate != expected:
        pytest.fail(f"usage_estimate={estimate}")


def test_deny_raises_before_any_openai_request(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    services.plan(PREBURN, CHECK_PATH, PlannedAnswer(200, make_check_body("deny")))
    with pytest.raises(DecisionDenied) as raised:
        wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    if raised.value.decision.decision_id != DECISION_ID:
        pytest.fail(f"decision_id={raised.value.decision.decision_id}")
    if services.paths() != [(PREBURN, CHECK_PATH)]:
        pytest.fail(f"paths={services.paths()}")


def test_route_swaps_the_model(services: FakeServices, wrapped: WrappedOpenAI) -> None:
    services.plan(
        PREBURN, CHECK_PATH, PlannedAnswer(200, make_check_body("route", OPENAI, ROUTE_MODEL))
    )
    wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    call = services.bodies(OPENAI, CHAT_PATH)[0]
    if call["model"] != ROUTE_MODEL:
        pytest.fail(f"model={call['model']}")
    if services.bodies(PREBURN, CHECK_PATH)[0]["model"] != MODEL:
        pytest.fail(f"check={services.bodies(PREBURN, CHECK_PATH)[0]}")


def test_route_to_another_provider_releases_and_raises(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    route = make_check_body("route", "anthropic", "claude-sonnet-5")
    services.plan(PREBURN, CHECK_PATH, PlannedAnswer(200, route))
    with pytest.raises(DecisionNotApplicableError) as raised:
        wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    if raised.value.decision.provider != "anthropic":
        pytest.fail(f"provider={raised.value.decision.provider}")
    if services.paths() != [(PREBURN, CHECK_PATH), (PREBURN, RELEASE_PATH)]:
        pytest.fail(f"paths={services.paths()}")
    if services.bodies(PREBURN, RELEASE_PATH) != [{"decision_id": DECISION_ID}]:
        pytest.fail(f"release={services.bodies(PREBURN, RELEASE_PATH)}")


@pytest.mark.parametrize(
    ("path", "call_parameters", "limit_parameter", "limit"),
    [
        (CHAT_PATH, {"messages": MESSAGES}, "max_completion_tokens", 512),
        (
            CHAT_PATH,
            {"messages": MESSAGES, "max_completion_tokens": 4096},
            "max_completion_tokens",
            512,
        ),
        (CHAT_PATH, {"messages": MESSAGES, "max_tokens": 4096}, "max_tokens", 512),
        (CHAT_PATH, {"messages": MESSAGES, "max_tokens": 100}, "max_tokens", 100),
        (RESPONSES_PATH, {"input": INPUT_TEXT}, "max_output_tokens", 512),
        (
            RESPONSES_PATH,
            {"input": INPUT_TEXT, "max_output_tokens": 4096},
            "max_output_tokens",
            512,
        ),
    ],
)
def test_cap_sets_the_limit_parameter_the_call_uses(
    services: FakeServices,
    wrapped: WrappedOpenAI,
    path: str,
    call_parameters: dict[str, Any],
    limit_parameter: str,
    limit: int,
) -> None:
    cap = make_check_body("cap", overrides={"max_completion_tokens": 512})
    services.plan(PREBURN, CHECK_PATH, PlannedAnswer(200, cap))
    if path == CHAT_PATH:
        wrapped.chat.completions.create(model=MODEL, preburn=CONTEXT, **call_parameters)
    else:
        wrapped.responses.create(model=MODEL, preburn=CONTEXT, **call_parameters)
    call = services.bodies(OPENAI, path)[0]
    limits = {name: value for name, value in call.items() if name.startswith("max_")}
    if limits != {limit_parameter: limit}:
        pytest.fail(f"limits={limits}")


def test_cap_with_an_override_the_wrapper_cannot_apply_releases_and_raises(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    cap = make_check_body("cap", overrides={"audio": False})
    services.plan(PREBURN, CHECK_PATH, PlannedAnswer(200, cap))
    with pytest.raises(DecisionNotApplicableError):
        wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    if services.paths() != [(PREBURN, CHECK_PATH), (PREBURN, RELEASE_PATH)]:
        pytest.fail(f"paths={services.paths()}")


def test_provider_error_releases_and_reraises(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    error_body = {"error": {"message": "server error", "type": "server_error"}}
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(500, error_body))
    with pytest.raises(openai.InternalServerError):
        wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, RELEASE_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    if services.bodies(PREBURN, RELEASE_PATH) != [{"decision_id": DECISION_ID}]:
        pytest.fail(f"release={services.bodies(PREBURN, RELEASE_PATH)}")


def test_provider_error_is_reraised_when_the_release_fails(
    services: FakeServices, wrapped: WrappedOpenAI, caplog: pytest.LogCaptureFixture
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(500, {"error": {"message": "server error"}}))
    services.plan(PREBURN, RELEASE_PATH, PlannedAnswer(503))
    with (
        caplog.at_level(logging.WARNING, logger="preburn"),
        pytest.raises(openai.InternalServerError),
    ):
        wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    expected = f"wrapper.release_failed decision_id={DECISION_ID} feature=chat error=APIError"
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")


def test_unreachable_preburn_falls_back_and_reports_the_call(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    services.plan(PREBURN, CHECK_PATH, PlannedAnswer(503))
    wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, REPORT_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    report = services.bodies(PREBURN, REPORT_PATH)[0]
    reported = (report["decision_source"], report["model"], report["usage"])
    if reported != ("fallback", MODEL, SENT_CHAT_USAGE) or not report["idempotency_key"]:
        pytest.fail(f"report={report}")


def test_failed_report_still_returns_the_openai_response(
    services: FakeServices, wrapped: WrappedOpenAI, caplog: pytest.LogCaptureFixture
) -> None:
    services.plan(PREBURN, REPORT_PATH, PlannedAnswer(503))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        completion = wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, preburn=CONTEXT
        )
    if completion.id != "chatcmpl-1":
        pytest.fail(f"completion={completion!r}")
    expected = f"wrapper.report_failed decision_id={DECISION_ID} feature=chat error=APIError"
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")


def test_responses_usage_without_token_details_reports_no_cached_tokens(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    usage = {"input_tokens": 40, "output_tokens": 5, "total_tokens": 45}
    services.plan(OPENAI, RESPONSES_PATH, PlannedAnswer(200, {**RESPONSE, "usage": usage}))
    response = wrapped.responses.create(model=MODEL, input=INPUT_TEXT, preburn=CONTEXT)
    if response.id != "resp_1":
        pytest.fail(f"response={response!r}")
    expected = {"input_tokens": "40", "cached_input_tokens": "0", "output_tokens": "5"}
    if [report["usage"] for report in services.bodies(PREBURN, REPORT_PATH)] != [expected]:
        pytest.fail(f"reports={services.bodies(PREBURN, REPORT_PATH)}")


def test_usage_that_cannot_be_mapped_releases_and_returns_the_result(
    services: FakeServices, wrapped: WrappedOpenAI, caplog: pytest.LogCaptureFixture
) -> None:
    usage = {**CHAT_USAGE, "prompt_tokens": 10, "prompt_tokens_details": {"cached_tokens": 20}}
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, {**CHAT_COMPLETION, "usage": usage}))
    with caplog.at_level(logging.WARNING, logger="preburn"):
        completion = wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, preburn=CONTEXT
        )
    if completion.id != "chatcmpl-1":
        pytest.fail(f"completion={completion!r}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, RELEASE_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    expected = f"wrapper.report_failed decision_id={DECISION_ID} feature=chat error=ValueError"
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")


def test_closed_preburn_client_never_replaces_the_openai_result(
    services: FakeServices, caplog: pytest.LogCaptureFixture
) -> None:
    preburn = make_preburn(services, "buffered")

    def close_preburn_then_answer(request: httpx2.Request) -> httpx2.Response:
        preburn.close()
        return services.handle_openai(request)

    with preburn, make_openai(services, close_preburn_then_answer) as client:
        wrapped = wrap_openai(client, preburn)
        with caplog.at_level(logging.WARNING, logger="preburn"):
            completion = wrapped.chat.completions.create(
                model=MODEL, messages=MESSAGES, preburn=CONTEXT
            )
    if completion.id != "chatcmpl-1":
        pytest.fail(f"completion={completion!r}")
    expected = f"wrapper.report_failed decision_id={DECISION_ID} feature=chat error=RuntimeError"
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")


def test_stream_outlives_a_closed_preburn_client(
    services: FakeServices, caplog: pytest.LogCaptureFixture
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()))
    with make_preburn(services, "buffered") as preburn, make_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        stream = wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
        )
        chunks = [next(stream)]
        preburn.close()
        with caplog.at_level(logging.WARNING, logger="preburn"):
            chunks.extend(stream)
    if len(chunks) != CHAT_STREAM_LENGTH:
        pytest.fail(f"chunks={len(chunks)}")
    expected = f"wrapper.report_failed decision_id={DECISION_ID} feature=chat error=RuntimeError"
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")


def test_buffered_preburn_client_reports_calls_in_a_batch(services: FakeServices) -> None:
    services.plan(OPENAI, RESPONSES_PATH, PlannedAnswer(200, events=make_response_stream()))
    with make_preburn(services, "buffered") as preburn, make_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
        events = list(
            wrapped.responses.create(model=MODEL, input=INPUT_TEXT, stream=True, preburn=CONTEXT)
        )
        if services.bodies(PREBURN, REPORTS_PATH) or services.bodies(PREBURN, REPORT_PATH):
            pytest.fail(f"paths={services.paths()}")
    if len(events) != len(make_response_stream()):
        pytest.fail(f"events={len(events)}")
    batches = services.bodies(PREBURN, REPORTS_PATH)
    usages = [report["usage"] for report in batches[0]["reports"]]
    if len(batches) != 1 or usages != [SENT_CHAT_USAGE, SENT_RESPONSE_USAGE]:
        pytest.fail(f"batches={batches}")


def test_settle_failures_keep_the_api_key_out_of_logs(
    services: FakeServices, caplog: pytest.LogCaptureFixture
) -> None:
    api_key = make_preburn_api_key()
    services.plan(PREBURN, REPORT_PATH, PlannedAnswer(401, {"code": "authentication_required"}))
    services.plan(PREBURN, RELEASE_PATH, PlannedAnswer(401, {"code": "authentication_required"}))
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()[:2]))
    with (
        make_preburn(services, api_key=api_key) as preburn,
        make_openai(services) as client,
        caplog.at_level(logging.DEBUG, logger="preburn"),
    ):
        wrapped = wrap_openai(client, preburn)
        wrapped.responses.create(model=MODEL, input=INPUT_TEXT, preburn=CONTEXT)
        list(
            wrapped.chat.completions.create(
                model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
            )
        )
    if len(caplog.records) != 2:
        pytest.fail(f"messages={caplog.messages}")
    formatter = logging.Formatter()
    secret = api_key.removeprefix("pb_test_runtime_")
    if any(secret in formatter.format(record) for record in caplog.records):
        pytest.fail("api key in a log record")


def test_streaming_reports_once_from_the_usage_chunk(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()))
    stream = wrapped.chat.completions.create(
        model=MODEL,
        messages=MESSAGES,
        stream=True,
        stream_options={"include_obfuscation": False},
        preburn=CONTEXT,
    )
    reports_at_usage_chunk = -1
    chunks: list[ChatCompletionChunk] = []
    for chunk in stream:
        chunks.append(chunk)
        if chunk.usage is not None:
            reports_at_usage_chunk = len(services.bodies(PREBURN, REPORT_PATH))
    if len(chunks) != CHAT_STREAM_LENGTH or reports_at_usage_chunk != 1:
        pytest.fail(f"chunks={len(chunks)} reports_at_usage_chunk={reports_at_usage_chunk}")
    call = services.bodies(OPENAI, CHAT_PATH)[0]
    expected_options = {"include_obfuscation": False, "include_usage": True}
    if (call["stream"], call["stream_options"]) != (True, expected_options):
        pytest.fail(f"call={call}")
    stream.close()
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, REPORT_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    if services.bodies(PREBURN, REPORT_PATH)[0]["usage"] != SENT_CHAT_USAGE:
        pytest.fail(f"report={services.bodies(PREBURN, REPORT_PATH)}")


def test_stream_closed_early_releases(services: FakeServices, wrapped: WrappedOpenAI) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()))
    with wrapped.chat.completions.create(
        model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
    ) as stream:
        first = next(stream)
    if first.choices[0].delta.content != "Hel":
        pytest.fail(f"first={first!r}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, RELEASE_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")


def test_stream_ending_without_usage_releases(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()[:2]))
    chunks = list(
        wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
        )
    )
    if len(chunks) != 2:
        pytest.fail(f"chunks={len(chunks)}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, RELEASE_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")


def test_responses_stream_reports_from_the_completed_event(
    services: FakeServices, wrapped: WrappedOpenAI
) -> None:
    services.plan(OPENAI, RESPONSES_PATH, PlannedAnswer(200, events=make_response_stream()))
    events = list(
        wrapped.responses.create(model=MODEL, input=INPUT_TEXT, stream=True, preburn=CONTEXT)
    )
    if [event.type for event in events] != [event["type"] for event in make_response_stream()]:
        pytest.fail(f"events={[event.type for event in events]}")
    if "stream_options" in services.bodies(OPENAI, RESPONSES_PATH)[0]:
        pytest.fail(f"call={services.bodies(OPENAI, RESPONSES_PATH)[0]}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, RESPONSES_PATH), (PREBURN, REPORT_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    if services.bodies(PREBURN, REPORT_PATH)[0]["usage"] != SENT_RESPONSE_USAGE:
        pytest.fail(f"report={services.bodies(PREBURN, REPORT_PATH)}")


def test_other_attributes_pass_through(services: FakeServices) -> None:
    with make_preburn(services) as preburn, make_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        if wrapped.models is not client.models or wrapped.api_key != client.api_key:
            pytest.fail("client attributes not passed through")
        if wrapped.chat.completions.list != client.chat.completions.list:
            pytest.fail("chat completions attributes not passed through")
        if wrapped.responses.retrieve != client.responses.retrieve:
            pytest.fail("responses attributes not passed through")


@pytest.mark.asyncio
async def test_wrap_openai_rejects_a_sync_and_async_mix() -> None:
    services = build_services()
    async with make_async_preburn(services) as async_preburn:
        with make_openai(services) as client, pytest.raises(TypeError):
            wrap_openai(client, async_preburn)  # type: ignore[call-overload]


@pytest.mark.asyncio
async def test_async_chat_completion_checks_calls_and_reports_usage(
    services: FakeServices,
) -> None:
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        if not isinstance(wrapped, AsyncWrappedOpenAI):
            pytest.fail(f"wrapped={wrapped!r}")
        completion = await wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, max_completion_tokens=400, preburn=CONTEXT
        )
    if completion.id != "chatcmpl-1":
        pytest.fail(f"completion={completion!r}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, REPORT_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    estimate = services.bodies(PREBURN, CHECK_PATH)[0]["usage_estimate"]
    expected_input = str(estimate_tokens(SYSTEM_TEXT, USER_TEXT))
    if estimate != {"input_tokens": expected_input, "output_tokens": "400"}:
        pytest.fail(f"usage_estimate={estimate}")
    report = services.bodies(PREBURN, REPORT_PATH)[0]
    if (report["decision_id"], report["usage"]) != (DECISION_ID, SENT_CHAT_USAGE):
        pytest.fail(f"report={report}")


@pytest.mark.asyncio
async def test_async_responses_create_reports_cached_input_tokens(
    services: FakeServices,
) -> None:
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        await wrapped.responses.create(model=MODEL, input=INPUT_TEXT, preburn=CONTEXT)
    report = services.bodies(PREBURN, REPORT_PATH)[0]
    if (report["decision_id"], report["usage"]) != (DECISION_ID, SENT_RESPONSE_USAGE):
        pytest.fail(f"report={report}")


@pytest.mark.asyncio
async def test_async_provider_error_releases_and_reraises(services: FakeServices) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(500, {"error": {"message": "server error"}}))
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        with pytest.raises(openai.InternalServerError):
            await wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, RELEASE_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")


@pytest.mark.asyncio
async def test_async_deny_raises_before_any_openai_request(services: FakeServices) -> None:
    services.plan(PREBURN, CHECK_PATH, PlannedAnswer(200, make_check_body("deny")))
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        with pytest.raises(DecisionDenied):
            await wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
    if services.paths() != [(PREBURN, CHECK_PATH)]:
        pytest.fail(f"paths={services.paths()}")


@pytest.mark.asyncio
async def test_async_streaming_reports_once_from_the_usage_chunk(
    services: FakeServices,
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()))
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        stream = await wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
        )
        chunks = [chunk async for chunk in stream]
    if len(chunks) != CHAT_STREAM_LENGTH:
        pytest.fail(f"chunks={len(chunks)}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, REPORT_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")
    if services.bodies(PREBURN, REPORT_PATH)[0]["usage"] != SENT_CHAT_USAGE:
        pytest.fail(f"report={services.bodies(PREBURN, REPORT_PATH)}")


@pytest.mark.asyncio
async def test_async_stream_outlives_a_closed_preburn_client(
    services: FakeServices, caplog: pytest.LogCaptureFixture
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()))
    preburn = make_async_preburn(services, report_mode="buffered")
    async with make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        stream = await wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
        )
        chunks = [await anext(stream)]
        await preburn.aclose()
        with caplog.at_level(logging.WARNING, logger="preburn"):
            chunks.extend([chunk async for chunk in stream])
    if len(chunks) != CHAT_STREAM_LENGTH:
        pytest.fail(f"chunks={len(chunks)}")
    expected = f"wrapper.report_failed decision_id={DECISION_ID} feature=chat error=RuntimeError"
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")


@pytest.mark.asyncio
async def test_async_buffered_preburn_client_reports_calls_in_a_batch(
    services: FakeServices,
) -> None:
    async with (
        make_async_preburn(services, report_mode="buffered") as preburn,
        make_async_openai(services) as client,
    ):
        wrapped = wrap_openai(client, preburn)
        await wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
        if services.bodies(PREBURN, REPORTS_PATH) or services.bodies(PREBURN, REPORT_PATH):
            pytest.fail(f"paths={services.paths()}")
    batches = services.bodies(PREBURN, REPORTS_PATH)
    if len(batches) != 1 or batches[0]["reports"][0]["usage"] != SENT_CHAT_USAGE:
        pytest.fail(f"batches={batches}")


@pytest.mark.asyncio
async def test_async_stream_closed_early_releases(services: FakeServices) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()))
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        async with await wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
        ) as stream:
            first = await anext(stream)
    if first.choices[0].delta.content != "Hel":
        pytest.fail(f"first={first!r}")
    expected_paths = [(PREBURN, CHECK_PATH), (OPENAI, CHAT_PATH), (PREBURN, RELEASE_PATH)]
    if services.paths() != expected_paths:
        pytest.fail(f"paths={services.paths()}")


@pytest.mark.parametrize(
    ("path", "call_parameters"),
    [
        (RESPONSES_PATH, {"input": INPUT_TEXT}),
        (RESPONSES_PATH, {"input": INPUT_TEXT, "model": None}),
        (RESPONSES_PATH, {"input": INPUT_TEXT, "model": openai.omit}),
        (CHAT_PATH, {"messages": MESSAGES}),
    ],
)
def test_call_without_a_model_raises_before_any_request(
    services: FakeServices, wrapped: WrappedOpenAI, path: str, call_parameters: dict[str, Any]
) -> None:
    operation = "responses.create" if path == RESPONSES_PATH else "chat.completions.create"
    with pytest.raises(PreburnError) as raised:
        if path == RESPONSES_PATH:
            wrapped.responses.create(preburn=CONTEXT, **call_parameters)
        else:
            wrapped.chat.completions.create(preburn=CONTEXT, **call_parameters)
    if raised.value.code != "model_missing" or operation not in raised.value.detail:
        pytest.fail(f"code={raised.value.code} detail={raised.value.detail}")
    if services.requests:
        pytest.fail(f"paths={services.paths()}")


@pytest.mark.asyncio
async def test_async_call_without_a_model_raises_before_any_request(
    services: FakeServices,
) -> None:
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        with pytest.raises(PreburnError) as raised:
            await wrapped.responses.create(input=INPUT_TEXT, preburn=CONTEXT)
    if raised.value.code != "model_missing" or services.requests:
        pytest.fail(f"code={raised.value.code} paths={services.paths()}")


@pytest.mark.asyncio
async def test_async_cancelled_call_releases_before_the_cancellation_propagates(
    services: FakeServices,
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, held=True))
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        call = asyncio.create_task(
            wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
        )
        await services.hold_reached.wait()
        call.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(call, CANCELLATION_DEADLINE_SECONDS)
        released = services.bodies(PREBURN, RELEASE_PATH)
    if released != [{"decision_id": DECISION_ID}]:
        pytest.fail(f"paths={services.paths()}")


@pytest.mark.asyncio
async def test_async_release_after_a_cancellation_waits_at_most_the_check_timeout(
    services: FakeServices, caplog: pytest.LogCaptureFixture
) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, held=True))
    services.plan(PREBURN, RELEASE_PATH, PlannedAnswer(204, held=True))
    async with (
        make_async_preburn(services, SHORT_CHECK_TIMEOUT_SECONDS) as preburn,
        make_async_openai(services) as client,
    ):
        wrapped = wrap_openai(client, preburn)
        call = asyncio.create_task(
            wrapped.chat.completions.create(model=MODEL, messages=MESSAGES, preburn=CONTEXT)
        )
        await services.hold_reached.wait()
        call.cancel()
        with (
            caplog.at_level(logging.WARNING, logger="preburn"),
            pytest.raises(asyncio.CancelledError),
        ):
            await asyncio.wait_for(call, CANCELLATION_DEADLINE_SECONDS)
    expected = f"wrapper.release_failed decision_id={DECISION_ID} feature=chat error=TimeoutError"
    if caplog.messages != [expected]:
        pytest.fail(f"messages={caplog.messages}")
    if services.paths()[-1] != (PREBURN, RELEASE_PATH):
        pytest.fail(f"paths={services.paths()}")


@pytest.mark.asyncio
async def test_async_cancelled_stream_read_releases(services: FakeServices) -> None:
    services.plan(OPENAI, CHAT_PATH, PlannedAnswer(200, events=make_chat_stream()[:1], held=True))
    async with make_async_preburn(services) as preburn, make_async_openai(services) as client:
        wrapped = wrap_openai(client, preburn)
        stream = await wrapped.chat.completions.create(
            model=MODEL, messages=MESSAGES, stream=True, preburn=CONTEXT
        )
        first = await anext(stream)
        read = asyncio.ensure_future(anext(stream))
        await services.hold_reached.wait()
        read.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(read, CANCELLATION_DEADLINE_SECONDS)
        released = services.bodies(PREBURN, RELEASE_PATH)
        await stream.close()
    if first.choices[0].delta.content != "Hel":
        pytest.fail(f"first={first!r}")
    if released != [{"decision_id": DECISION_ID}] or services.bodies(PREBURN, REPORT_PATH):
        pytest.fail(f"paths={services.paths()}")
