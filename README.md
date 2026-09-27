# Preburn Python SDK

Python client for [Preburn](https://github.com/preburn/preburn), a self-hosted service that decides before each AI call whether to allow it, route it to another model, cap it or deny it, based on each customer's margin. The SDK checks each call before it runs, reports its usage afterwards, and returns a fallback decision when Preburn cannot be reached.

It needs Python 3.10 to 3.14 and a running Preburn server. The [server README](https://github.com/preburn/preburn#readme) starts one with Docker Compose.

## Install

The SDK is not on PyPI. Install it from a git tag:

<!-- x-release-please-start-version -->
```sh
pip install "preburn @ git+https://github.com/preburn/sdk-python@v0.1.0"
```
<!-- x-release-please-end -->

With the OpenAI wrapper:

<!-- x-release-please-start-version -->
```sh
pip install "preburn[openai] @ git+https://github.com/preburn/sdk-python@v0.1.0"
```
<!-- x-release-please-end -->

The same requirement strings work as lines in `requirements.txt`.

## Quickstart

Create a runtime API key for the test environment. With the server's Docker Compose setup:

```sh
docker compose exec api /preburn admin api-key create --environment test --scope runtime --name my-app
```

The command prints only the key. Point the SDK at the server:

```sh
export PREBURN_BASE_URL=http://localhost:8080
export PREBURN_API_KEY=pb_test_runtime_...
```

Check each call before it runs, then report its usage:

```python
from preburn import Preburn

preburn = Preburn()

decision = preburn.check(
    "customer_42",
    "chat",
    "openai",
    "gpt-6-luna",
    usage_estimate={"input_tokens": 1200, "output_tokens": 400},
)
decision.raise_for_denial()

try:
    usage = run_model(decision.provider, decision.model, decision.overrides)
except Exception:
    preburn.release(decision)
    raise

preburn.report(decision, usage)
```

`run_model` stands for your provider call. It returns the measured usage, such as `{"input_tokens": 1180, "output_tokens": 312}`.

`check` takes:

- `customer_id`: the customer's id in your system. A check for an unknown id creates the customer without a plan, so it follows the environment's default plan.
- `feature`: the feature of your product the call serves. Lowercase letters, digits and underscores, starting with a letter, at most 64 characters.
- `provider` and `model`: where the call would run, named as in the pricing catalog.
- `usage_estimate`: the usage you expect by meter. Preburn reserves its cost.
- `usage_ceiling`: the most usage the call can reach by meter.
- `attributes`: request attributes that select the price, such as a resolution.
- `customer_user_id`: the id of the customer's user in your system.

The decision's `outcome` says what to do:

| Outcome | What to do |
|---|---|
| `allow` | Run the call as requested. |
| `route` | Run it on `decision.provider` and `decision.model`. |
| `cap` | Run it with `decision.overrides` applied. |
| `deny` | Do not run it. `raise_for_denial()` raises `DecisionDenied`. |

`decision.provider` and `decision.model` always name what to run. They are the requested ones unless the outcome is route, so passing them to your call covers allow and route alike. `decision.overrides` maps Preburn parameter names, such as `max_tokens`, `duration` or `audio`, to the values to use. Route decisions can carry overrides too. Each name sets a provider parameter that depends on the model, such as `max_completion_tokens` for `max_tokens` on OpenAI models. `GET /api/v1/policies/parameter-mappings` lists the mappings of each model and needs an admin key.

Every decision except deny holds its estimated cost against the customer's allowance until you report it, release it, or it expires at `decision.expires_at`. Report every call that ran and release every call that did not. A deny holds nothing, and the server answers 409 `decision_not_reportable` when you report one.

Usage maps meter names to quantities as `int` or `decimal.Decimal`, with at most 6 decimals. A `float` raises `TypeError`.

## Configuration

`Preburn` and `AsyncPreburn` take the same options:

| Option | Default | Meaning |
|---|---|---|
| `api_key` | `PREBURN_API_KEY` | API key. The key picks the environment: `pb_test_...` keys act on test data, `pb_live_...` keys on live data. A runtime key covers every SDK call. |
| `base_url` | `PREBURN_BASE_URL` | Server URL with `http` or `https`, such as `http://localhost:8080`. |
| `check_timeout` | `0.25` | Seconds a check may take in total before it falls back. See [Check timeout](#check-timeout). |
| `report_mode` | `"auto"` | `"sync"`, `"buffered"` or `"auto"`. See [Report modes](#report-modes). |
| `flush_interval` | `1.0` | Seconds between background flushes in buffered mode. |
| `batch_size` | `100` | Pending reports that start a flush before the interval ends. |
| `max_pending` | `10_000` | Pending reports kept while the server cannot be reached. |
| `transport` | `None` | Keyword only. An `httpx` transport for every request, such as a proxy or a mock. |

A missing or malformed key or base URL, or an invalid option, raises `ConfigurationError` when the client is created. An empty environment variable counts as unset. Requests other than checks allow 5 seconds for each network phase.

Without a `transport`, the client reads the proxy for `base_url` once, when it is created, from `HTTP_PROXY`, `HTTPS_PROXY`, `ALL_PROXY` and `NO_PROXY`, and on macOS and Windows from the system proxy settings. Change them before creating the client.

### Check timeout

Both clients apply `check_timeout` to the whole check, from resolving the server's host to reading the end of the answer, and fall back when it passes. `Preburn` sends each check from a pool of up to 100 threads, so a slow name lookup or connection never holds the caller past the deadline.

## Fallback

A check falls back instead of raising when the request fails with a transport error or a timeout, when the answer cannot be decoded or a 2xx answer is not JSON, or when the server answers 502, 503 or 504. The SDK then returns a fallback decision:

- `is_fallback` is `True`. `decision_id`, `reason`, `expires_at` and `signals` are `None`, and nothing is reserved.
- `outcome` is `allow` or `deny`: the last `fallback_outcome` the server sent for this customer and feature, else the last one for this feature, else `allow`.
- `provider` and `model` are the requested ones and `overrides` is empty.
- The SDK logs a `check.fallback` warning.

The server sends `fallback_outcome` in every check response. It is the `on_unreachable` setting, `allow` or `deny`, of the policy that matched the check, and `allow` when no policy matched. Set `on_unreachable` to `deny` on the policies of features that must never run unchecked. The client keeps these outcomes in memory, up to 10,000 entries with the least recently used dropped first. A new process therefore falls back to `allow` until its first checks succeed.

Report a fallback decision like any other. It goes out with `decision_source` `fallback` and an idempotency key the SDK created at the check, so reporting it twice stores one ledger entry. It counts in the customer's period that contains its `occurred_at`. Releasing a fallback decision sends nothing.

Other failures raise. See [Errors](#errors).

To report the usage of a call that had no check, pass `ReportFields`:

```python
from preburn import ReportFields

preburn.report(
    ReportFields(customer_id="customer_42", feature="chat", provider="openai", model="gpt-6-luna"),
    {"input_tokens": 900, "output_tokens": 210},
)
```

## Report modes

`report()` works in one of these modes:

- `sync` posts each report to `/api/v1/report` and returns a `ReportResult` with `ledger_entry_id`, `cost`, `cost_status` and `duplicate`. Server errors raise `PreburnError`, and a server that cannot be reached raises `httpx.HTTPError`.
- `buffered` queues the report and returns `None`. A background flusher sends batches of up to 500 reports and 1,000,000 bytes to `/api/v1/reports` every `flush_interval` seconds, and sooner once `batch_size` reports are pending. The sync client flushes from a daemon thread. The async client flushes from a task on the running event loop, started by the first report.
- `auto`, the default, picks `sync` when `AWS_LAMBDA_FUNCTION_NAME`, `K_SERVICE`, `FUNCTIONS_WORKER_RUNTIME` or `VERCEL` is set, because a serverless function can be frozen before a background flush runs. Everywhere else it picks `buffered`.

`report()` stamps `occurred_at` with the current time when you pass none, so a report the flusher sends later still counts in the period the call ran in. Pass a timezone-aware `datetime` to set it yourself. A naive one raises `ValueError`. An attribute value that is not a JSON type, such as a `Decimal`, raises `TypeError` before the report is queued or sent.

The SDK reports a server decision by its `decision_id`, so a second report of it changes nothing and returns `duplicate=True`. `attributes` passed to `report()` override the attributes of the check.

In buffered mode:

- A batch that cannot reach the server, whose answer cannot be decoded, or that gets 502, 503 or 504 is retried after 0.5, 1 and 2 seconds, then goes back to the front of the queue for the next flush.
- A batch answered with 429 or another 5xx status goes back to the queue without retries.
- A batch rejected with any other status, or answered with a 2xx body that is not JSON, is dropped, counted as dropped and logged as `reports.rejected`. A single report the server rejects inside a batch is dropped and logged the same way, unless it failed with 429 or a 5xx status, which puts it back in the queue.
- A batch lost to an answer the flusher cannot process is counted as dropped and logged as `reports.flush_failed`.
- A report too large for any batch is dropped, counted and logged as `reports.oversized`.
- The queue holds `max_pending` reports. When it is full, the oldest report is dropped and counted. The count goes out in the `Preburn-Dropped-Reports` header of the next batch, so the server records the loss.

Close the client to send what is pending:

- `flush()` sends the pending reports now. Reports that cannot be sent stay queued.
- `close()` sends the pending reports once more, stops the thread and closes the connections. Reports it cannot send are lost and logged as `reports.unsent`. `with Preburn() as preburn:` closes on exit. A report after `close()` raises `RuntimeError`.
- A sync client that is never closed flushes at interpreter exit, waiting at most 5 seconds. Reports still pending after that are lost and logged as `reports.flush_timed_out` or `reports.unsent`.

### Forked processes

A `Preburn` client created before `os.fork()`, as with gunicorn's `--preload`, Celery's prefork pool or `multiprocessing` with the fork start method, keeps working in the child. The child gets its own connections, an empty fallback cache and an empty report queue with its own flush thread, started by the child's first report. Reports the parent queued stay with the parent. The child never reads the system proxy settings, since that lookup crashes a forked child on macOS. It uses the proxy the parent read. A `transport` you pass is shared by both processes, so with a transport that holds connections, create the client after the fork. Python 3.12 and later print a `DeprecationWarning` when a process with running threads forks, which includes any process with a buffered client, because of its flush thread.

## Async client

```python
import asyncio

from preburn import AsyncPreburn


async def main() -> None:
    async with AsyncPreburn() as preburn:
        decision = await preburn.check(
            "customer_42",
            "chat",
            "openai",
            "gpt-6-luna",
            usage_estimate={"input_tokens": 1200, "output_tokens": 400},
        )
        decision.raise_for_denial()
        try:
            usage = await run_model(decision.provider, decision.model, decision.overrides)
        except Exception:
            await preburn.release(decision)
            raise
        await preburn.report(decision, usage)


asyncio.run(main())
```

`AsyncPreburn` takes the same options as `Preburn`. `check`, `report`, `release`, `flush`, `customers.upsert` and `revenue.record` are coroutines. Close the client with `await preburn.aclose()` or with `async with`. `aclose()` ends the flush task and sends the pending reports once more, with no time limit. Reports it cannot send are lost and logged as `reports.unsent`.

One `AsyncPreburn` can serve one event loop after another, such as one `asyncio.run()` per job. Each loop gets its own connections and flush task, except that a `transport` you pass serves every loop. Reports still pending when a loop ends go out with the next loop's flush or with `aclose()`.

The async client has no exit hook. An async client that is never closed with `aclose()` loses its pending reports at exit.

## OpenAI wrapper

The wrapper needs the `openai` extra.

```python
from openai import OpenAI

from preburn import CallContext, Preburn
from preburn.wrappers.openai import wrap_openai

preburn = Preburn()
client = wrap_openai(OpenAI(), preburn)

completion = client.chat.completions.create(
    model="gpt-6-luna",
    messages=[{"role": "user", "content": "Summarize this ticket."}],
    max_completion_tokens=400,
    preburn=CallContext(customer_id="customer_42", feature="support_summary"),
)
```

`wrap_openai` takes an `OpenAI` client with a `Preburn` client, or an `AsyncOpenAI` client with an `AsyncPreburn` client. Mixing them raises `TypeError`. The wrapper checks `chat.completions.create` and `responses.create`. Both take the extra keyword argument `preburn=CallContext(customer_id, feature, customer_user_id=None)`, which never reaches OpenAI. Every other method and attribute passes through to the OpenAI client unchecked.

Each checked call goes through these steps:

1. Estimate the usage. Input tokens are `ceil(characters / 4)` over message and input text, text content parts, `instructions` and the tool definitions as JSON. Output tokens are the call's `max_tokens`, `max_completion_tokens` or `max_output_tokens`, else 1024 (`DEFAULT_OUTPUT_TOKEN_ESTIMATE`).
2. Check the call with provider `openai` and the call's `model`. A call without `model` raises `PreburnError` with code `model_missing` and sends nothing.
3. Apply the decision:
   - Deny raises `DecisionDenied` before OpenAI is called.
   - Route replaces `model` with the target model when the target provider is `openai`. A route to another provider releases the decision and raises `DecisionNotApplicableError`.
   - A token limit override (`max_tokens`, `max_completion_tokens` or `max_output_tokens`) on a route or cap goes to the limit parameter the call uses: `max_tokens` when a chat call passes it, else `max_completion_tokens` for chat and `max_output_tokens` for responses. It only lowers the limit, so a call that asks for 20 tokens under a cap of 50 keeps 20. Any other override releases the decision and raises `DecisionNotApplicableError`.
   - Allow leaves the call as it is.
4. Call OpenAI. When the call raises, the wrapper releases the decision and re-raises the original exception. A cancelled async call releases the decision, waiting at most `check_timeout`, before the cancellation propagates.
5. Report the usage from the response: prompt or input tokens minus cached tokens as `input_tokens`, cached tokens as `cached_input_tokens`, completion or output tokens as `output_tokens`. Missing cached token details count as 0. A response without usage releases the decision instead.

When Preburn cannot be reached, the check falls back as described in [Fallback](#fallback). Other check errors raise before OpenAI is called. A failed report or release is logged as `wrapper.report_failed` or `wrapper.release_failed` and never replaces the OpenAI result, stream or error, even when the Preburn client closes during the call. Usage the wrapper cannot read, such as more cached tokens than input tokens, is logged as `wrapper.report_failed` and releases the decision. The wrapper sends no request attributes, such as `service_tier`, with the check.

### Streaming

With `stream=True` the wrapper returns a stream of OpenAI's chunks or events that reports once, when the usage arrives. For chat completions it adds `include_usage` to your `stream_options` and reports from the final usage chunk. For responses it reports from the `response.completed`, `response.incomplete` or `response.failed` event. A stream that ends, fails or is closed without usage releases the decision.

If you stop reading early, close the stream or read it inside `with` (`async with` for async clients). Otherwise the reservation stays until it expires.

```python
with client.chat.completions.create(
    model="gpt-6-luna",
    messages=[{"role": "user", "content": "Write one sentence about budgets."}],
    stream=True,
    preburn=CallContext(customer_id="customer_42", feature="chat"),
) as stream:
    for chunk in stream:
        for choice in chunk.choices:
            print(choice.delta.content or "", end="")
```

## Customers and revenue

```python
from datetime import datetime, timezone
from decimal import Decimal

customer = preburn.customers.upsert("customer_42", display_name="Acme", metadata={"crm_id": "42"})

entry = preburn.revenue.record(
    "customer_42",
    "subscription",
    Decimal("49.00"),
    period_start=datetime(2026, 9, 1, tzinfo=timezone.utc),
    period_end=datetime(2026, 10, 1, tzinfo=timezone.utc),
    source_reference="in_1042",
)
```

- `customers.upsert` creates the customer or replaces all of its fields. A `display_name` or `plan_id` left as `None` is cleared, and a customer without a plan follows the environment's default plan. `metadata` left as `None` stores an empty object.
- `revenue.record` adds revenue to the customer's margin. `subscription` and `adjustment` add their amount to net revenue, and `stripe_fee`, `refund` and `credit_note` subtract it. Amounts are non-negative `Decimal` or `int` USD values with at most 9 decimals. Periods are timezone-aware, and `period_end` equals `period_start` for a one-time line. Recording the same `source_reference` again returns the stored entry with `duplicate=True`.
- Both send at once, never buffered, and raise on any failure, including `httpx.HTTPError` when the server cannot be reached.

## Money and time

Amounts come back as `decimal.Decimal` USD values, exact to 9 decimals. `signals.pace` is `Decimal("Infinity")` for cost against a zero allowance, and `signals.projected_margin` is `Decimal("-Infinity")` for cost without revenue. Timestamps come back as timezone-aware UTC `datetime` values.

## Errors

Every error the SDK raises derives from `PreburnError`. It carries `status` (`None` when the SDK raised it without a response), `code`, `detail` and `errors`, a tuple of `InvalidField(location, message)`. Match on `code`. The server's codes are listed in its [errors reference](https://github.com/preburn/preburn/blob/main/docs/errors.md).

| Error | Raised when |
|---|---|
| `ConfigurationError` | An option or environment variable is missing or invalid when the client is created. Code `configuration_invalid`. |
| `AuthenticationError` | The server answers 401: the key is missing, unknown or revoked. |
| `ScopeError` | The server answers 403: the key may not call the route. |
| `ValidationError` | The server answers 422. `errors` names each invalid field. |
| `RateLimitError` | The server answers 429. `retry_after` holds the seconds to wait. |
| `APIError` | The server answers any other error status. A body that is not a problem document, a success body that is not JSON, or an answer that cannot be decoded gives code `unexpected_response`. |
| `DecisionDenied` | `raise_for_denial()` or a wrapped call meets a deny. `decision` holds the decision. Code `decision_denied`. |
| `DecisionNotApplicableError` | The OpenAI wrapper cannot apply a decision, such as a route to another provider or an override it does not know. The decision is released first. `decision` holds it. Code `decision_not_applicable`. |
| `PreburnError` with code `model_missing` | A wrapped call has no `model`. Nothing was sent. |

Checks fall back on transport errors, timeouts, answers they cannot read and 502, 503 or 504 answers instead of raising. Releases, sync reports, customer upserts and revenue records raise `httpx.HTTPError` when the server cannot be reached. Invalid quantities, amounts and naive datetimes raise `ValueError`. Floats as quantities or amounts, and attribute values that are not JSON types, raise `TypeError`.

## Logging

The SDK logs to the `preburn` logger of the standard `logging` module. Each message is an event name followed by `key=value` pairs, and no message holds the API key.

| Event | Level | Meaning |
|---|---|---|
| `check.fallback` | warning | A check fell back. Carries `feature`, `outcome` and the cause. |
| `reports.requeued` | warning | Reports went back to the queue after a failed send. |
| `reports.rejected` | warning | The server rejected reports, or answered a batch with a body that is not JSON, and they were dropped. |
| `reports.oversized` | warning | A report too large for any batch was dropped. Carries `bytes` and `maximum_bytes`. |
| `reports.flush_failed` | error | A background flush raised an unexpected exception. The batch it was sending counts as dropped. |
| `reports.unsent` | warning | Reports were still pending when the client closed. |
| `reports.flush_timed_out` | warning | The sync client's exit flush did not finish within 5 seconds. |
| `wrapper.report_failed` | warning | The OpenAI wrapper could not report a call. |
| `wrapper.release_failed` | warning | The OpenAI wrapper could not release a decision. |

## Examples

[`examples/`](examples) holds scripts that run against a Preburn server. Set `PREBURN_BASE_URL` and `PREBURN_API_KEY` first.

| Script | What it shows |
|---|---|
| [`record_revenue.py`](examples/record_revenue.py) | A customer upsert and a monthly subscription entry. A second run in the same month returns the stored entry. |
| [`check_and_report.py`](examples/check_and_report.py) | A check, a stand-in model call and a sync report. |
| [`async_worker.py`](examples/async_worker.py) | Concurrent checks with `AsyncPreburn`, and buffered reports sent when the client closes. |
| [`openai_chat.py`](examples/openai_chat.py) | The OpenAI wrapper on a chat completion and a streamed one. Needs the `openai` extra and your own `OPENAI_API_KEY`, which pays for the calls. |

```sh
python examples/record_revenue.py
python examples/check_and_report.py
```

From a clone of this repository, `uv run --all-extras python examples/check_and_report.py` runs a script with the package installed.

## Versioning

Releases follow [semantic versioning](https://semver.org). Before 1.0.0 a minor release can change the API, so pin a tag in the install line. [CHANGELOG.md](CHANGELOG.md) lists the changes in each release.

The SDK calls version 1 of the server API (`/api/v1`). Its contract tests check the requests it builds and the responses it parses against the OpenAPI document of the server release named in [`tests/contract/SERVER_VERSION`](tests/contract/SERVER_VERSION).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues as described in [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE). See [NOTICE](NOTICE).
