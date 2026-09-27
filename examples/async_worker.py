"""Handle jobs concurrently with the async client, which buffers reports and sends them in batches.

Run it against a Preburn server with a runtime API key:

    export PREBURN_BASE_URL=http://localhost:8080
    export PREBURN_API_KEY=pb_test_runtime_...
    python examples/async_worker.py

`run_model` stands in for your provider call, so the example needs no provider key. Leaving
`async with` closes the client, which sends the buffered reports. An async client that is never
closed with `aclose()` loses its pending reports at exit.
"""

import asyncio
import math
from collections.abc import Mapping

from preburn import AsyncPreburn, AttributeValue

FEATURE = "ticket_summary"
PROVIDER = "openai"
MODEL = "gpt-6-luna"
OUTPUT_TOKEN_LIMIT = 300
CHARACTERS_PER_TOKEN = 4
MODEL_LATENCY_SECONDS = 0.1
JOBS = (
    ("example-customer", "Summarize ticket 1041 for the account owner."),
    ("example-customer", "Summarize ticket 1042 for the account owner."),
    ("example-customer-2", "Summarize ticket 2210 for the on-call engineer."),
    ("example-customer-3", "Summarize ticket 3307 for the billing team."),
)


def count_prompt_tokens(prompt: str) -> int:
    return math.ceil(len(prompt) / CHARACTERS_PER_TOKEN)


async def run_model(
    provider: str, model: str, prompt: str, overrides: Mapping[str, AttributeValue]
) -> dict[str, int]:
    """Stands in for a provider call and returns the token usage the call would have.

    A real call sets each override on the provider parameter it maps to, such as
    `max_tokens` on OpenAI's `max_completion_tokens`.
    """
    print(f"call provider={provider} model={model} overrides={dict(overrides)}")
    await asyncio.sleep(MODEL_LATENCY_SECONDS)
    return {"input_tokens": count_prompt_tokens(prompt), "output_tokens": OUTPUT_TOKEN_LIMIT // 2}


async def handle_job(preburn: AsyncPreburn, customer_id: str, prompt: str) -> None:
    decision = await preburn.check(
        customer_id,
        FEATURE,
        PROVIDER,
        MODEL,
        usage_estimate={
            "input_tokens": count_prompt_tokens(prompt),
            "output_tokens": OUTPUT_TOKEN_LIMIT,
        },
    )
    if decision.outcome == "deny":
        print(f"job denied customer_id={customer_id} reason={decision.reason}")
        return
    try:
        usage = await run_model(decision.provider, decision.model, prompt, decision.overrides)
    except Exception:
        await preburn.release(decision)
        raise
    await preburn.report(decision, usage)
    print(
        f"job done customer_id={customer_id} outcome={decision.outcome} "
        f"fallback={decision.is_fallback}"
    )


async def main() -> None:
    async with AsyncPreburn(report_mode="buffered") as preburn:
        await asyncio.gather(
            *(handle_job(preburn, customer_id, prompt) for customer_id, prompt in JOBS)
        )


if __name__ == "__main__":
    asyncio.run(main())
