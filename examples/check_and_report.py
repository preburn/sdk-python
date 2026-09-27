"""Check a model call with Preburn, run it with the decision applied, then report its usage.

Run it against a Preburn server with a runtime API key:

    export PREBURN_BASE_URL=http://localhost:8080
    export PREBURN_API_KEY=pb_test_runtime_...
    python examples/check_and_report.py

`run_model` stands in for your provider call, so the example needs no provider key.
"""

import math
from collections.abc import Mapping

from preburn import AttributeValue, Preburn

CUSTOMER_ID = "example-customer"
FEATURE = "chat"
PROVIDER = "openai"
MODEL = "gpt-6-luna"
PROMPT = "Summarize the last three support tickets in two sentences."
OUTPUT_TOKEN_LIMIT = 400
CHARACTERS_PER_TOKEN = 4


def count_prompt_tokens(prompt: str) -> int:
    return math.ceil(len(prompt) / CHARACTERS_PER_TOKEN)


def run_model(
    provider: str, model: str, prompt: str, overrides: Mapping[str, AttributeValue]
) -> dict[str, int]:
    """Stands in for a provider call and returns the token usage the call would have.

    A real call sets each override on the provider parameter it maps to, such as
    `max_tokens` on OpenAI's `max_completion_tokens`.
    """
    print(f"call provider={provider} model={model} overrides={dict(overrides)}")
    return {"input_tokens": count_prompt_tokens(prompt), "output_tokens": OUTPUT_TOKEN_LIMIT // 2}


def main() -> None:
    with Preburn(report_mode="sync") as preburn:
        decision = preburn.check(
            CUSTOMER_ID,
            FEATURE,
            PROVIDER,
            MODEL,
            usage_estimate={
                "input_tokens": count_prompt_tokens(PROMPT),
                "output_tokens": OUTPUT_TOKEN_LIMIT,
            },
        )
        print(
            f"decision outcome={decision.outcome} reason={decision.reason} "
            f"reserved={decision.reserved_amount} fallback={decision.is_fallback}"
        )
        if decision.outcome == "deny":
            return
        try:
            usage = run_model(decision.provider, decision.model, PROMPT, decision.overrides)
        except Exception:
            preburn.release(decision)
            raise
        result = preburn.report(decision, usage)
        print(f"report {result}")


if __name__ == "__main__":
    main()
