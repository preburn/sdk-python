"""Wrap an OpenAI client so Preburn checks each chat completion and reports its token usage.

Needs the `openai` extra and your own OpenAI API key, which pays for the two calls:

    export PREBURN_BASE_URL=http://localhost:8080
    export PREBURN_API_KEY=pb_test_runtime_...
    export OPENAI_API_KEY=...
    python examples/openai_chat.py

The OpenAI client also reads `OPENAI_BASE_URL`, so the calls can go to an OpenAI-compatible
server or a local mock instead.
"""

from openai import OpenAI

from preburn import CallContext, DecisionDenied, Preburn
from preburn.wrappers.openai import wrap_openai

MODEL = "gpt-6-luna"
OUTPUT_TOKEN_LIMIT = 200
CONTEXT = CallContext(
    customer_id="example-customer", feature="chat", customer_user_id="example-user"
)


def main() -> None:
    with Preburn() as preburn, OpenAI() as openai_client:
        client = wrap_openai(openai_client, preburn)
        try:
            completion = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": "Name three ways to cut AI costs."}],
                max_completion_tokens=OUTPUT_TOKEN_LIMIT,
                preburn=CONTEXT,
            )
            print(completion.choices[0].message.content)
            with client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": "Write one sentence about budgets."}],
                max_completion_tokens=OUTPUT_TOKEN_LIMIT,
                stream=True,
                preburn=CONTEXT,
            ) as stream:
                for chunk in stream:
                    for choice in chunk.choices:
                        print(choice.delta.content or "", end="")
            print()
        except DecisionDenied as denied:
            print(f"denied reason={denied.decision.reason}")


if __name__ == "__main__":
    main()
