# Contributing to the Preburn Python SDK

Everyone taking part in the project follows the [Code of Conduct](CODE_OF_CONDUCT.md). Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md), never in a public issue.

## Development setup

You need [uv](https://docs.astral.sh/uv/) and make. uv installs the package and the development tools pinned in `uv.lock`, and downloads Python when no installed version matches. `versions.env` pins the uv version CI uses and the default Python version.

| Command | What it does |
|---|---|
| `uv sync --all-extras` | Create `.venv` with the package, the `openai` extra and the development tools |
| `uv run ruff format` | Format the code |
| `uv run ruff check` | Lint the code |
| `uv run mypy` | Type check `src/`, `tests/` and `examples/` in strict mode |
| `uv run pytest tests/unit` | Run the unit tests |
| `uv lock` | Update `uv.lock` after a dependency change in `pyproject.toml` |

The make targets run the same commands through `uv run`. `make help` lists them.

- `make lint typecheck test` runs the checks CI runs.
- `make test PYTHON=3.10` runs a target with another Python version. CI runs 3.10 to 3.14.
- `make test DOCKER=1` runs a target in the official uv image, for contributors without Python. It needs Docker.
- `make test-contract` checks the requests and responses the SDK handles against the server's OpenAPI document.
- `make test-integration` runs against a live server named by `PREBURN_BASE_URL` and `PREBURN_API_KEY`.

## Sign off your commits

Preburn uses the [Developer Certificate of Origin](https://developercertificate.org) (DCO). By signing off a commit you certify that you wrote the change or have the right to submit it under the project's open-source license. You also acknowledge that the contribution and your sign-off, including your name and email, are public and kept indefinitely.

Sign off with `git commit -s`. Git adds a trailer with the name and email from your Git configuration:

```
Signed-off-by: Your Name <you@example.com>
```

To sign off commits already on your branch, run `git rebase --signoff main`. CI checks every commit in a pull request except merge commits and fails when one has no `Signed-off-by` trailer.

## Commit messages

Use the Conventional Commits format. Start the subject with a type:

- `feat:` a new feature
- `fix:` a bug fix
- `docs:` documentation only
- `chore:` build, tooling or maintenance

Example: `fix: keep buffered reports after a failed flush`. The release tooling reads these types to write the changelog and choose the next version.

## Pull requests

- Add or update tests for every change in behavior.
- `make lint typecheck test` must pass.
- After changing dependencies in `pyproject.toml`, run `uv lock` and commit `uv.lock`.
- Put ignores for your own editor or tools in `.git/info/exclude`, not in `.gitignore`.

## Code standards

### Python

- Format and lint with ruff. `pyproject.toml` sets a line length of 100 and the rule sets E, F, I, UP, B, SIM and RUF, plus pydocstyle with the Google convention for `src/`.
- Type check with mypy in strict mode over `src/`, `tests/` and `examples/`.
- Write Google-style docstrings on the public API. Modules whose names start with an underscore are private.
- Use full words for names, no abbreviations.
- Models are dataclasses. Money and quantities are `decimal.Decimal`, never `float`.
- Log through `logging.getLogger("preburn")` with key=value messages written as f-strings.
- Library code has no `assert` and no `print`.
- Unit tests use `httpx.MockTransport` and never reach the network.

### Writing

These rules apply to docs, docstrings, the README and log messages.

- No em dashes, no double hyphens used as dashes, no semicolons joining clauses, no unicode arrows or ellipses, no emoji.
- No filler, no cutesy phrasing, no "successfully" endings, no gratuitous exclamation marks.
- Short, direct, active sentences. Sentence case headings.
- Log messages are event names with key=value attributes and a lowercase first word.
