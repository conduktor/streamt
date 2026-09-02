"""Keep gradual mypy adoption from regressing while legacy errors are reduced."""

from __future__ import annotations

import sys

from mypy import api

MAX_ERRORS = 0


def main() -> int:
    """Run mypy and fail only when the checked-in error ceiling is exceeded."""
    stdout, stderr, exit_status = api.run(["src/streamt", "--no-error-summary"])
    output = stdout + stderr
    sys.stdout.write(output)

    if exit_status not in (0, 1):
        sys.stderr.write(f"mypy failed to run (exit {exit_status})\n")
        return exit_status

    error_count = sum(": error:" in line for line in output.splitlines())
    sys.stdout.write(f"mypy baseline: {error_count} errors (maximum {MAX_ERRORS})\n")
    if error_count > MAX_ERRORS:
        sys.stderr.write(
            "The mypy error count increased. Fix the new errors or, if this is an "
            "intentional baseline reset, update MAX_ERRORS with an explanation.\n"
        )
        return 1
    if error_count < MAX_ERRORS:
        sys.stdout.write(f"Baseline can be lowered to {error_count}.\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
