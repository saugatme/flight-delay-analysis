# Agent instructions

## Scope

This repository analyses BTS flight data in WSL 2 and Ubuntu. Use uv-managed Python 3.12, Java 17, and local PySpark 4.2. Spark local mode uses one computer. It is not a distributed cluster.

Do not change raw data. Do not add raw or processed data to Git.

## Learning mode

Use learning mode for code work.

1. Explain non-trivial code before you write it.
2. Explain the design choice and one practical alternative.
3. Let the user make a first attempt.
4. Review the attempt for correctness, data quality, Spark behavior, security, readability, and tests.
5. Ask one short understanding question after code.
6. Do not continue until the user answers that question.

Keep explanations short. Use common words. State assumptions. Show the exact command to run when it helps.

## Safe work rules

- Inspect `git status` before edits.
- Preserve user changes.
- Use `uv run` for project commands.
- Run Ruff, relevant tests, and the Spark check after code changes.
- Treat the source CSV as input only.
- Write derived data to `data/processed/`.
- Write tables and charts to `outputs/`.
- Do not say that local Spark is a cluster.

## Data rules

- Expected input: BTS January 2025 CSV in `data/raw/2025-01/`.
- Expected input size: 539,747 records and 110 source columns.
- Confirm all 28 required columns before transformation.
- Keep row counts and key null checks in the pipeline.
- Record filters and denominator rules for every rate.

## Git rules

- Never commit data, secrets, `.venv`, Spark warehouse files, or generated outputs.
- Do not commit unless the user asks for a commit.
- Keep commits small and describe the user-visible result.
