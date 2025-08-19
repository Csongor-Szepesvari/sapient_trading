# Module: Platform and Ops

First-pass consolidation of section E content. We will refine later.

## E1. Repo & Env
- Monorepo `trading-suite/` with Poetry or uv; Python 3.11.
- Dockerfile; devcontainer for VSCode.

Tech & Integration (first pass)
- How we use it: Reproducible env and tooling across the team.

## E2. Config & Orchestration
- Hydra/OmegaConf config trees — https://hydra.cc
- Prefect for scheduled jobs (daily runs) — https://www.prefect.io

Tech & Integration (first pass)
- How we use it: Profiles for backtest/paper; scheduled daily flow with retries.

## E3. CI
- GitHub Actions: unit tests, ruff, mypy.

Deliverables (from doc)
- `pyproject.toml`, `docker/Dockerfile`, `.github/workflows/ci.yml`
- `conf/` with separate profiles for backtest, paper. 