# Module: Decision and Reasoning (Aggregator + RL)

First-pass consolidation of section C content. We will refine later.

## C1. Aggregator V0 (Rules)
- Combine sentiment z‑score and momentum (e.g., 20D return) into `alpha_score`.
- Rules: `alpha>t_buy → buy`, `alpha<t_sell → sell`, else hold.

Tech & Integration (first pass)
- What it is: Rule-based aggregator producing `alpha` and rationale.
- How we use it: Generates `{date, symbol, alpha, rationale}` for execution.

## C2. HRM Placeholder
- Reasoning stub that logs factors used and a one‑paragraph justification.

## C3. RL Track (parallel spike)
- Portfolio PPO (daily rebalancing) using Stable-Baselines3.
- State: {top‑k features incl. sentiment, momentum, risk metrics}.
- Action: target weights.
- Reward: daily portfolio return with risk penalties.

Tech & Integration (first pass)
- SB3 PPO: Stable, on‑policy; quick to experiment.
- How we use it: Synthetic env first; hooks ready for features.

## C4. Uncertainty (True Confidence)
- Meta‑labeling classifier `P(success|context)` using last N trades & market context; threshold drives position scaling.

Deliverables (from doc)
- `decision/aggregator_v0.py` + config thresholds.
- `rl/ppo_portfolio_spike.py` (synthetic env; hooks ready). 