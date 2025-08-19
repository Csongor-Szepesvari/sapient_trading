# Module: NLP and LLM Agents

First-pass consolidation of section B content. We will refine structure and details later.

## B1. Sentiment Agent (MVP)
- Implement FinBERT pipeline via `transformers` for financial sentiment.
  - Model card: https://huggingface.co/ProsusAI/finbert
- Input: normalized news headlines per ticker (T‑1, T‑0)
- Output: `sentiment_score ∈ [-1,1]`, `confidence`

Tech & Integration (first pass)
- What it is: Transformers-based text classifier (finance-tuned) for sentiment.
- How we use it: Batch inference on daily normalized headlines; write `news_sent`, `news_conf` to features.
- Use cases: Daily sentiment feature; supports event windows.
- FAQ (integration)
  - Performance: Batch with caching; run offline as part of features build.
  - Alternatives: Other HF models later; open-weights preferred.

## B2. Fundamentals Agent (Phase-in)
- Prompt-template for FinR1-style reasoning or local Llama 7B fine-tuned Q&A (post-training later).
  - Reference (FinR1): https://huggingface.co/collections/FinNLP/fin-r1-65f5f41f01b41e7877f7

Tech & Integration (first pass)
- What it is: LLM reasoning over fundamentals; starts with prompt templates.
- How we use it: Not in MVP loop; plan interface returning `{date, symbol, fund_score}`.
- Use cases: Earnings reasoning, qualitative signals.
- FAQ (integration)
  - Latency/cost: Keep out of MVP daily loop; stage results offline.
  - Alternatives: Finetuned smaller models later.

## B3. Agent Wrapper
- Define `AgentAPI` interface (sync + batch) and package a `sentiment_agent` module returning typed dataframes that conform to the feature contract (Section 2).

Tech & Integration (first pass)
- What it is: Unified callable interface and schema guarantees for agents.
- How we use it: Ensure outputs match `features_daily` contract; support batch execution.
- Use cases: Hot-swappable agents; consistent downstream integration.
- FAQ (integration)
  - Validation: Schema checks before write; log latency/coverage.
  - Extensibility: Same pattern for fundamentals agent when phased in.

## Deliverables (from doc)
- `agents/sentiment/` with callable `run_sentiment(df_news)->df_sentiment`.
- Tests: latency, accuracy sanity (tiny hand-labeled set). 