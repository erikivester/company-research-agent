# Company Research Agent - Pricing Overview

## Fixed Monthly Costs

| Service | Monthly Cost | Purpose |
|---------|--------------|---------|
| **Tavily API Subscription** | $30.00 | Web search API (includes 4,000 credits) |
| **Google Cloud Run** | $6.71 | Application hosting & compute |
| **Google Secret Manager** | $0.89 | Secure API key storage |
| **Google Artifact Registry** | $0.19 | Docker container storage |
| **Google Cloud Storage** | $0.02 | Logs & temporary files |
| **Total Fixed Costs** | **$37.81/month** | |

---

## Variable Costs Per Company Research

| Service | Cost per Company | What It Does |
|---------|------------------|--------------|
| **Google Gemini API** | $0.05 - $0.12* | AI processing (5 briefings + 1 executive summary) |
| **Tavily API (over quota)** | $0.038** | Web searches (4.8 credits @ $0.008/credit) |
| **OpenAI API** | $0.001 | Query generation, tagging, homepage selection |
| **Total Variable Cost** | **$0.09 - $0.16/company** | |

\* **OPTIMIZED**: Now using `gemini-2.5-flash-lite` (70% cost reduction). PDF generation disabled. Range reflects production vs. testing overhead.
\** Only applies after using 4,000 monthly credits (~830 companies). Within quota: $0/company.

---

## Cost Breakdown by Service

### 1. Tavily (Web Search) - 9% of variable costs
- **What**: Performs web searches to gather company information
- **Usage**: ~4.8 search credits per company (20 queries)
- **Cost model**: $30/month subscription covers first 4,000 credits, then $0.008/credit

### 2. Google Gemini (AI Processing) - 40-70% of variable costs ✅ OPTIMIZED
- **What**: Generates 5 detailed briefings + 1 executive summary per company
- **Models used** (after optimization):
  - `gemini-2.5-flash-lite` for briefings ($0.10 input / $0.40 output per 1M tokens) ← **70% cheaper!**
  - `gemini-2.0-flash-exp` for executive summary ($0.10 input / $0.40 output per 1M tokens)
  - PDF generation: **Disabled** (was using extra processing)
- **Cost reduction**: Switched from `gemini-2.5-flash` ($0.30/$2.50) to Flash Lite - saves ~$0.25/company
- **Token usage**: Still high due to large contexts, but now much cheaper per token

### 3. OpenAI (Lightweight Tasks) - <1% of variable costs
- **What**: Query generation, homepage selection, company tagging
- **Usage**: 3 calls per company using `gpt-4o-mini`
- **Cost**: Negligible (~$0.001 per company)

### 4. Google Cloud (Infrastructure) - Fixed monthly cost
- **What**: Hosting, storage, secrets management
- **Cost model**: Relatively stable ~$8/month regardless of usage volume

---

## Monthly Cost Examples

**Optimized Estimate** (with gemini-2.5-flash-lite + no PDF, ~$0.12/company):

| Companies/Month | Fixed Costs | Variable Costs | Total | Cost/Company |
|----------------|-------------|----------------|-------|--------------|
| 50 | $37.81 | $6.00 | $43.81 | $0.88 |
| 100 | $37.81 | $12.00 | $49.81 | $0.50 |
| 200 | $37.81 | $24.00 | $61.81 | $0.31 |
| 500 | $37.81 | $60.00 | $97.81 | $0.20 |
| 1,000 | $37.81 | $120.00 | $157.81 | $0.16 |

**Before Optimization** (old costs with gemini-2.5-flash + PDF):

| Companies/Month | Fixed Costs | Variable Costs | Total | Cost/Company |
|----------------|-------------|----------------|-------|--------------|
| 200 | $37.81 | $50.00 | $87.81 | $0.44 |
| 500 | $37.81 | $125.00 | $162.81 | $0.33 |

**Savings**: ~70% reduction in variable costs ($0.25 → $0.12 per company) through model switch and disabling PDF generation.

---

## Cost Optimization Opportunities

### Implemented Optimizations ✅

1. **✅ DONE: Switched to `gemini-2.5-flash-lite`** instead of `gemini-2.5-flash`
   - Savings: ~70% of Gemini costs ($0.30/$2.50 → $0.10/$0.40 per 1M tokens)
   - Estimated reduction: $0.386 → $0.12 per company

2. **✅ DONE: Disabled PDF generation**
   - Removes extra processing overhead
   - Estimated savings: ~$0.01-0.02 per company

**Total achieved**: Reduced variable cost from **~$0.43 to ~$0.12 per company** (72% reduction)

### Additional Optimization Opportunities

3. **Reduce input document size**: Truncate from 80,000 to 20,000 characters
   - Potential savings: Additional 50-60% reduction in input token costs

4. **Reduce output token limits**: Lower from 8,192 to 4,096 tokens per briefing
   - Potential savings: 50% reduction in output token costs

**Further potential**: Could reduce to **~$0.05-0.08 per company** with additional optimizations

---

## Summary

- **Fixed overhead**: ~$38/month to keep the application running
- **Variable cost (OPTIMIZED)**: ~$0.09-0.16 per company (production estimate: ~$0.12)
- **Optimization achieved**: 72% cost reduction through:
  - ✅ Switched to `gemini-2.5-flash-lite` (70% cheaper)
  - ✅ Disabled PDF generation
- **Primary cost driver**: Still Gemini API, but now only 40-70% of variable costs (down from 90%)
- **Recent billing context**: Old data ($0.386/company) included testing overhead + expensive gemini-2.5-flash model
- **Break-even scale**: Most cost-efficient at 200+ companies/month (~$0.20-0.31 per company)
- **Expected monthly costs at 200 companies**: ~$62 (down from ~$88 before optimization)
