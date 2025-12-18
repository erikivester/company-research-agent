# Company Research Agent - Pricing Guide

## Executive Summary

**Why is Gemini so expensive?** Your tool makes **6 Gemini API calls per company** (5 detailed briefings + 1 executive summary), and the actual token usage is **extremely high** - the dashboard shows peak usage of 783K tokens per minute for gemini-2.5-flash. This massive token consumption explains why Gemini costs $0.386 per company (~91% of your variable costs).

**Root Cause**: You're sending very large documents (possibly full web pages) to Gemini with minimal truncation, and generating lengthy briefings with up to 8,192 tokens each.

**Current Costs**:
- **Fixed**: ~$38/month (Tavily subscription + Google Cloud hosting)
- **Variable**: ~$0.425 per company (mostly Gemini: $0.386)
- **At 200 companies/month**: $115.19/month total ($0.58 per company)

**Major Optimization Opportunities**:
1. **Implement aggressive input truncation** - Could reduce costs by 50-70%
2. **Switch to `gemini-2.5-flash-lite`** - Additional 70% cost reduction
3. **Reduce max output tokens** from 8,192 to 4,096 - 50% output cost reduction
4. **Combined savings**: Could potentially reduce Gemini costs from $0.386 to ~$0.05-0.08 per company

---

## Cost Summary

### Fixed Monthly Costs
| Service | Cost/Month | Notes |
|---------|-----------|-------|
| **Tavily API** | $30 | Includes 4,000 credits/month (Project plan) |
| **Google Cloud Run** | ~$6.71 | Based on recent billing (Nov 10-19) |
| **Google Secret Manager** | ~$0.89 | Based on recent billing |
| **Google Artifact Registry** | ~$0.19 | Based on recent billing |
| **Google Cloud Storage** | ~$0.02 | Based on recent billing |
| **Total Fixed Costs** | **~$37.81/month** | |

### Variable Costs Per Company Research

| Service | Usage | Cost per Company |
|---------|-------|-----------------|
| **Tavily API** | ~4.8 credits avg | $0.038* |
| **OpenAI API** | 3 calls (gpt-4o-mini) | ~$0.0009 |
| **Google Gemini API** | 1 call (executive summary) | ~$0.386** |
| **Total Variable Cost** | | **~$0.425 per company** |

\* At $0.008/credit (PAYG rate after 4,000 monthly credits used)
\** Based on recent billing: $38.62 for ~100 companies = ~$0.386/company

---

## Detailed Cost Breakdown

### 1. Tavily API (Web Search)

**Monthly Subscription**: $30/month for 4,000 credits

**Usage per company**:
- 20 search queries (4 queries × 5 research categories)
- Mix of basic (1 credit) and advanced (2 credits) searches:
  - Advanced searches: ~55 (News, FLW categories) = 110 credits
  - Basic searches: ~72 (Company, Contacts, Engagement) = 72 credits
  - **Average per company: ~4.8 credits**

**Actual usage** (based on billing data):
- Period: Nov 10-19, 2025
- Total searches: 127 searches
- Total credits used: 237 credits
- Companies researched: ~49
- Average: **4.84 credits per company**

**Cost structure**:
- First 4,000 credits/month: Included in $30 subscription
- Additional credits: $0.008 per credit (PAYG)
- **Within subscription**: $0 per company (up to ~830 companies/month)
- **Over subscription**: $0.038 per company ($0.008 × 4.8 credits)

---

### 2. OpenAI API

**Usage per company**: 3 API calls using `gpt-4o-mini`

1. **Query Generation** (1 call):
   - Input: ~500 tokens
   - Output: ~300 tokens

2. **Company Homepage Selection** (1 call):
   - Input: ~400 tokens
   - Output: ~50 tokens

3. **Tagging/Classification** (1 call):
   - Input: ~3,000 tokens
   - Output: ~200 tokens

**Total per company**:
- Input: ~3,900 tokens
- Output: ~550 tokens

**Current pricing** (Jan 2025):
- `gpt-4o-mini`: $0.15/1M input tokens, $0.60/1M output tokens

**Cost calculation**:
```
(3,900 × $0.15 + 550 × $0.60) / 1,000,000 = $0.000915 per company
```

**Approximate cost**: **$0.0009 per company**

---

### 3. Google Gemini API

**⚠️ This is your primary cost driver!**

**Usage per company**: **6 Gemini API calls total**

#### Briefing Generation (5 calls using `gemini-2.5-flash`)
- **5 separate briefings** per company:
  1. Company Brief
  2. News & Signals
  3. FLW & Sustainability
  4. Contacts
  5. Engagement & Affiliations
- Each briefing call:
  - Input: ~8,000-20,000 tokens (curated research documents)
  - Output: ~1,000-8,192 tokens (structured briefing)
  - Max output tokens: 8,192 per briefing

#### Executive Summary (1 call using `gemini-2.0-flash-exp`)
- Input: ~6,000-10,000 tokens (all briefings + context)
- Output: ~2,000-3,000 tokens (executive summary)
- Max output tokens: 4,096

**Pricing**:
- `gemini-2.5-flash`: $0.30/1M input tokens, $2.50/1M output tokens
- `gemini-2.0-flash-exp`: $0.10/1M input tokens, $0.40/1M output tokens

**Estimated cost per company**:
```
Briefings (5 calls): 5 × (12,000 × $0.30 + 3,000 × $2.50) / 1,000,000 = $0.0555
Executive Summary: (8,000 × $0.10 + 2,500 × $0.40) / 1,000,000 = $0.0018
Total estimated: ~$0.057 per company
```

**Actual cost** (based on recent billing):
- Period: Nov 10-19, 2025
- Total Gemini charges: $38.62
- Companies researched: ~100 (estimated)
- **Average: ~$0.386 per company**

**⚠️ ACTUAL TOKEN USAGE** (from Google AI Studio dashboard):

Based on your billing screenshots, over 28 days:
- **gemini-2.5-flash**: 783,070 tokens per minute (peak usage)
- **gemini-2.5-pro**: 2.37M tokens per minute (extremely high!)
- **gemini-2.0-flash-exp**: 3,970 tokens per minute

**This explains the high costs!** Your actual token usage is **much higher** than estimated, likely because:
1. **Very large input documents** being sent to Gemini (possibly full web pages)
2. **Generating lengthy briefings** with high output token counts
3. **Possible duplicate or retry calls** during rate limiting
4. **You may be using gemini-2.5-pro somewhere** (most expensive model)

**Critical Recommendations**:
1. **Investigate gemini-2.5-pro usage** - This model costs 5x more than Flash. Check if this is intentional.
2. **Implement aggressive input truncation** - You're sending massive amounts of text to Gemini
3. **Switch to `gemini-2.5-flash-lite`** - 70% cost reduction ($0.10 input / $0.40 output)
4. **Reduce `max_output_tokens`** from 8,192 to 4,096 per briefing
5. **Add token counting/logging** to monitor actual usage per company

---

### 4. Google Cloud Services

**Recent billing** (Nov 10-19, 2025):
- **Cloud Run**: $6.71
- **Secret Manager**: $0.89
- **Artifact Registry**: $0.19
- **Cloud Storage**: $0.02
- **Cloud Build**: ~$0.00

These are fixed/semi-fixed costs that scale gradually with usage. Cloud Run scales to zero when idle.

---

## Monthly Cost Scenarios

### Scenario 1: Low Volume (50 companies/month)
*Staying within Tavily free tier*

| Item | Calculation | Cost |
|------|------------|------|
| Tavily subscription | Fixed | $30.00 |
| Tavily usage (240 credits) | Within subscription | $0.00 |
| OpenAI | 50 × $0.0009 | $0.05 |
| Gemini | 50 × $0.386 | $19.30 |
| Google Cloud | Fixed | $7.81 |
| **TOTAL** | | **$57.16/month** |
| **Cost per company** | | **$1.14** |

---

### Scenario 2: Medium Volume (200 companies/month)
*Staying within Tavily subscription*

| Item | Calculation | Cost |
|------|------------|------|
| Tavily subscription | Fixed | $30.00 |
| Tavily usage (960 credits) | Within subscription | $0.00 |
| OpenAI | 200 × $0.0009 | $0.18 |
| Gemini | 200 × $0.386 | $77.20 |
| Google Cloud | Fixed | $7.81 |
| **TOTAL** | | **$115.19/month** |
| **Cost per company** | | **$0.58** |

---

### Scenario 3: High Volume (500 companies/month)
*Exceeding Tavily subscription, using PAYG*

| Item | Calculation | Cost |
|------|------------|------|
| Tavily subscription | Fixed | $30.00 |
| Tavily PAYG | 496 companies × 4.8 credits × $0.008 | $19.02 |
| OpenAI | 500 × $0.0009 | $0.45 |
| Gemini | 500 × $0.386 | $193.00 |
| Google Cloud | Fixed | $7.81 |
| **TOTAL** | | **$250.28/month** |
| **Cost per company** | | **$0.50** |

---

### Scenario 4: Very High Volume (1,000 companies/month)
*Heavy PAYG usage*

| Item | Calculation | Cost |
|------|------------|------|
| Tavily subscription | Fixed | $30.00 |
| Tavily PAYG | 830 companies × 4.8 credits × $0.008 | $31.87 |
| OpenAI | 1,000 × $0.0009 | $0.90 |
| Gemini | 1,000 × $0.386 | $386.00 |
| Google Cloud | ~Fixed | $10.00 |
| **TOTAL** | | **$458.77/month** |
| **Cost per company** | | **$0.46** |

---

## Email Generation (Optional Add-on)

If using the email generation feature:

**Cost per email**: ~$0.011 (using `gpt-4o`)

**Usage**:
- Model: `gpt-4o` (default, configurable to `gpt-4o-mini` for lower cost)
- Input: ~2,500 tokens (template + research + Airtable context)
- Output: ~500 tokens (personalized email + subject)

**Pricing**:
- `gpt-4o`: $2.50/1M input tokens, $10.00/1M output tokens
- Cost: `(2,500 × $2.50 + 500 × $10.00) / 1,000,000 = $0.011`

**Example**: If generating 100 emails/month:
- Additional cost: 100 × $0.011 = **$1.10/month**

---

## Key Insights

### Why Is Gemini So Expensive?

**The Issue**: Gemini costs $0.386 per company, which is **~91% of your total variable costs** and much higher than expected.

**Root Causes**:
1. **6 Gemini API calls per company** (5 briefings + 1 executive summary)
2. **Using expensive `gemini-2.5-flash` model** for briefings:
   - Costs $2.50 per million OUTPUT tokens
   - Each briefing can output up to 8,192 tokens
   - If you're hitting high token counts: 5 briefings × 8,000 tokens × $2.50/1M = $0.10 just for output
3. **Large input contexts**: Sending up to 80,000 characters of research per briefing
4. **Possibly using reasoning/thinking mode** which costs even more

**Potential Savings**:
- Switch to `gemini-2.5-flash-lite`: **70% cost reduction** ($0.10/$0.40 vs $0.30/$2.50)
- Reduce briefing output tokens from 8,192 to 4,096: **50% output cost reduction**
- More aggressive input truncation: Could save 20-30% on input costs
- **Combined potential savings**: Could reduce Gemini costs from $0.386 to ~$0.08 per company

### Cost Drivers
1. **Gemini API** is the largest variable cost (~91% of per-company variable costs)
2. **Tavily API** becomes significant only after exceeding 4,000 credits/month (~830 companies)
3. **OpenAI API** is negligible (~0.2% of per-company costs)
4. **Fixed costs** are predictable at ~$38/month

### Economies of Scale
- **Under 830 companies/month**: Cost per company decreases as you research more (fixed costs amortize)
- **Over 830 companies/month**: Cost per company stays relatively flat (~$0.46-0.50)

### Cost Optimization
- You're already using cost-efficient models (`gpt-4o-mini` for most tasks)
- Consider monitoring Gemini pricing as `gemini-2.0-flash-exp` moves out of experimental phase
- Tavily subscription provides excellent value up to ~830 companies/month

---

## Billing Data Reference

Based on actual usage (Nov 10-19, 2025):
- **Companies researched**: ~49
- **Tavily searches**: 127 queries
- **Tavily credits used**: 237 credits (~4.84 per company)
- **Google Cloud total**: $46.43
  - Gemini: $38.62 (83%)
  - Cloud Run: $6.71 (14%)
  - Other: $1.10 (3%)

---

## Where to Monitor Costs

1. **Tavily**: Dashboard at https://tavily.com/dashboard
2. **OpenAI**: Usage dashboard at https://platform.openai.com/usage
3. **Google Cloud**: Billing at https://console.cloud.google.com/billing
4. **Gemini**: Included in Google Cloud billing under "Gemini API"
