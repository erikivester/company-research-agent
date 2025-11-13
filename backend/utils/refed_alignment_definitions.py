"""
ReFED Alignment Category Definitions
====================================

This module provides comprehensive context for each ReFED alignment category,
including mission alignment, detailed signals, examples, and classification guidance.

Based on ReFED's mission to catalyze the food system toward evidence-based action
to stop wasting food, these categories help identify how organizations align with
ReFED's work across data & insights, capital & innovation, and collaborative action.

ReFED Context:
- Vision: A sustainable, resilient, and inclusive food system that optimizes
  environmental resources, minimizes climate impacts, and makes the best use of
  the food we grow
- Mission: Catalyze the food system toward evidence-based action to stop wasting food
- Core Values: Collaborative Expertise, Diverse Perspectives, Credible Data,
  Practical Ambition, Tangible Outcomes
"""

from typing import Dict, List, TypedDict


class AlignmentCategory(TypedDict):
    """Structure for alignment category definitions."""

    name: str
    description: str
    mission_alignment: str
    signals: List[str]
    examples: List[str]
    keywords: List[str]
    related_programs: List[str]


# ============================================================================
# REFED ALIGNMENT CATEGORY DEFINITIONS
# ============================================================================

ALIGNMENT_DEFINITIONS: Dict[str, AlignmentCategory] = {
    "Insights Engine Engagement": {
        "name": "Insights Engine Engagement",
        "description": "Organizations actively using or citing ReFED's data tools and platforms to inform their food waste strategies and decision-making.",
        "mission_alignment": "Aligns with ReFED's Data & Insights strategy by demonstrating adoption of evidence-based frameworks. These users leverage ReFED's comprehensive food waste data infrastructure to guide their actions.",
        "signals": [
            "References ReFED Insights Engine in reports, presentations, or marketing materials",
            "Cites data from Food Waste Monitor (state/sector-level tracking tool)",
            "Uses Impact Calculator to quantify food waste reduction benefits",
            "References Solutions Database for technology/solution research",
            "Mentions ReFED's cost-benefit analyses or ROI frameworks",
            "Uses ReFED's Roadmap data in strategic planning",
            "Cites ReFED in academic research, whitepapers, or case studies",
            "Downloads or embeds ReFED data visualizations",
            "References ReFED's food waste flow analyses (farm to fork)",
            "Uses ReFED metrics in sustainability reporting or goal-setting",
        ],
        "examples": [
            "Corporate sustainability teams using Insights Engine to benchmark performance",
            "Consultants citing ReFED data in client proposals",
            "Nonprofits using Impact Calculator to demonstrate program value",
            "Researchers referencing ReFED's economic analyses in publications",
            "Policy makers using Food Waste Monitor to track state progress",
            "Solution providers using Solutions Database for market intelligence",
        ],
        "keywords": [
            "insights engine",
            "food waste monitor",
            "impact calculator",
            "solutions database",
            "ReFED data",
            "ReFED roadmap",
            "food waste metrics",
            "benchmarking",
            "cost-benefit analysis",
        ],
        "related_programs": ["Insights Engine", "Food Waste Monitor", "Impact Calculator"],
    },
    "Data Contributor / Partner": {
        "name": "Data Contributor / Partner",
        "description": "Organizations with data assets or infrastructure that could contribute to ReFED's evidence base through partnerships, data sharing, or API integrations.",
        "mission_alignment": "Supports ReFED's Credible Data value by expanding the breadth and depth of food waste intelligence. These partners help ReFED deliver more comprehensive evidence and insights.",
        "signals": [
            "Operates food waste tracking/measurement platforms",
            "Has proprietary ESG or sustainability data systems",
            "Offers APIs or data feeds related to food systems",
            "Publishes food waste reports or datasets",
            "Maintains dashboards tracking food flows or waste",
            "Conducts waste audits or compositional studies",
            "Mentions data partnerships or data-sharing initiatives",
            "References open data policies or data transparency",
            "Has measurement infrastructure (IoT sensors, smart bins, etc.)",
            "Operates supply chain visibility platforms",
            "Runs GHG accounting or carbon footprint tools",
            "Member of data collaboratives (e.g., CDP, SASB, GRI)",
        ],
        "examples": [
            "Waste management companies with tonnage data by food type",
            "ESG data platforms tracking corporate food waste metrics",
            "Supply chain visibility providers with spoilage data",
            "Academic institutions conducting waste characterization studies",
            "Industry associations collecting member waste data",
            "Municipal composting programs with diversion statistics",
            "Food banks tracking rescue volumes and sources",
            "Smart packaging companies with shelf-life data",
        ],
        "keywords": [
            "data partnership",
            "API",
            "data sharing",
            "ESG data",
            "measurement platform",
            "waste tracking",
            "data exchange",
            "dashboard",
            "transparency",
            "open data",
        ],
        "related_programs": ["Insights Engine", "Food Waste Monitor"],
    },
    "Business Services Opportunity": {
        "name": "Business Services Opportunity",
        "description": "Organizations with stated food waste reduction goals but unclear implementation plans, representing opportunities for ReFED's strategic advisory and implementation support services.",
        "mission_alignment": "Advances ReFED's Collaborative Action strategy by helping organizations translate commitments into measurable action. These companies need guidance to move from ambition to implementation.",
        "signals": [
            "Has public food waste reduction goals or commitments",
            "Published sustainability report mentioning food waste without clear action plan",
            "References food waste as ESG priority without specific initiatives",
            "Mentions interest in pilots or proof-of-concepts",
            "Issued RFPs or RFIs related to food waste solutions",
            "Recently set science-based targets that include food waste",
            "Mentions need for strategy development or roadmap",
            "References consultants or seeking advisory services",
            "Has sustainability team but limited operational food waste programs",
            "Mentions measurement challenges or data gaps",
            "Seeking peer learning or best practices",
            "References budget for food waste initiatives",
        ],
        "examples": [
            "Retailers with waste reduction goals but no donation program",
            "Restaurants chains setting waste targets without clear tactics",
            "CPG manufacturers with vague circularity commitments",
            "Hospitality companies exploring waste-to-value opportunities",
            "Foodservice operators seeking solution recommendations",
            "Distributors interested in spoilage reduction pilots",
        ],
        "keywords": [
            "food waste goal",
            "commitment",
            "roadmap needed",
            "pilot",
            "proof of concept",
            "RFP",
            "strategy development",
            "implementation support",
            "advisory services",
            "consulting",
        ],
        "related_programs": ["Business Solutions", "Strategic Advisory Services"],
    },
    "U.S. Food Waste Pact Prospect/Member": {
        "name": "U.S. Food Waste Pact Prospect/Member",
        "description": "Organizations aligned with or suitable for the U.S. Food Waste Pact—a public-private partnership for cross-value-chain collaboration on 50% reduction by 2030, including Target-Measure-Act commitments.",
        "mission_alignment": "Central to ReFED's Collaborative Action strategy, bringing together businesses across the supply chain to drive coordinated, measurable progress toward the national 50x30 goal.",
        "signals": [
            "Signatory or member of U.S. Food Waste Pact",
            "Public commitment to 50% food waste reduction by 2030",
            "References SDG 12.3 or Champions 12.3",
            "Has cross-value-chain collaboration programs (e.g., supplier engagement)",
            "Measures Scope 3 food waste emissions",
            "Reports food waste data using FLW Protocol or standard",
            "Engages suppliers or customers on food waste reduction",
            "Member of industry coalitions (e.g., CGF, FMI initiatives)",
            "Has formal TMA (Target-Measure-Act) framework",
            "Participates in pre-competitive collaboration",
            "References systemic change or collective impact",
            "Coordinates with peer companies on food waste",
        ],
        "examples": [
            "Major retailers with supplier waste reduction programs",
            "CPG brands engaging across their value chain",
            "Foodservice companies coordinating with distributors",
            "Food manufacturers addressing agricultural supply waste",
            "Grocery chains working with donation partners",
            "Current U.S. Food Waste Pact signatories",
        ],
        "keywords": [
            "food waste pact",
            "50x30",
            "champions 12.3",
            "SDG 12.3",
            "cross-value chain",
            "scope 3",
            "supplier engagement",
            "TMA",
            "target measure act",
            "collective action",
        ],
        "related_programs": ["U.S. Food Waste Pact", "Business Solutions"],
    },
    "FWFC: Capital-Seeking": {
        "name": "FWFC: Capital-Seeking",
        "description": "Organizations (for-profit or nonprofit) actively seeking funding to develop, pilot, or scale food waste solutions—ideal candidates for ReFED's Food Waste Finance Collaborative (FWFC).",
        "mission_alignment": "Supports ReFED's Capital & Innovation strategy by identifying high-impact solutions that need financial backing to scale. FWFC connects these organizations to aligned capital providers.",
        "signals": [
            "Actively fundraising (Series A/B/C, growth capital, debt)",
            "Seeking grants or impact investment",
            "Mentions need for pilot funding or proof-of-concept capital",
            "References 'looking for investors' or 'seeking partners'",
            "Recently closed funding round with plans to scale",
            "Has impact metrics but limited operational capital",
            "Nonprofit seeking project funding or capacity building",
            "Mentions technology commercialization needs",
            "Has pilot results and seeks scale-up funding",
            "References blended finance or catalytic capital needs",
            "Startup or scaleup in growth phase",
            "Mentions customer acquisition funding needs",
        ],
        "examples": [
            "AI inventory management startups seeking Series A",
            "Upcycled food brands raising growth capital",
            "Community composting nonprofits seeking expansion grants",
            "Surplus food marketplace platforms fundraising",
            "Anaerobic digestion projects seeking project finance",
            "Food rescue apps seeking pilot funding",
            "Date label tech companies raising seed rounds",
        ],
        "keywords": [
            "fundraising",
            "seeking investment",
            "series A",
            "series B",
            "growth capital",
            "pilot funding",
            "grant seeking",
            "scale-up",
            "impact investment",
            "catalytic capital",
        ],
        "related_programs": ["Food Waste Finance Collaborative (FWFC)", "Catalytic Grant Fund"],
    },
    "FWFC: Capital Provider": {
        "name": "FWFC: Capital Provider",
        "description": "Investors, lenders, foundations, or corporate VCs with capital to deploy in food waste solutions—potential partners for ReFED's Food Waste Finance Collaborative deal flow.",
        "mission_alignment": "Essential to ReFED's Capital & Innovation strategy by providing the financial resources to scale high-impact solutions. These partners help address the funding gap in food waste innovation.",
        "signals": [
            "Venture capital or private equity firm",
            "Impact investor or ESG-focused fund",
            "Corporate venture capital arm",
            "Foundation with food/climate/circular economy grantmaking",
            "Bank or lender with sustainable finance programs",
            "Family office with impact investing mandate",
            "References climate tech, food tech, or ag tech investing",
            "Has portfolio companies in food/sustainability sectors",
            "Mentions SDG-aligned investments",
            "References carbon credit or environmental markets",
            "Project finance for infrastructure (e.g., composting, AD)",
            "Participates in impact investing networks (e.g., GIIN, Toniic)",
        ],
        "examples": [
            "VC firms specializing in climate tech or food tech",
            "Foundations funding food systems transformation",
            "Corporate VCs from CPG companies investing in innovation",
            "Impact funds with circular economy thesis",
            "Development finance institutions with food security focus",
            "Angel investors with sustainability focus",
            "Banks with green bonds or sustainable lending programs",
        ],
        "keywords": [
            "venture capital",
            "impact investor",
            "foundation",
            "corporate VC",
            "climate tech",
            "food tech",
            "sustainable finance",
            "ESG investing",
            "circular economy fund",
            "grantmaking",
        ],
        "related_programs": ["Food Waste Finance Collaborative (FWFC)"],
    },
    "Catalytic Grant Fund Fit": {
        "name": "Catalytic Grant Fund Fit",
        "description": "Nonprofits and community-based initiatives working on prevention, rescue, or recycling projects with measurable impact potential but facing funding gaps—targets for ReFED's Catalytic Grant Fund.",
        "mission_alignment": "Embodies ReFED's commitment to Diverse Perspectives and equitable access to resources. The Catalytic Grant Fund seeds grassroots innovation and community-level solutions often overlooked by traditional funders.",
        "signals": [
            "Nonprofit or community-based organization",
            "Works on food rescue, donation, or redistribution",
            "Operates composting or organics recycling programs",
            "Focuses on source reduction or prevention",
            "Has measurable impact metrics (meals, tons diverted, GHG reduced)",
            "Serves underserved or BIPOC communities",
            "Mentions funding gap or resource constraints",
            "Has pilot results or proof of concept",
            "References scaling challenges or capacity needs",
            "Works at community or regional level",
            "Partners with food banks, pantries, or mutual aid networks",
            "Has clear theory of change and outcomes",
        ],
        "examples": [
            "Community composting programs seeking equipment funding",
            "Food rescue nonprofits expanding to new geographies",
            "Gleaning organizations building processing capacity",
            "Urban farms with food recovery and distribution programs",
            "Food recovery apps serving underserved areas",
            "School food waste education and diversion programs",
            "Mutual aid networks with food redistribution focus",
        ],
        "keywords": [
            "nonprofit",
            "community-based",
            "food rescue",
            "composting",
            "food recovery",
            "donation",
            "redistribution",
            "prevention",
            "measurable impact",
            "funding gap",
        ],
        "related_programs": ["Catalytic Grant Fund", "COVID-19 Food Waste Solutions Fund"],
    },
    "Events & Sponsorship (Summit/FWAN)": {
        "name": "Events & Sponsorship (Summit/FWAN)",
        "description": "Organizations likely to participate in, sponsor, or speak at ReFED's convenings including the annual Food Waste Summit and Food Waste Action Network (FWAN) programming.",
        "mission_alignment": "Advances ReFED's Collaborative Action strategy by building the 'Big Tent' for the food waste sector. Events create spaces for peer learning, networking, and cross-sector collaboration.",
        "signals": [
            "History of conference speaking or sponsorship",
            "Active in FWAN or other ReFED convenings",
            "Member of food waste practitioner networks",
            "Mentions attending sustainability conferences (e.g., Climate Week NYC)",
            "Has speakers bureau or executive thought leadership program",
            "Sponsors industry events or trade shows",
            "Participates in webinars or virtual events",
            "References regional food waste meetups or coalitions",
            "Has corporate social responsibility or partnership team",
            "Mentions interest in peer learning or collaboration",
            "Active in industry associations with event components",
            "Hosts or co-hosts food waste events",
        ],
        "examples": [
            "Corporate sponsors of Food Waste Summit",
            "FWAN members attending regional meetups",
            "Solution providers showcasing at conferences",
            "Nonprofit leaders speaking on panels",
            "Academics presenting research at ReFED events",
            "Foundations networking at convenings",
            "Industry association members attending summits",
        ],
        "keywords": [
            "FWAN",
            "food waste summit",
            "conference",
            "sponsorship",
            "speaking",
            "convening",
            "networking",
            "webinar",
            "event participation",
            "climate week",
        ],
        "related_programs": ["Food Waste Action Network (FWAN)", "Food Waste Summit", "Webinar Series"],
    },
    "Policy & Public Affairs Alignment": {
        "name": "Policy & Public Affairs Alignment",
        "description": "Organizations engaged in policy advocacy, regulatory development, or government affairs related to food waste—potential partners for ReFED's policy initiatives.",
        "mission_alignment": "Supports ReFED's Thought Leadership strategy by shaping the regulatory and policy environment to enable food waste solutions at scale. These partners help advance systemic policy change.",
        "signals": [
            "Has government affairs or public policy team",
            "Issues policy statements or position papers on food waste",
            "Member of policy coalitions or advocacy groups",
            "Engages with EPA, USDA, FDA, or state agencies on food waste",
            "References food donation liability protection (e.g., Emerson Act)",
            "Advocates for organics recycling mandates or bans",
            "Works on date label standardization policy",
            "Engages in state or municipal food waste policy development",
            "References tax incentives for food donation",
            "Works on procurement policy or government contracting",
            "Participates in regulatory comment processes",
            "Has lobbyists or advocacy budget for food/climate policy",
        ],
        "examples": [
            "Industry associations advocating for food donation incentives",
            "Nonprofits working on organics recycling mandates",
            "Companies supporting date label standardization",
            "Trade groups engaging on liability protection",
            "State government agencies developing waste policies",
            "NGOs advocating for food recovery infrastructure funding",
            "Waste companies working on organics regulations",
        ],
        "keywords": [
            "policy",
            "advocacy",
            "government affairs",
            "regulation",
            "legislation",
            "coalition",
            "emerson act",
            "organics ban",
            "date label policy",
            "food donation tax incentive",
        ],
        "related_programs": ["Policy & Advocacy Initiatives", "Date Label Working Group"],
    },
    "Measurement & Disclosure": {
        "name": "Measurement & Disclosure",
        "description": "Organizations actively measuring and publicly disclosing food waste data using standardized protocols—demonstrating leadership in transparency and accountability.",
        "mission_alignment": "Reflects ReFED's Credible Data and Tangible Outcomes values by normalizing measurement and reporting practices. These leaders set the bar for transparency and enable industry-wide progress tracking.",
        "signals": [
            "Reports food waste metrics in sustainability reports",
            "Uses FLW Protocol (Food Loss and Waste Protocol) or FLW Standard",
            "Discloses via CDP (Carbon Disclosure Project)",
            "Reports to WRAP (UK Waste & Resources Action Programme)",
            "Has public food waste reduction goals with baselines",
            "Tracks and reports food waste KPIs (tons, meals, GHG)",
            "Uses GRI, SASB, or TCFD frameworks that include food waste",
            "Mentions third-party verification or assurance of waste data",
            "Reports Scope 3 food waste emissions",
            "Has formal measurement methodology or protocol",
            "Conducts regular waste audits with published results",
            "References measurement as key to strategy",
        ],
        "examples": [
            "Major retailers publishing annual food waste metrics",
            "CPG companies reporting waste via CDP",
            "Foodservice companies using FLW Protocol",
            "Food manufacturers with verified waste reduction claims",
            "Hospitality brands tracking and disclosing waste KPIs",
            "Champions 12.3 members with public reporting",
            "Companies with science-based targets including food waste",
        ],
        "keywords": [
            "FLW protocol",
            "CDP",
            "WRAP",
            "food waste disclosure",
            "sustainability report",
            "ESG reporting",
            "measurement",
            "waste audit",
            "GRI",
            "SASB",
            "verification",
        ],
        "related_programs": ["Insights Engine", "U.S. Food Waste Pact"],
    },
    "Solution Adopter (Corporate)": {
        "name": "Solution Adopter (Corporate)",
        "description": "Companies actively implementing food waste solutions across operations—from source reduction to redistribution to recycling—demonstrating operational commitment beyond goals.",
        "mission_alignment": "Drives ReFED's Tangible Outcomes value by translating strategy into action. These organizations prove out solutions at scale, creating case studies and adoption pathways for others.",
        "signals": [
            "Operates food donation or rescue programs",
            "Uses inventory management or demand forecasting AI",
            "Implements dynamic pricing or markdown optimization",
            "Has byproduct valorization or upcycling programs",
            "Uses shelf-life extension technologies",
            "Operates on-site composting or anaerobic digestion",
            "Implements standardized date labeling (Best if Used By / Use By)",
            "Has food waste tracking software deployed",
            "Uses smart packaging or freshness indicators",
            "Operates surplus food marketplaces internally",
            "Has employee engagement programs on food waste",
            "Implements cold chain optimization",
            "Uses portion control or pre-plating technologies",
        ],
        "examples": [
            "Grocery chains with AI-powered shrink reduction",
            "Restaurants using dynamic pricing apps",
            "Food manufacturers with byproduct upcycling programs",
            "Distributors optimizing cold chain to reduce spoilage",
            "Retailers with standardized date labels across products",
            "Foodservice operators donating surplus to food banks",
            "Hotels composting kitchen and buffet waste",
        ],
        "keywords": [
            "food donation",
            "inventory AI",
            "dynamic pricing",
            "upcycling",
            "byproduct valorization",
            "composting",
            "anaerobic digestion",
            "date labeling",
            "shelf-life extension",
            "waste tracking",
        ],
        "related_programs": ["Solutions Database", "Business Solutions", "U.S. Food Waste Pact"],
    },
    "Solution Provider (Vendor/Innovator)": {
        "name": "Solution Provider (Vendor/Innovator)",
        "description": "B2B technology vendors, service providers, and innovators offering commercial solutions to help food businesses reduce waste—potential partners for solution acceleration and scaling.",
        "mission_alignment": "Central to ReFED's Capital & Innovation strategy by providing the tools and services that make food waste reduction operationally feasible. These innovators drive solution adoption at scale.",
        "signals": [
            "B2B SaaS or hardware for food waste reduction",
            "Serves retail, CPG, foodservice, or hospitality customers",
            "Has published case studies with food waste outcomes",
            "Offers inventory management, forecasting, or supply chain tools",
            "Provides composting, anaerobic digestion, or recycling services",
            "Operates food rescue or surplus redistribution platform",
            "Offers shelf-life extension or packaging innovation",
            "Provides waste tracking or measurement software",
            "Has customer testimonials from food businesses",
            "Offers dynamic pricing or markdown optimization",
            "Provides consulting or advisory on food waste reduction",
            "Listed in ReFED Solutions Database or similar directories",
        ],
        "examples": [
            "Inventory AI platforms for retailers",
            "Food rescue apps connecting donors and recipients",
            "Smart packaging companies with freshness indicators",
            "Composting service providers for commercial kitchens",
            "Upcycled ingredient suppliers to CPG brands",
            "Waste tracking software for foodservice operators",
            "Cold chain monitoring IoT companies",
            "Dynamic pricing apps for restaurants and grocery",
        ],
        "keywords": [
            "B2B solution",
            "technology vendor",
            "food tech",
            "software platform",
            "service provider",
            "case studies",
            "customer success",
            "solution provider",
            "waste reduction technology",
            "innovator",
        ],
        "related_programs": ["Solutions Database", "Food Waste Finance Collaborative"],
    },
    "Communications & Thought Leadership": {
        "name": "Communications & Thought Leadership",
        "description": "Organizations with significant communications platforms, media reach, or executive visibility on sustainability—potential partners for amplifying food waste messaging and driving cultural change.",
        "mission_alignment": "Supports ReFED's Thought Leadership strategy by reaching broader audiences and shifting cultural narratives around food waste. These partners help motivate more actors through compelling storytelling.",
        "signals": [
            "Runs sustainability marketing campaigns",
            "Has executive thought leaders active on social media or conferences",
            "Publishes sustainability content (blogs, podcasts, reports)",
            "Media outlet covering food/sustainability topics",
            "Influencer or creator focused on sustainability",
            "Has large social media following on sustainability topics",
            "Produces educational content on food waste",
            "Partners with NGOs on awareness campaigns",
            "Has won sustainability communications awards",
            "Runs consumer engagement programs on food waste",
            "Hosts podcasts or webinars on sustainability",
            "Has communications or PR team focused on ESG",
        ],
        "examples": [
            "Brands with consumer-facing food waste campaigns",
            "Sustainability media outlets covering food systems",
            "Corporate executives speaking at major conferences",
            "Influencers creating content on zero waste living",
            "NGOs running public awareness campaigns",
            "Media companies producing food waste documentaries",
            "Agencies specializing in sustainability communications",
        ],
        "keywords": [
            "communications",
            "thought leadership",
            "marketing campaign",
            "media",
            "sustainability messaging",
            "executive visibility",
            "content creation",
            "influencer",
            "public awareness",
            "storytelling",
        ],
        "related_programs": ["Communications & PR", "Food Waste Action Network (FWAN)"],
    },
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def get_alignment_category_names() -> List[str]:
    """Return list of all alignment category names in order."""
    return [cat["name"] for cat in ALIGNMENT_DEFINITIONS.values()]


def get_enhanced_prompt_for_category(category_name: str) -> str:
    """
    Generate enhanced prompt text for a specific alignment category.

    Args:
        category_name: Name of the alignment category

    Returns:
        Formatted string with category details for use in classification prompts
    """
    if category_name not in ALIGNMENT_DEFINITIONS:
        return f"- **{category_name}:** (No detailed definition available)"

    cat = ALIGNMENT_DEFINITIONS[category_name]

    # Format signals as bullet points
    signals_text = "\n    ".join([f"• {signal}" for signal in cat["signals"][:8]])  # Limit to 8 for brevity

    prompt_text = f"""- **{cat['name']}:**
  *Mission Alignment:* {cat['mission_alignment']}

  *Key Signals:*
    {signals_text}"""

    return prompt_text


def get_all_enhanced_prompts() -> str:
    """
    Generate complete enhanced prompt text for all alignment categories.

    Returns:
        Formatted string with all category details for classification
    """
    category_prompts = []

    for category_name in get_alignment_category_names():
        category_prompts.append(get_enhanced_prompt_for_category(category_name))

    return "\n\n".join(category_prompts)


def get_keywords_for_search() -> Dict[str, List[str]]:
    """
    Return keyword mapping for each category to assist in preliminary filtering.

    Returns:
        Dictionary mapping category names to their associated keywords
    """
    return {cat["name"]: cat["keywords"] for cat in ALIGNMENT_DEFINITIONS.values()}


def get_category_by_keyword(keyword: str) -> List[str]:
    """
    Find alignment categories associated with a given keyword.

    Args:
        keyword: Search keyword (case-insensitive)

    Returns:
        List of category names matching the keyword
    """
    keyword_lower = keyword.lower()
    matching_categories = []

    for cat in ALIGNMENT_DEFINITIONS.values():
        if any(keyword_lower in kw.lower() for kw in cat["keywords"]):
            matching_categories.append(cat["name"])

    return matching_categories
