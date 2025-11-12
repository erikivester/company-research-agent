# backend/nodes/executive_summary.py
import io
import logging
import os
from typing import Any, Dict

import google.generativeai as genai

from ..classes import ResearchState
from backend.utils.utils import company_name
from backend.utils.enhanced_pdf import create_executive_summary_pdf

logger = logging.getLogger(__name__)

class ExecutiveSummaryNode:
    async def run(self, state: ResearchState) -> ResearchState:
        """Entry point for workflow: calls generate_executive_summary."""
        return await self.generate_executive_summary(state)
    """
    Generates a dynamic 1-2 page executive summary using AI.
    The structure adapts based on research findings, prioritizing what's most relevant.
    Optimized for ReFED's mission to reduce food waste.
    """

    def __init__(self) -> None:
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set")

        genai.configure(api_key=self.gemini_key)
        self.gemini_model = genai.GenerativeModel('gemini-2.0-flash-exp', generation_config=genai.types.GenerationConfig(
            temperature=0.3,
            max_output_tokens=4096
        ))
        logger.info("Executive Summary node initialized with Gemini 2.0 Flash Exp")

    async def generate_executive_summary(self, state: ResearchState) -> ResearchState:
        """
        Generate a dynamic, narrative-driven executive summary optimized for ReFED's mission.
        """
        company = company_name(state)
        websocket_manager = state.get('websocket_manager')
        job_id = state.get('job_id')

        if websocket_manager and job_id:
            await websocket_manager.send_status_update(
                job_id=job_id,
                status="generating_summary",
                message=f"Generating executive summary for {company}",
                result={"step": "Executive Summary", "company": company}
            )

        # Gather all available briefings
        briefings = {
            'company_brief': state.get('company_brief_briefing', ''),
            'news_signals': state.get('news_signal_briefing', ''),
            'flw_sustainability': state.get('flw_sustainability_briefing', ''),
            'contacts': state.get('contact_briefing', ''),
            'engagement': state.get('engagement_briefing', '')
        }

        # Get company identity and metadata
        company_url = state.get('company_url', '')
        industry = state.get('industry', 'Unknown')
        hq_location = state.get('hq_location', 'Unknown')

        # Count available data points
        data_richness = {
            'has_company_info': bool(briefings['company_brief']),
            'has_news': bool(briefings['news_signals']),
            'has_flw': bool(briefings['flw_sustainability']),
            'has_contacts': bool(briefings['contacts']),
            'has_engagement': bool(briefings['engagement']),
        }

        # Build context-aware prompt
        prompt = self._build_summary_prompt(
            company=company,
            company_url=company_url,
            industry=industry,
            hq_location=hq_location,
            briefings=briefings,
            data_richness=data_richness
        )

        try:
            logger.info(f"Generating executive summary for {company}")
            response = await self.gemini_model.generate_content_async(
                prompt,
                request_options={'timeout': 120}
            )

            summary = ""
            if response and response.parts:
                summary = "".join(part.text for part in response.parts if hasattr(part, 'text')).strip()

            if not summary:
                logger.error("Failed to generate executive summary")
                summary = self._create_fallback_summary(company, briefings)

            logger.info(f"Successfully generated executive summary ({len(summary)} chars)")
            
            # Store in state
            state['executive_summary'] = summary
            
            # Generate PDF from the summary
            pdf_path = None  # Initialize pdf_path to None
            try:
                import tempfile
                from datetime import datetime
                # Save PDF to a temp file in the pdfs directory
                pdfs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../pdfs')
                os.makedirs(pdfs_dir, exist_ok=True)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                pdf_filename = f"executive_summary_{company.replace(' ', '_').lower()}_{timestamp}.pdf"
                pdf_path = os.path.join(pdfs_dir, pdf_filename)
                with open(pdf_path, 'wb') as f:
                    pdf_buffer = io.BytesIO()
                    create_executive_summary_pdf(summary, company, pdf_buffer)
                    pdf_buffer.seek(0)
                    f.write(pdf_buffer.read())
                
                logger.info(f"Successfully generated executive summary PDF at {pdf_path}")
            except Exception as pdf_err:
                logger.error(f"Failed to generate PDF from summary: {pdf_err}", exc_info=True)
                pdf_path = None # Ensure pdf_path is None on failure
            
            state['executive_summary_pdf_file'] = pdf_path
            
            # Also update final_summary for PDF generation
            state['final_summary'] = {'markdown_report': summary}

            if websocket_manager and job_id:
                await websocket_manager.send_status_update(
                    job_id=job_id,
                    status="summary_complete",
                    message=f"Executive summary generated for {company}",
                    result={"step": "Executive Summary", "success": True, "length": len(summary)}
                )

            return state

        except Exception as e:
            logger.error(f"Error generating executive summary: {e}", exc_info=True)
            state['executive_summary'] = self._create_fallback_summary(company, briefings)
            return state

    def _build_summary_prompt(
        self, 
        company: str, 
        company_url: str,
        industry: str,
        hq_location: str,
        briefings: Dict[str, str],
        data_richness: Dict[str, bool]
    ) -> str:
        """Build a context-aware prompt for executive summary generation."""
        
        refed_context = """
**About ReFED:**
ReFED is the leading national nonprofit focused exclusively on ending food waste across the U.S. food system. 
Our mission is to catalyze evidence-based action to stop wasting food—for the climate, environment, people, and economy.

**ReFED's Voice & Style:**
- Narrative-driven, not corporate boilerplate
- Data-backed insights with specific numbers
- Solutions-focused and actionable
- Highlight measurable impact (GHG, costs, donations)
- Connect business interests to social/environmental outcomes
"""

        # Build a rich context summary from the briefings
        context_summary = []
        
        if briefings['company_brief']:
            context_summary.append(f"**Company Profile:** {len(briefings['company_brief'])} chars of company information available")
        if briefings['flw_sustainability']:
            context_summary.append(f"**Sustainability Data:** {len(briefings['flw_sustainability'])} chars including food waste initiatives")
        if briefings['news_signals']:
            context_summary.append(f"**Recent News:** {len(briefings['news_signals'])} chars of market signals and developments")
        if briefings['engagement']:
            context_summary.append(f"**Partnerships:** {len(briefings['engagement'])} chars on collaborations and initiatives")
        if briefings['contacts']:
            context_summary.append(f"**Key Contacts:** {len(briefings['contacts'])} chars of personnel information")

        prompt = f"""{refed_context}

**Your Task:**
Create a highly customized, narrative-driven executive summary for **{company}**.

**Critical Requirements:**
1. **BE COMPANY-SPECIFIC**: Extract unique facts, numbers, quotes, and initiatives from the research. NO GENERIC STATEMENTS.
2. **VARY THE STRUCTURE**: Adapt sections based on what's ACTUALLY in the data. Don't force a template.
3. **LEAD WITH IMPACT**: Start with the most compelling, unique insight about THIS company.
4. **USE SPECIFIC DATA**: Pull out revenue figures, program names, partnership details, sustainability targets, executive quotes.
5. **TELL A STORY**: Make this read like an analyst's memo, not a Wikipedia article.
6. **BE SELECTIVE**: 800-1200 words max. Only include what matters for ReFED's mission.

**Company Context:**
- **Company:** {company}
- **Industry:** {industry}
- **Location:** {hq_location}
- **Website:** {company_url}

**Available Research:**
{chr(10).join(context_summary)}

---

**RESEARCH CONTENT:**

"""

        # Add full briefings with clear delineation
        if briefings['company_brief']:
            prompt += f"\n### COMPANY OVERVIEW & OPERATIONS\n{briefings['company_brief'][:3000]}\n\n"
        
        if briefings['flw_sustainability']:
            prompt += f"\n### FOOD WASTE & SUSTAINABILITY PROGRAMS\n{briefings['flw_sustainability'][:3000]}\n\n"
        
        if briefings['news_signals']:
            prompt += f"\n### RECENT DEVELOPMENTS & MARKET SIGNALS\n{briefings['news_signals'][:2000]}\n\n"
        
        if briefings['engagement']:
            prompt += f"\n### PARTNERSHIPS & COLLABORATIONS\n{briefings['engagement'][:2000]}\n\n"
        
        if briefings['contacts']:
            prompt += f"\n### KEY PERSONNEL\n{briefings['contacts'][:1000]}\n\n"

        prompt += f"""

---

**WRITING INSTRUCTIONS:**

**DO:**
- Extract and cite specific numbers, program names, dates, quotes
- Identify unique aspects of {company}'s approach to sustainability/food waste
- Connect their business model to food waste reduction opportunities
- Note specific partnerships, coalitions, or commitments mentioned
- Highlight any innovation, technology, or measurement approaches
- Reference executive leadership by name if mentioned
- Call out any ESG scores, rankings, or public commitments
- Identify concrete next steps for ReFED engagement

**DON'T:**
- Use generic language that could apply to any company
- Make up facts not in the research
- Force sections that have no real content
- Repeat information without adding insight
- Use corporate jargon or buzzwords without substance
- Include a "Background" or "Overview" section unless it contains unique insights

**STRUCTURE GUIDANCE (ADAPT BASED ON CONTENT):**

If they have strong food waste programs:
→ Lead with "A Food Waste Leader: [Company]'s Commitment in Action"

If they're a potential partner but no programs yet:
→ Lead with "Untapped Potential: Why [Company] Is Positioned to Lead"

If recent news/changes are significant:
→ Lead with "A Company in Transition: Opportunities Amid Change"

**Example Section Headers (choose what fits):**
- "The Numbers Tell the Story: [Company]'s Food Waste Impact"
- "From Commitment to Action: [Specific Program Name]"
- "[Executive Name]'s Vision for Sustainability"
- "Supply Chain Opportunities: Where Food Is Wasted"
- "Innovation in Practice: [Specific Technology/Approach]"
- "Partnership Ecosystem: Current and Potential Allies"
- "The Business Case: ROI on Food Waste Reduction"
- "Next Steps: A Roadmap for Collaboration"

**OUTPUT FORMAT:**
- Start with a compelling, specific title (not just "[Company] Executive Summary")
- Use ## for major sections, ### for subsections
- Include bullet points for lists
- Bold key terms and numbers
- Keep paragraphs short (3-5 sentences)
- End with concrete, actionable next steps

Return ONLY the markdown summary. No preamble, no meta-commentary.
"""

        return prompt

    def _create_fallback_summary(self, company: str, briefings: Dict[str, str]) -> str:
        """Create a basic fallback summary if AI generation fails."""
        sections = []
        
        sections.append(f"# {company} Research Summary\n")
        sections.append(f"*Executive Summary - Generated {os.environ.get('REPORT_DATE', 'Today')}*\n\n")
        
        if briefings['company_brief']:
            sections.append("## Company Overview\n")
            sections.append(briefings['company_brief'][:500] + "...\n\n")
        
        if briefings['flw_sustainability']:
            sections.append("## Sustainability & Food Waste\n")
            sections.append(briefings['flw_sustainability'][:500] + "...\n\n")
        
        if briefings['engagement']:
            sections.append("## Partnership Opportunities\n")
            sections.append(briefings['engagement'][:500] + "...\n\n")
        
        return ''.join(sections)


# Export the node function for the graph
async def generate_executive_summary_node(state: ResearchState) -> ResearchState:
    """Wrapper function for use in LangGraph."""
    node = ExecutiveSummaryNode()
    return await node.generate_executive_summary(state)
