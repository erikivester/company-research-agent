import asyncio
import uuid
import logging
import sys
from pathlib import Path

# Ensure repo root is on PYTHONPATH so 'backend' imports resolve when running tests directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.graph import Graph
from langchain_core.messages import SystemMessage
import os

# Provide dummy API keys so node constructors that require keys don't raise during this smoke test.
# These are not used because we stop after the grounding node.
os.environ.setdefault("TAVILY_API_KEY", "DUMMY")
os.environ.setdefault("OPENAI_API_KEY", "DUMMY")
os.environ.setdefault("GEMINI_API_KEY", "DUMMY")

logging.basicConfig(level=logging.INFO)

async def main():
    job_id = str(uuid.uuid4())
    g = Graph(websocket_manager=None, job_id=job_id)
    # Directly call the grounding node with a prepared state dict to avoid
    # depending on LangGraph's astream merging behavior. This isolates the
    # grounding node and verifies the company value is preserved.
    state = {
        "company": "SmokeCo LLC",
        "company_url": None,
        "industry": "Testing",
        "hq_location": "Nowhere",
        "job_id": job_id,
        "websocket_manager": None,
        "messages": []
    }

    result_state = await g.ground.run(state)
    print("--- GROUNDING RESULT ---")
    print(result_state)

if __name__ == '__main__':
    asyncio.run(main())
