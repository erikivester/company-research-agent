import asyncio
import uuid
import logging
import sys
from pathlib import Path
import os

# Ensure repo root is on PYTHONPATH so 'backend' imports resolve when running tests directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Provide dummy API keys so node constructors that require keys don't raise during this smoke test.
os.environ.setdefault("TAVILY_API_KEY", "DUMMY")
os.environ.setdefault("OPENAI_API_KEY", "DUMMY")
os.environ.setdefault("GEMINI_API_KEY", "DUMMY")

from backend.graph import Graph
from langchain_core.messages import SystemMessage

logging.basicConfig(level=logging.INFO)

async def main():
    job_id = str(uuid.uuid4())
    # Instantiate Graph with the company provided so the InputState has the
    # correct initial company value (this mirrors how application.py constructs
    # the Graph in real runs).
    g = Graph(company="AstreamCo", websocket_manager=None, job_id=job_id)
    compiled = g.workflow.compile()

    # initial input mirrors what Graph.run() uses
    initial_input = {
        "messages": [SystemMessage(content="Astream smoke start")],
        "websocket_manager": None
    }

    # Provide top-level config keys (this is what application.py should pass)
    # We'll still pass a config dict, but company is already present on the
    # Graph's InputState via the constructor above.
    thread = {
        "company": "AstreamCo",
        "company_url": None,
        "industry": "Testing",
        "hq_location": "Nowhere",
        "job_id": job_id,
        # Include optional fields to test propagation
        "airtable_record_id": None,
        "google_drive_folder_url": None
    }

    print("Starting compiled.astream() smoke run — streaming state updates:\n")

    # Use Graph.run(...) which sets up initial_input and merges the provided
    # thread config into it. This mirrors the real application flow.
    async for state_update in g.run(thread=thread):
        print("--- STATE UPDATE ---")
        # state_update may map node name -> state dict, but some nodes may
        # yield non-dict payloads. Handle both safely.
        for node_name, state in state_update.items():
            if isinstance(state, dict):
                company_val = state.get('company')
                keys = list(state.keys())
            else:
                company_val = None
                keys = [repr(state)]
            print(f"Node: {node_name}, company={company_val!r}, keys={keys}")
        print()

    print("astream completed")

if __name__ == '__main__':
    asyncio.run(main())
