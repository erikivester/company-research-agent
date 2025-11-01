import asyncio
import logging
import sys
import types

# Inject a lightweight stub for 'fastapi' to avoid installing the dependency during smoke tests
fake_fastapi = types.ModuleType('fastapi')
class WebSocket:
    async def send_text(self, msg):
        pass
class WebSocketDisconnect(Exception):
    pass
fake_fastapi.WebSocket = WebSocket
fake_fastapi.WebSocketDisconnect = WebSocketDisconnect
sys.modules['fastapi'] = fake_fastapi

# Now import the module under test
from backend.services.websocket_manager import WebSocketManager

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("smoke_test")

async def run_test():
    manager = WebSocketManager()

    # Case 1: No job_id anywhere
    await manager.safe_send(state=None, job_id=None, status="test", message="no job_id present")
    logger.info("safe_send completed with no job_id (expected: debug message, no exception)")

    # Case 2: job_id present in state
    state = {"company": "TestCo", "job_id": "job-123", "websocket_manager": manager}
    await manager.safe_send(state=state, job_id=None, status="test", message="job_id in state present")
    logger.info("safe_send completed with job_id in state (expected: broadcast attempted, no exception)")

    # Case 3: explicit job_id passed
    await manager.safe_send(state=None, job_id="explicit-job", status="test", message="explicit job_id")
    logger.info("safe_send completed with explicit job_id (expected: broadcast attempted, no exception)")

if __name__ == '__main__':
    asyncio.run(run_test())
    print("SMOKE TEST (stub): completed")
