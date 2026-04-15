import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app


@pytest.mark.asyncio
async def test_health_returns_200():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_mcp_endpoint_exists():
    """Verify /mcp route is registered. The handler requires the lifespan task
    group, so a raw POST raises RuntimeError — but reaching the handler proves
    the route exists (a missing route would return 404, not raise)."""
    with pytest.raises(RuntimeError, match="Task group is not initialized"):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            await ac.post("/mcp", json={})
