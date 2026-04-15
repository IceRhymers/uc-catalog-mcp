from unittest.mock import MagicMock

import pytest
from httpx import AsyncClient, ASGITransport
from app.db.client import get_db
from app.main import app


def _override_get_db():
    yield MagicMock()


@pytest.mark.asyncio
async def test_health_returns_200():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_mcp_endpoint_exists():
    app.dependency_overrides[get_db] = _override_get_db
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            response = await ac.post("/mcp", json={})
        assert response.status_code != 404
    finally:
        app.dependency_overrides.clear()
