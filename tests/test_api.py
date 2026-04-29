"""
Tests for api/main.py — all DB calls are mocked so no real PostgreSQL needed.
"""
import pytest
from unittest.mock import AsyncMock, patch
from httpx import AsyncClient, ASGITransport

from main import app


def _record(**kwargs):
    """Return a plain dict that behaves correctly with dict(r) calls in the API."""
    return kwargs


@pytest.fixture
async def mock_pool():
    pool = AsyncMock()
    pool.close = AsyncMock()
    return pool


@pytest.fixture
async def client(mock_pool):
    with patch("asyncpg.create_pool", return_value=mock_pool):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as ac:
            yield ac, mock_pool


# ── /health ──────────────────────────────────────────────────────────────────

async def test_health_ok(client):
    ac, pool = client
    pool.fetchval.return_value = 1
    resp = await ac.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ── /search ───────────────────────────────────────────────────────────────────

async def test_search_returns_results(client):
    ac, pool = client
    pool.fetch.return_value = [
        _record(
            id=1,
            address="1 Rue de la Paix, Luxembourg",
            longitude=6.13,
            latitude=49.61,
            stop_id="BUS_001",
            stop_name="Place d'Armes",
            distance_m=250.0,
            color_class="green",
            road_connected=True,
        )
    ]
    resp = await ac.get("/search", params={"q": "Rue de la Paix"})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["address"] == "1 Rue de la Paix, Luxembourg"
    assert data[0]["color_class"] == "green"


async def test_search_empty_results(client):
    ac, pool = client
    pool.fetch.return_value = []
    resp = await ac.get("/search", params={"q": "Nonexistent Street"})
    assert resp.status_code == 200
    assert resp.json() == []


async def test_search_requires_min_length(client):
    ac, pool = client
    resp = await ac.get("/search", params={"q": "a"})
    assert resp.status_code == 422


async def test_search_custom_limit(client):
    ac, pool = client
    pool.fetch.return_value = []
    resp = await ac.get("/search", params={"q": "Luxembourg", "limit": 5})
    assert resp.status_code == 200
    _, call_args, _ = pool.fetch.mock_calls[0]
    assert call_args[2] == 5  # second positional param is the limit


async def test_search_limit_out_of_range(client):
    ac, pool = client
    resp = await ac.get("/search", params={"q": "Luxembourg", "limit": 100})
    assert resp.status_code == 422


# ── /address/{id} ─────────────────────────────────────────────────────────────

async def test_get_address_found(client):
    ac, pool = client
    pool.fetchrow.return_value = _record(
        id=42,
        address="5 Avenue de la Liberté, Luxembourg",
        longitude=6.12,
        latitude=49.60,
        stop_id="BUS_042",
        stop_name="Gare Centrale",
        distance_m=800.0,
        color_class="green",
        road_connected=True,
    )
    resp = await ac.get("/address/42")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == 42
    assert data["stop_name"] == "Gare Centrale"
    assert data["distance_m"] == 800.0


async def test_get_address_not_found(client):
    ac, pool = client
    pool.fetchrow.return_value = None
    resp = await ac.get("/address/9999")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Address not found"


# ── /geojson ──────────────────────────────────────────────────────────────────

async def test_geojson_returns_feature_collection(client):
    ac, pool = client
    pool.fetch.return_value = [
        _record(
            id=1,
            address="1 Test Street",
            longitude=6.13,
            latitude=49.61,
            stop_name="Test Stop",
            distance_m=500.0,
            color_class="green",
            road_connected=True,
        )
    ]
    resp = await ac.get(
        "/geojson", params={"west": 6.0, "south": 49.5, "east": 6.3, "north": 49.7}
    )
    assert resp.status_code == 200
    fc = resp.json()
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 1
    feat = fc["features"][0]
    assert feat["type"] == "Feature"
    assert feat["geometry"]["type"] == "Point"
    assert feat["geometry"]["coordinates"] == [6.13, 49.61]
    assert feat["properties"]["color_class"] == "green"
    assert feat["properties"]["distance_m"] == 500


async def test_geojson_rounds_distance(client):
    ac, pool = client
    pool.fetch.return_value = [
        _record(
            id=2,
            address="2 Test Street",
            longitude=6.14,
            latitude=49.62,
            stop_name="Stop B",
            distance_m=1234.7,
            color_class="yellow",
            road_connected=False,
        )
    ]
    resp = await ac.get(
        "/geojson", params={"west": 6.0, "south": 49.5, "east": 6.3, "north": 49.7}
    )
    assert resp.json()["features"][0]["properties"]["distance_m"] == 1235


async def test_geojson_none_distance(client):
    ac, pool = client
    pool.fetch.return_value = [
        _record(
            id=3,
            address="3 Test Street",
            longitude=6.15,
            latitude=49.63,
            stop_name=None,
            distance_m=None,
            color_class="red",
            road_connected=True,
        )
    ]
    resp = await ac.get(
        "/geojson", params={"west": 6.0, "south": 49.5, "east": 6.3, "north": 49.7}
    )
    assert resp.json()["features"][0]["properties"]["distance_m"] is None


@pytest.mark.parametrize(
    "zoom,expected_limit",
    [(10, 500), (12, 3000), (14, 15000), (18, 15000)],
)
async def test_geojson_zoom_limits(client, zoom, expected_limit):
    ac, pool = client
    pool.fetch.return_value = []
    await ac.get(
        "/geojson",
        params={"west": 6.0, "south": 49.5, "east": 6.3, "north": 49.7, "zoom": zoom},
    )
    _, call_args, _ = pool.fetch.mock_calls[0]
    # Fifth positional param is the LIMIT value
    assert call_args[5] == expected_limit


async def test_geojson_missing_bbox_params(client):
    ac, pool = client
    resp = await ac.get("/geojson", params={"west": 6.0})
    assert resp.status_code == 422


# ── /path/{id} ────────────────────────────────────────────────────────────────

async def test_path_not_found(client):
    ac, pool = client
    pool.fetchrow.return_value = None
    resp = await ac.get("/path/999")
    assert resp.status_code == 404
    assert "routing data" in resp.json()["detail"]
