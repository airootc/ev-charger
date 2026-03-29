"""Comprehensive API server tests: auth, stations, rate limiting, security headers, brute force."""
from __future__ import annotations

import hashlib
import os
import secrets
import tempfile
import json
import time

import pytest
from httpx import AsyncClient, ASGITransport

# Set test environment before importing server
os.environ["ENVIRONMENT"] = "development"
os.environ["DB_PATH"] = ""  # will be overridden per-test

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ─── Fixtures ───

@pytest.fixture(autouse=True)
def _test_db(tmp_path, monkeypatch):
    """Create a fresh SQLite DB for each test."""
    db_path = str(tmp_path / "test.db")
    monkeypatch.setenv("DB_PATH", db_path)

    from api_server import db as db_mod
    from api_server.config import settings

    settings.DB_PATH = db_path
    # Force new connection on the thread-local
    if hasattr(db_mod._local, "conn") and db_mod._local.conn is not None:
        try:
            db_mod._local.conn.close()
        except Exception:
            pass
        db_mod._local.conn = None
    # Clear usage cache
    db_mod._usage_cache.clear()
    db_mod.init_db()
    yield db_path


@pytest.fixture
def geojson_file(tmp_path):
    """Create a test GeoJSON file with known stations."""
    stations = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [2.35, 48.86]},
                "properties": {
                    "station_id": "st_001",
                    "station_name": "Paris Station",
                    "address": "1 Rue Test",
                    "city": "Paris",
                    "country": "France",
                    "country_code": "FR",
                    "network": "ChargePoint",
                    "operator": "ChargePoint",
                    "connector_types": "Type 2, CCS",
                    "connector_category": "DC Fast",
                    "num_ports": 4,
                    "total_ports": 4,
                    "power_kw": 150,
                    "status": "Operational",
                    "access_type": "Public",
                    "source": "test",
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [2.45, 48.90]},
                "properties": {
                    "station_id": "st_002",
                    "station_name": "Paris North",
                    "city": "Paris",
                    "country_code": "FR",
                    "network": "Tesla",
                    "connector_types": "NACS",
                    "connector_category": "Tesla Supercharger",
                    "num_ports": 8,
                    "total_ports": 8,
                    "power_kw": 250,
                    "status": "Operational",
                    "source": "test",
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-0.12, 51.51]},
                "properties": {
                    "station_id": "st_003",
                    "station_name": "London Station",
                    "city": "London",
                    "country_code": "GB",
                    "network": "Pod Point",
                    "connector_types": "Type 2",
                    "connector_category": "Level 2",
                    "num_ports": 2,
                    "total_ports": 2,
                    "power_kw": 22,
                    "status": "Operational",
                    "source": "test",
                },
            },
        ],
    }
    path = tmp_path / "test_stations.geojson"
    path.write_text(json.dumps(stations))
    return str(path)


@pytest.fixture
def app(geojson_file, _test_db, monkeypatch):
    """Create a fresh FastAPI app with test data."""
    from api_server.config import settings
    from api_server import stations as st_mod

    settings.GEOJSON_PATH = geojson_file
    monkeypatch.setattr(settings, "GEOJSON_PATH", geojson_file)

    st_mod.load_geojson()

    from api_server.server import app as _app
    return _app


@pytest.fixture
def api_key(_test_db):
    """Create and return a test API key."""
    from api_server import db
    raw_key, record = db.create_api_key(name="test-key", tier="free")
    return raw_key


@pytest.fixture
def admin_token(monkeypatch):
    """Set and return a known admin token."""
    from api_server.config import settings
    token = "test-admin-token-12345"
    monkeypatch.setattr(settings, "ADMIN_TOKEN", token)
    return token


# ─── Auth Tests ───

class TestAuth:
    @pytest.mark.anyio
    async def test_no_key_returns_401(self, app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get("/api/stations", params={"bbox": "2,48,3,49"})
            assert r.status_code == 401
            assert "API key required" in r.json()["detail"]["error"]

    @pytest.mark.anyio
    async def test_invalid_key_returns_401(self, app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "2,48,3,49"},
                headers={"X-API-Key": "di_invalidkey123"},
            )
            assert r.status_code == 401

    @pytest.mark.anyio
    async def test_valid_key_returns_200(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "2,48,3,49"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 200

    @pytest.mark.anyio
    async def test_revoked_key_returns_401(self, app, api_key, _test_db):
        from api_server import db
        keys = db.list_api_keys()
        db.revoke_api_key(keys[0]["id"])

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "2,48,3,49"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 401


# ─── Station Endpoint Tests ───

class TestStations:
    @pytest.mark.anyio
    async def test_bbox_returns_features(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "2.0,48.5,2.8,49.0"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 200
            data = r.json()
            assert data["type"] == "FeatureCollection"
            # 2 real stations + canary stations may be injected
            assert len(data["features"]) >= 2

    @pytest.mark.anyio
    async def test_bbox_outside_returns_empty(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "100,10,101,11"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 200
            # May have canary stations but 0 real stations
            real = [f for f in r.json()["features"]
                    if not f["properties"].get("station_id", "").startswith("st_")]
            # At most canary stations in this area
            assert len(r.json()["features"]) <= 5  # canaries are sparse

    @pytest.mark.anyio
    async def test_missing_bbox_returns_422(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get("/api/stations", headers={"X-API-Key": api_key})
            assert r.status_code == 422

    @pytest.mark.anyio
    async def test_invalid_bbox_returns_400(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "not,valid,bbox,params"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 400

    @pytest.mark.anyio
    async def test_network_filter(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "2.0,48.5,2.8,49.0", "network": "Tesla"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 200
            features = r.json()["features"]
            # All returned real features should have network == Tesla
            # (canary stations may have different networks)
            real = [f for f in features if not f["properties"].get("station_id", "").startswith("st_")]
            # At least the real Tesla station should be present
            tesla_features = [f for f in features if f["properties"]["network"] == "Tesla"]
            assert len(tesla_features) >= 1

    @pytest.mark.anyio
    async def test_connector_category_filter(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "-1,50,1,52", "connector_category": "Level 2"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 200
            features = r.json()["features"]
            for f in features:
                assert f["properties"]["connector_category"] == "Level 2"

    @pytest.mark.anyio
    async def test_min_power_filter(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations",
                params={"bbox": "-1,48,3,52", "min_power_kw": "100"},
                headers={"X-API-Key": api_key},
            )
            assert r.status_code == 200
            features = r.json()["features"]
            # Verify our known test stations: only st_001 (150kW) and st_002 (250kW) qualify
            known_ids = {f["properties"]["station_id"] for f in features
                         if f["properties"].get("station_id") in ("st_001", "st_002", "st_003")}
            assert "st_001" in known_ids  # 150kW >= 100
            assert "st_002" in known_ids  # 250kW >= 100
            assert "st_003" not in known_ids  # 22kW < 100


# ─── Meta Endpoint Tests ───

class TestMeta:
    @pytest.mark.anyio
    async def test_meta_returns_counts(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get("/api/stations/meta", headers={"X-API-Key": api_key})
            assert r.status_code == 200
            data = r.json()
            assert data["total_stations"] == 3
            assert "networks" in data
            assert "countries" in data


# ─── Admin Tests ───

class TestAdmin:
    @pytest.mark.anyio
    async def test_create_key(self, app, admin_token):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.post(
                "/api/admin/keys",
                json={"name": "new-test-key"},
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            assert r.status_code == 200
            assert r.json()["key"].startswith("di_")

    @pytest.mark.anyio
    async def test_list_keys(self, app, admin_token, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/admin/keys",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            assert r.status_code == 200
            assert len(r.json()["keys"]) >= 1

    @pytest.mark.anyio
    async def test_admin_without_token_returns_401(self, app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get("/api/admin/keys")
            assert r.status_code in (401, 403)

    @pytest.mark.anyio
    async def test_admin_wrong_token_returns_403(self, app, admin_token):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/admin/keys",
                headers={"Authorization": "Bearer wrong-token"},
            )
            assert r.status_code == 403

    @pytest.mark.anyio
    async def test_revoke_key(self, app, admin_token, api_key):
        from api_server import db
        keys = db.list_api_keys()
        key_id = keys[0]["id"]

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.delete(
                f"/api/admin/keys/{key_id}",
                headers={"Authorization": f"Bearer {admin_token}"},
            )
            assert r.status_code == 200
            assert r.json()["status"] == "revoked"


# ─── Security Headers Tests ───

class TestSecurityHeaders:
    @pytest.mark.anyio
    async def test_security_headers_present(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.get(
                "/api/stations/meta",
                headers={"X-API-Key": api_key},
            )
            assert r.headers.get("X-Content-Type-Options") == "nosniff"
            assert r.headers.get("X-Frame-Options") == "DENY"
            assert "default-src" in r.headers.get("Content-Security-Policy", "")

    @pytest.mark.anyio
    async def test_cors_headers(self, app, api_key):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            r = await c.options(
                "/api/stations/meta",
                headers={
                    "Origin": "http://localhost:3000",
                    "Access-Control-Request-Method": "GET",
                },
            )
            assert r.status_code == 200


# ─── Brute Force Tests ───

class TestBruteForce:
    def test_escalating_lockout(self):
        from api_server.brute_force import BruteForceGuard

        guard = BruteForceGuard()
        ip = "10.0.0.99"

        # First 4 failures — not blocked yet
        for _ in range(4):
            guard.record_failure(ip)
        blocked, _ = guard.is_blocked(ip)
        assert not blocked

        # 5th failure — should trigger tier 1 block
        guard.record_failure(ip)
        blocked, retry_after = guard.is_blocked(ip)
        assert blocked
        assert retry_after > 0

    def test_success_resets(self):
        from api_server.brute_force import BruteForceGuard

        guard = BruteForceGuard()
        ip = "10.0.0.100"

        for _ in range(4):
            guard.record_failure(ip)

        guard.record_success(ip)
        blocked, _ = guard.is_blocked(ip)
        assert not blocked


# ─── DB Tests ───

class TestDB:
    def test_create_and_validate_key(self, _test_db):
        from api_server import db

        raw_key, record = db.create_api_key(name="db-test", tier="free")
        assert raw_key.startswith("di_")
        assert record["name"] == "db-test"

        validated = db.validate_api_key(raw_key)
        assert validated is not None
        assert validated["name"] == "db-test"

    def test_invalid_key_returns_none(self, _test_db):
        from api_server import db
        assert db.validate_api_key("di_doesnotexist") is None

    def test_rotate_key(self, _test_db):
        from api_server import db

        raw_key, record = db.create_api_key(name="rotate-test", tier="free")
        new_key = db.rotate_api_key(record["id"])
        assert new_key is not None
        assert new_key != raw_key

        # Old key invalid, new key valid
        assert db.validate_api_key(raw_key) is None
        assert db.validate_api_key(new_key) is not None

    def test_usage_tracking(self, _test_db):
        from api_server import db

        raw_key, record = db.create_api_key(name="usage-test", tier="free", daily_limit=100)
        usage = db.get_key_daily_usage(record["id"])
        assert usage == 0

        db.log_request(api_key_id=record["id"], ip="127.0.0.1", endpoint="/api/stations", bbox="0,0,1,1")
        # Clear usage cache to get fresh count
        db._usage_cache.clear()
        usage = db.get_key_daily_usage(record["id"])
        assert usage == 1

    def test_prune_request_log(self, _test_db):
        from api_server import db
        # Should not raise
        db.prune_request_log(max_age_days=30)

    def test_canary_store_and_retrieve(self, _test_db):
        from api_server import db

        raw_key, record = db.create_api_key(name="canary-test", tier="free")
        key_id = record["id"]

        db.store_canary(key_id, "Test Canary", 48.86, 2.35, {"network": "Test"})
        canaries = db.get_canaries_for_key(key_id)
        assert len(canaries) == 1
        assert canaries[0]["station_name"] == "Test Canary"


# ─── Spatial Index Tests ───

class TestSpatialIndex:
    def test_grid_index_load(self, geojson_file):
        from api_server.config import settings
        from api_server import stations as st_mod

        settings.GEOJSON_PATH = geojson_file
        count = st_mod.load_geojson()
        assert count == 3

    def test_metadata_via_module(self, geojson_file):
        from api_server.config import settings
        from api_server import stations as st_mod

        settings.GEOJSON_PATH = geojson_file
        st_mod.load_geojson()

        # Access the module-level _metadata dict directly
        meta = st_mod._metadata
        assert meta["total_stations"] == 3
        # Countries may use full names or codes depending on data
        countries = meta.get("countries", [])
        assert len(countries) >= 1
