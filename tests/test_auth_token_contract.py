import json
import logging
import logging.config
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from starlette.testclient import TestClient

import api.server
from adapters.tetherpool_client import TetherPoolClient


SERVICE_TOKEN = "zcode-secret"
OTHER_TOKEN = "other-secret"


def _client(monkeypatch, tokens=None):
    monkeypatch.setattr(api.server, "API_TOKEN", "")
    monkeypatch.setattr(
        api.server,
        "SERVICE_TOKENS",
        tokens or {"zcode-hub": SERVICE_TOKEN},
    )
    return TestClient(api.server.app)


def test_target_service_body_scope_cannot_be_overridden_by_query(monkeypatch):
    client = _client(monkeypatch, {
        "zcode-hub": SERVICE_TOKEN,
        "other-svc": OTHER_TOKEN,
    })
    payload = {
        "account_id": "auth-scope-account",
        "event_type": "rate_limited_429",
        "target_service": "zcode-hub",
        "force_rebind": False,
    }

    # Endpoint consumes body.target_service; a sibling token plus an unrelated
    # query service must not authorize the write against zcode-hub.
    response = client.post(
        "/v1/battery/event?service=other-svc",
        json=payload,
        headers={"Authorization": f"Bearer {OTHER_TOKEN}"},
    )
    assert response.status_code == 401

    allowed = client.post(
        "/v1/battery/event",
        json=payload,
        headers={"Authorization": f"Bearer {SERVICE_TOKEN}"},
    )
    assert allowed.status_code == 200


def test_target_service_query_scope_cannot_be_overridden_by_service(monkeypatch):
    client = _client(monkeypatch, {
        "zcode-hub": SERVICE_TOKEN,
        "other-svc": OTHER_TOKEN,
    })
    response = client.post(
        "/v1/release_cooldown?target_service=zcode-hub&service=other-svc&exit_ip=192.0.2.9",
        headers={"Authorization": f"Bearer {OTHER_TOKEN}"},
    )
    assert response.status_code == 401

    allowed = client.post(
        "/v1/release_cooldown?target_service=zcode-hub&exit_ip=192.0.2.9",
        headers={"Authorization": f"Bearer {SERVICE_TOKEN}"},
    )
    assert allowed.status_code == 200


def test_tetherpool_client_selects_bearer_token_for_target_service(monkeypatch):
    captured = []

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen(request, timeout):
        captured.append((request, timeout))
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    client = TetherPoolClient(
        api_base="http://127.0.0.1:9100",
        api_token="",
        service_tokens={"zcode-hub": SERVICE_TOKEN},
    )
    client.report_battery_error("auth-sdk-account", service="zcode-hub")

    request, timeout = captured[0]
    assert request.get_header("Authorization") == f"Bearer {SERVICE_TOKEN}"
    assert timeout == client.timeout


def test_tetherpool_client_sends_global_bearer_token(monkeypatch):
    captured = []

    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"{}"

    def fake_urlopen(request, timeout):
        captured.append(request)
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)
    client = TetherPoolClient(api_token="global-secret", service_tokens={})
    client.get_status()

    assert captured[0].get_header("Authorization") == "Bearer global-secret"


def test_uvicorn_access_log_redacts_query_token_and_launcher_uses_config():
    root = Path(__file__).resolve().parents[1]
    config_path = root / "config" / "uvicorn-logging.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert "redact_query_token" in config["filters"]
    assert "redact_query_token" in config["handlers"]["access"].get("filters", [])
    launcher = (root / "start.sh").read_text(encoding="utf-8")
    assert "--log-config" in launcher and "uvicorn-logging.json" in launcher

    logging.config.dictConfig(config)
    from api.access_log import RedactQueryTokenFilter

    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='%s - "%s %s HTTP/%s" %d',
        args=("127.0.0.1:12345", "GET", "/dashboard?token=log-secret&x=1", "1.1", 302),
        exc_info=None,
    )
    assert RedactQueryTokenFilter().filter(record)
    rendered = record.getMessage()
    assert "log-secret" not in rendered
    assert parse_qs(urlsplit(record.args[2]).query)["token"] == ["[REDACTED]"]
