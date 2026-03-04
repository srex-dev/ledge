from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app


def test_optional_risk_event_emitted_when_enabled(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ENABLE_AI_RISK_CHECK", "1")
    app = create_app(str(tmp_path / "risk.db"))
    client = TestClient(app)

    response = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "auth-risk-0001",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 5000,
            "expires_in_days": 7,
        },
    )
    assert response.status_code == 200

    with app.state.services.repository.transaction() as tx:
        events = tx.list_events()
    risk_events = [event for event in events if event["event_type"] == "fraud.risk_assessed"]
    assert len(risk_events) == 1


def test_ai_mode_falls_back_to_heuristic_when_provider_missing(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("ENABLE_AI_RISK_CHECK", "1")
    monkeypatch.setenv("RISK_SCORER_MODE", "ai")
    monkeypatch.delenv("AI_RISK_PROVIDER", raising=False)
    app = create_app(str(tmp_path / "risk-fallback.db"))
    client = TestClient(app)

    response = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "auth-risk-0002",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 5000,
            "expires_in_days": 7,
        },
    )
    assert response.status_code == 200

    with app.state.services.repository.transaction() as tx:
        events = tx.list_events()
    risk_events = [event for event in events if event["event_type"] == "fraud.risk_assessed"]
    assert len(risk_events) == 1
    assert '"model_type": "heuristic"' in risk_events[0]["payload_json"]
