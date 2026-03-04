def test_request_trace_headers_present(client):
    response = client.get("/internal/health")
    assert response.status_code == 200
    assert response.headers.get("x-request-id")
    assert response.headers.get("x-elapsed-ms") is not None


def test_failure_event_emitted_on_validation_error(client):
    auth = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "obs-auth-0001",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 1000,
            "expires_in_days": 7,
        },
        headers={"x-request-id": "req-obs-123"},
    )
    assert auth.status_code == 200
    auth_id = auth.json()["instruction_id"]

    bad_settlement = client.post(
        "/instructions/settlement",
        json={
            "idempotency_key": "obs-settle-0001",
            "authorization_instruction_id": auth_id,
            "merchant_account_id": "acct_merchant_clearing",
            "amount_cents": 2000,
        },
        headers={"x-request-id": "req-obs-123"},
    )
    assert bad_settlement.status_code == 422

    with client.app.state.services.repository.transaction() as tx:
        events = tx.list_events()
    failed = [event for event in events if event["event_type"] == "instruction.failed"]
    dlq = [event for event in events if event["event_type"] == "instruction.dlq"]
    assert len(failed) >= 1
    assert len(dlq) >= 1
    assert '"request_id": "req-obs-123"' in failed[-1]["payload_json"]


def test_internal_metrics_summarizes_failure_counts(client):
    auth = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "obs-auth-0002",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 1000,
            "expires_in_days": 7,
        },
    )
    assert auth.status_code == 200
    auth_id = auth.json()["instruction_id"]
    bad_settlement = client.post(
        "/instructions/settlement",
        json={
            "idempotency_key": "obs-settle-0002",
            "authorization_instruction_id": auth_id,
            "merchant_account_id": "acct_merchant_clearing",
            "amount_cents": 2000,
        },
    )
    assert bad_settlement.status_code == 422

    metrics = client.get("/internal/metrics")
    assert metrics.status_code == 200
    body = metrics.json()
    assert body["totals"]["instruction_failed"] >= 1
    assert body["totals"]["instruction_dlq"] >= 1
    assert body["event_counts"]["instruction.failed"] >= 1
    assert body["event_counts"]["instruction.dlq"] >= 1
    assert body["instruction_failed_by_error_type"]["ValidationError"] >= 1
