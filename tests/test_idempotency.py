def test_same_idempotency_key_returns_cached_result(client):
    payload = {
        "idempotency_key": "idem-key-0001",
        "customer_id": "cust_demo",
        "checking_account_id": "acct_customer_checking",
        "hold_account_id": "acct_customer_holds",
        "amount_cents": 1000,
        "expires_in_days": 7,
    }
    first = client.post("/instructions/authorization", json=payload)
    second = client.post("/instructions/authorization", json=payload)
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()


def test_same_idempotency_key_with_different_payload_conflicts(client):
    first = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "idem-key-0002",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 1000,
            "expires_in_days": 7,
        },
    )
    assert first.status_code == 200

    second = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "idem-key-0002",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 1200,
            "expires_in_days": 7,
        },
    )
    assert second.status_code == 409
