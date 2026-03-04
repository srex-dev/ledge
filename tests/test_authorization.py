def test_authorization_reduces_checking_balance(client):
    response = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "auth-key-0001",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 5000,
            "expires_in_days": 7,
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["instruction_type"] == "authorization"
    assert body["hold_remaining_cents"] == 5000

    balance = client.get("/accounts/acct_customer_checking/balances")
    assert balance.status_code == 200
    assert balance.json()["ledger_balance_cents"] == 5000
