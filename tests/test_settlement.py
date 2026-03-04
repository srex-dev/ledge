def test_settlement_and_release_flow(client):
    auth = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "auth-key-0002",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 5000,
            "expires_in_days": 7,
        },
    )
    auth_body = auth.json()
    auth_id = auth_body["instruction_id"]

    settlement = client.post(
        "/instructions/settlement",
        json={
            "idempotency_key": "settle-key-0002",
            "authorization_instruction_id": auth_id,
            "merchant_account_id": "acct_merchant_clearing",
            "amount_cents": 4800,
        },
    )
    assert settlement.status_code == 200
    settlement_body = settlement.json()
    assert settlement_body["hold_remaining_cents"] == 200

    release = client.post(
        "/instructions/hold-release",
        json={
            "idempotency_key": "release-key-0002",
            "authorization_instruction_id": auth_id,
        },
    )
    assert release.status_code == 200
    release_body = release.json()
    assert release_body["released_amount_cents"] == 200
    assert release_body["hold_remaining_cents"] == 0

    checking_balance = client.get("/accounts/acct_customer_checking/balances").json()
    merchant_balance = client.get("/accounts/acct_merchant_clearing/balances").json()
    holds_balance = client.get("/accounts/acct_customer_holds/balances").json()
    assert checking_balance["ledger_balance_cents"] == 5200
    assert merchant_balance["ledger_balance_cents"] == 4800
    assert holds_balance["ledger_balance_cents"] == 0


def test_reject_oversettlement(client):
    auth = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "auth-key-0003",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 5000,
            "expires_in_days": 7,
        },
    )
    auth_id = auth.json()["instruction_id"]
    settlement = client.post(
        "/instructions/settlement",
        json={
            "idempotency_key": "settle-key-0003",
            "authorization_instruction_id": auth_id,
            "merchant_account_id": "acct_merchant_clearing",
            "amount_cents": 5500,
        },
    )
    assert settlement.status_code == 422
