def test_replay_verify_and_reconciliation_report(client):
    auth = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "rec-auth-0001",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 2500,
            "expires_in_days": 7,
        },
    )
    assert auth.status_code == 200

    replay = client.get("/internal/replay/verify")
    assert replay.status_code == 200
    replay_body = replay.json()
    assert replay_body["accounts_checked"] >= 1
    assert replay_body["ok"] is True

    report = client.get("/internal/reconciliation/report")
    assert report.status_code == 200
    report_body = report.json()
    assert report_body["hold_ledger_cents"] == report_body["hold_remaining_cents"]
    assert report_body["status"] == "ok"
