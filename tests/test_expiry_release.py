from datetime import datetime, timedelta, timezone

from app.domain.enums import HoldStatus


def test_expired_hold_is_released(client):
    auth = client.post(
        "/instructions/authorization",
        json={
            "idempotency_key": "auth-key-expire",
            "customer_id": "cust_demo",
            "checking_account_id": "acct_customer_checking",
            "hold_account_id": "acct_customer_holds",
            "amount_cents": 2000,
            "expires_in_days": 7,
        },
    )
    assert auth.status_code == 200
    auth_id = auth.json()["instruction_id"]

    app_services = client.app.state.services
    with app_services.repository.transaction() as tx:
        hold = tx.get_hold(auth_id)
        assert hold is not None
        tx.update_hold(auth_id, hold["remaining_amount_cents"], HoldStatus.PENDING)
        tx.cur.execute(
            "UPDATE holds SET expires_at = ?, updated_at = ? WHERE auth_instruction_id = ?",
            (
                (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
                datetime.now(timezone.utc).isoformat(),
                auth_id,
            ),
        )

    result = client.post("/internal/holds/release-expired")
    assert result.status_code == 200
    assert result.json()["released"] == 1

    holds_balance = client.get("/accounts/acct_customer_holds/balances").json()
    checking_balance = client.get("/accounts/acct_customer_checking/balances").json()
    assert holds_balance["ledger_balance_cents"] == 0
    assert checking_balance["ledger_balance_cents"] == 10000
