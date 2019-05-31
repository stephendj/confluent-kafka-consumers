CREATE TABLE IF NOT EXISTS "xendit-deposit-service".creditcardsettlements_full
(
    id VARCHAR PRIMARY KEY,
    updated TIMESTAMP WITHOUT TIME ZONE,
    created TIMESTAMP WITHOUT TIME ZONE,
    user_id VARCHAR,
    settlement_date TIMESTAMP WITHOUT TIME ZONE,
    should_settle_directly BOOLEAN,
    status VARCHAR,
    credit_card_payments JSONB,
    total_payments_count BIGINT,
    total_fee_amount BIGINT,
    total_settled_amount BIGINT,
    total_refunded_amount BIGINT,
    total_gross_amount BIGINT,
    redeemed_transfer_transaction_id VARCHAR,
    fee_transaction_id VARCHAR,
    settlement_report_file_id VARCHAR
)