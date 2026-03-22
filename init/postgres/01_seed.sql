CREATE SCHEMA IF NOT EXISTS banking;

DROP TABLE IF EXISTS banking.transactions_raw;

CREATE TABLE banking.transactions_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    txn_id TEXT,
    customer_id TEXT,
    account_no TEXT,
    txn_ts_raw TEXT,
    amount_raw TEXT,
    currency_raw TEXT,
    txn_type_raw TEXT,
    channel_raw TEXT,
    merchant_raw TEXT,
    city_raw TEXT,
    country_raw TEXT,
    status_raw TEXT,
    reference_raw TEXT,
    ingestion_note TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO banking.transactions_raw (
    txn_id, customer_id, account_no, txn_ts_raw, amount_raw, currency_raw,
    txn_type_raw, channel_raw, merchant_raw, city_raw, country_raw,
    status_raw, reference_raw, ingestion_note
) VALUES
('TXN-20260321-0001', 'C001', '001-234-567-89', '2026-03-20 08:15:10', '1500000.50', 'VND', 'TRANSFER_OUT', 'MOBILE_APP', 'N/A', 'Ho Chi Minh', 'VN', 'SUCCESS', 'pay rent', 'standard row'),
('TXN-20260321-0002', 'C001', '001-234-567-89', '20/03/2026 09:00', '1,200,000', 'VND', 'transfer_out', 'mobile-app', 'N/A', 'HCM', 'VN', 'SUCCESS', 'utility bill', 'amount has comma separators'),
('TXN-20260321-0003', 'C002', '002-111-222-33', '2026-03-20T10:22:00+07:00', '-250000', 'VND', 'FEE', 'CORE', 'BANK_FEE', 'Ha Noi', 'VN', 'SUCCESS', 'monthly fee', 'negative amount'),
('TXN-20260321-0004', 'C003', '003-333-444-55', '2026/03/20 11:45:09', '89.99', 'USD', 'CARD_PAYMENT', 'POS', 'Circle K', 'Da Nang', 'VN', 'SUCCESS', '', 'empty reference'),
('TXN-20260321-0005', 'C004', '004-777-888-99', '2026-03-20 25:61:00', '450000', 'VND', 'CASH_WITHDRAWAL', 'ATM', 'ATM-001', 'Hue', 'VN', 'FAILED', 'timeout', 'invalid timestamp'),
('TXN-20260321-0006', 'C005', '005-555-123-00', NULL, '700000', 'VND', 'TRANSFER_IN', 'BRANCH', 'N/A', 'Can Tho', 'VN', 'SUCCESS', 'salary', 'missing timestamp'),
('TXN-20260321-0007', 'C006', '006-777-000-11', '2026-03-20 13:01:01', NULL, 'VND', 'TRANSFER_IN', 'MOBILE_APP', 'N/A', 'Hai Phong', 'VN', 'SUCCESS', 'cashback', 'missing amount'),
('TXN-20260321-0008', 'C006', '006-777-000-11', '2026-03-20 13:01:01', '0', 'VND', 'TRANSFER_IN', 'MOBILE_APP', 'N/A', 'Hai Phong', 'VN', 'SUCCESS', 'cashback', 'potential duplicate pair'),
('TXN-20260321-0008', 'C006', '006-777-000-11', '2026-03-20 13:01:02', '0', 'VND', 'TRANSFER_IN', 'MOBILE_APP', 'N/A', 'Hai Phong', 'VN', 'SUCCESS', 'cashback', 'duplicate txn_id'),
('TXN-20260321-0009', 'C007', '007-888-111-22', '2026-03-20 14:15:00', 'abc', 'VND', 'CARD_PAYMENT', 'POS', 'Highlands', 'Ha Noi', 'VN', 'SUCCESS', 'coffee', 'non-numeric amount'),
('TXN-20260321-0010', 'C008', '008-000-999-10', '2026-03-20 15:20:00', '999999999999', 'VND', 'TRANSFER_OUT', 'MOBILE_APP', 'N/A', 'HCM', 'VN', 'SUCCESS', 'house purchase', 'very large outlier amount'),
('TXN-20260321-0011', 'C009', '009-112-334-55', '2026-03-20 16:30:45', '35.5', NULL, 'CARD_PAYMENT', 'ECOM', 'Shopee', 'HCM', 'VN', 'SUCCESS', 'order #A12', 'missing currency'),
('TXN-20260321-0012', 'C010', '010-998-776-54', '03-20-2026 17:00:00', '150', 'usd', 'CARD_PAYMENT', 'ECOM', 'Amazon', 'N/A', 'US', 'success', 'intl order', 'lowercase currency and status'),
('TXN-20260321-0013', 'C011', '011-111-111-11', '2026-03-20 18:00:00', '   470000   ', 'VND', 'TRANSFER_IN', 'MOBILE_APP', 'N/A', 'Bien Hoa', 'VN', 'SUCCESS', 'bonus', 'amount with spaces'),
('TXN-20260321-0014', NULL, '012-555-444-33', '2026-03-20 18:20:20', '210000', 'VND', 'TRANSFER_OUT', 'MOBILE_APP', 'N/A', 'Nha Trang', 'VN', 'SUCCESS', 'gift', 'missing customer_id'),
('TXN-20260321-0015', 'C013', '', '2026-03-20 18:45:30', '120000', 'VND', 'TRANSFER_OUT', 'MOBILE_APP', 'N/A', 'Nha Trang', 'VN', 'SUCCESS', 'gift', 'empty account_no'),
('TXN-20260321-0016', 'C014', '014-111-222-33', '2026-03-20 19:12:00', '120000.0000', 'VND', 'transfer_out', 'Mobile App', 'N/A', 'hcm', 'vn', 'SUCCESS', 'same type different case', 'case inconsistency'),
('TXN-20260321-0017', 'C015', '015-321-654-98', '2026-03-20 20:00:00', '-999999999', 'VND', 'REVERSAL', 'CORE', 'N/A', 'Ha Noi', 'VN', 'SUCCESS', 'chargeback reversal', 'extreme negative outlier'),
(' TXN-20260321-0018 ', 'C016', '016-741-852-96', '2026-03-20 20:30:00', '50000', 'VND', 'CARD_PAYMENT', 'POS', 'Winmart', 'Ha Noi', 'VN', 'SUCCESS', ' groceries ', 'leading/trailing spaces in txn_id/reference'),
('TXN-20260321-0019', 'C017', '017-147-258-36', '2026-03-20 21:00:00', '75.25', 'EUR', 'CARD_PAYMENT', 'POS', 'IKEA', 'Stockholm', 'SE', 'PENDING', 'travel expense', 'foreign transaction'),
('TXN-20260321-0020', 'C018', '018-951-753-84', '2026-03-20 21:15:15', 'NULL', 'VND', 'TRANSFER_IN', 'API', 'Payroll', 'HCM', 'VN', 'SUCCESS', 'salary import', 'string NULL amount');
