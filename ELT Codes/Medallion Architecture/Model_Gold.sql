CREATE OR REFRESH MATERIALIZED VIEW dim_date AS
SELECT
    CAST(date_format(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    year(full_date)                                  AS year,
    quarter(full_date)                               AS quarter,
    month(full_date)                                 AS month,
    date_format(full_date, 'MMMM')                   AS month_name,
    date_format(full_date, 'yyyy-MM')                AS year_month,
    weekofyear(full_date)                            AS week_of_year,
    day(full_date)                                   AS day_of_month,
    dayofweek(full_date)                             AS day_of_week,
    date_format(full_date, 'EEEE')                   AS day_name,
    dayofweek(full_date) IN (6, 7)                   AS is_weekend      -- Egypt weekend: Friday + Saturday
FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS full_date);

-- Customer dimension - SCD Type 2 (one row per customer VERSION). Built in Silver (MD5 hash compare).
CREATE OR REFRESH MATERIALIZED VIEW dim_customer AS
SELECT
    customer_sk,                              -- surrogate key of the version = MD5(customer_id || scd_from_date)
    customer_id,                              -- business key
    customer_hash_key,                        -- MD5(customer_id)
    record_hash,                              -- MD5 of the tracked attributes
    customer_type, corporate_account_id, customer_name, customer_email, gender, birth_year, age_group,
    language, kyc_status, msisdn, activation_date,
    segment_id, operator_id, governorate_id, plan_id, contract_type_id, device_id,
    loyalty_tier_id, loyalty_points,
    scd_status, scd_is_current, scd_from_date, scd_last_date
FROM telecomservice.onelakedatabase.silver_dim_customer;

-- Vodafone / Etisalat / Orange / WE (view: pure pass-through of the silver MV, no backing table needed)
CREATE VIEW dim_operator AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_operators;

-- Egypt governorates + region (parent of dim_store and dim_corporate_account -> SNOWFLAKE)
CREATE VIEW dim_governorate AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_governorates;

-- Plans / packages (Flex, Red, Fakka, Hekaya, ...)
CREATE VIEW dim_plan AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_plans;

-- Offers and promotions
CREATE VIEW dim_offer AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_offers;

-- Voice / Data / SMS / Roaming / VAS
CREATE VIEW dim_service_type AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_service_types;

-- Wallets (Vodafone Cash...), cards, Fawry, cash, bank transfer
CREATE VIEW dim_payment_method AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_payment_methods;

-- Success / Failed / Pending
CREATE VIEW dim_transaction_status AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_transaction_statuses;

-- Why a transaction failed
CREATE VIEW dim_failure_reason AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_failure_reasons;

-- Send money, cash in/out, bill payment, QR payment
CREATE VIEW dim_wallet_txn_type AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_wallet_transaction_types;

-- Service channel: app, USSD, store, Fawry kiosk, ...
CREATE VIEW dim_channel AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_channels;

-- Individual / Youth / Corporate / VIP
CREATE VIEW dim_segment AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_customer_segments;

-- Phone brand / type / OS
CREATE VIEW dim_device AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_devices;

-- 3G / 4G / 4.5G / 5G
CREATE VIEW dim_network_type AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_network_types;

-- Bronze / Silver / Gold / Platinum
CREATE VIEW dim_loyalty_tier AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_loyalty_tiers;

-- Prepaid, postpaid, corporate contracts
CREATE VIEW dim_contract_type AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_contract_types;

-- Net 15 / 30 / 60 ... (B2B)
CREATE VIEW dim_payment_terms AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_payment_terms;

-- B2B order lifecycle
CREATE VIEW dim_order_status AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_order_statuses;

-- Own store / dealer / online / telesales / field sales
CREATE VIEW dim_sales_channel AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_sales_channels;

-- New line, MNP, replacement, device, upgrade, VIP number
CREATE VIEW dim_sale_type AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_sale_types;

-- SIMs, devices, accessories, VIP numbers
CREATE VIEW dim_product AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_products;

-- Industry of a corporate account (parent of dim_corporate_account -> SNOWFLAKE)
CREATE VIEW dim_industry AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_industries;

-- Points of sale (FK: governorate, operator, sales channel -> SNOWFLAKE)
CREATE VIEW dim_store AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_stores;

-- Sales reps and B2B account managers (FK: store)
CREATE VIEW dim_sales_rep AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_sales_reps;

-- Companies buying lines (FK: industry, governorate, operator, account manager)
CREATE VIEW dim_corporate_account AS
SELECT * FROM telecomservice.onelakedatabase.silver_map_corporate_accounts;


-- ============================ FACTS (4) ============================

-- 1) Grain: one usage record (call / data session / SMS / roaming / VAS)
CREATE OR REFRESH MATERIALIZED VIEW fact_usage AS
SELECT
    e.event_id,
    e.event_date_key AS date_key,
    e.event_timestamp,
    e.event_hour,
    d.customer_sk,
    e.customer_id,
    e.operator_id,
    e.governorate_id,
    e.plan_id,
    e.device_id,
    e.channel_id,
    e.network_type_id,
    e.service_type_id,
    e.offer_id,
    e.status_id,
    e.failure_reason_id,
    e.is_roaming,
    e.call_duration_sec,
    e.data_mb,
    e.sms_count,
    e.destination_msisdn,
    e.usage_charge_egp,
    e.balance_after_egp,
    e.fraud_score,
    e.is_fraud_suspected,
    e.cell_tower_id,
    e.latitude,
    e.longitude
FROM telecomservice.onelakedatabase.silver_telecom_events e
LEFT JOIN dim_customer d
       ON e.customer_id = d.customer_id
      AND e.event_timestamp >= d.scd_from_date
      AND (d.scd_last_date IS NULL OR e.event_timestamp < d.scd_last_date)
WHERE e.event_type = 'usage';


-- 2) Grain: one customer money transaction = recharge + wallet_transaction + subscription
CREATE OR REFRESH MATERIALIZED VIEW fact_transaction AS
SELECT
    e.event_id,
    e.event_type,                              -- recharge / wallet_transaction / subscription
    e.event_date_key AS date_key,
    e.event_timestamp,
    e.event_hour,
    d.customer_sk,
    e.customer_id,
    -- shared dimension keys
    e.operator_id,
    e.governorate_id,
    e.plan_id,                                 -- plan the customer had (for subscription = the previous plan)
    e.channel_id,
    e.payment_method_id,
    e.offer_id,
    e.status_id,
    e.failure_reason_id,
    -- shared measures
    e.event_amount_egp,
    e.fraud_score,
    e.is_fraud_suspected,
    -- recharge only
    e.recharge_amount_egp,
    e.vat_egp,
    e.bonus_balance_egp,
    e.cashback_egp,
    e.balance_after_egp,
    e.voucher_code,
    -- wallet_transaction only
    e.wallet_txn_type_id,
    e.wallet_amount_egp,
    e.wallet_fee_egp,
    e.wallet_balance_after_egp,
    e.counterparty_msisdn,
    e.merchant_name,
    -- subscription only
    e.subscribed_plan_id,
    e.subscription_action,
    e.subscription_price_egp,
    e.discount_egp,
    e.auto_renew,
    e.subscription_start_date,
    e.subscription_end_date
FROM telecomservice.onelakedatabase.silver_telecom_events e
LEFT JOIN dim_customer d
       ON e.customer_id = d.customer_id
      AND e.event_timestamp >= d.scd_from_date
      AND (d.scd_last_date IS NULL OR e.event_timestamp < d.scd_last_date)
WHERE e.event_type IN ('recharge', 'wallet_transaction', 'subscription');


-- 3) Grain: one sale (one invoice line: SIM / device / number sold in a store, dealer, online or telesales)

CREATE OR REFRESH MATERIALIZED VIEW fact_sales AS
SELECT
    e.event_id,
    e.invoice_number,
    e.event_date_key AS date_key,
    e.event_timestamp,
    e.event_hour,
    d.customer_sk,
    e.customer_id,
    -- dimension keys
    e.operator_id,
    e.governorate_id,
    e.plan_id,
    e.channel_id,
    e.payment_method_id,
    e.offer_id,
    e.status_id,
    e.failure_reason_id,
    e.sale_type_id,
    e.product_id,
    e.store_id,
    e.sales_rep_id,
    e.sales_channel_id,
    e.previous_operator_id,                    -- only for MNP port-in
    -- measures
    e.quantity,
    e.unit_price_egp,
    e.gross_amount_egp,
    e.sale_discount_egp,
    e.net_before_vat_egp,
    e.sale_vat_egp,
    e.total_amount_egp,
    e.cost_egp,
    e.margin_egp,
    e.commission_egp,
    e.installment_months,
    e.down_payment_egp,
    e.installment_monthly_egp,
    e.is_returned,
    e.activation_timestamp,
    e.sold_imei,
    e.sold_iccid,
    e.fraud_score,
    e.is_fraud_suspected
FROM telecomservice.onelakedatabase.silver_telecom_events e
LEFT JOIN dim_customer d
       ON e.customer_id = d.customer_id
      AND e.event_timestamp >= d.scd_from_date
      AND (d.scd_last_date IS NULL OR e.event_timestamp < d.scd_last_date)
WHERE e.event_type = 'sale';


-- 4) Grain: one B2B order (a company buying lines). No customer_sk: the buyer is dim_corporate_account
CREATE OR REFRESH MATERIALIZED VIEW fact_corporate_order AS
SELECT
    e.order_id,
    e.po_number,
    e.event_date_key AS date_key,
    e.event_timestamp,
    e.corporate_account_id,
    e.operator_id,
    e.governorate_id,
    e.plan_id,
    e.contract_type_id,
    e.payment_terms_id,
    e.order_status_id,
    e.account_manager_id,
    e.sales_rep_id,
    e.store_id,
    e.sales_channel_id,
    e.payment_method_id,
    e.status_id,
    e.failure_reason_id,
    e.lines_ordered,
    e.lines_provisioned,
    e.bulk_discount_pct,
    e.monthly_fee_per_line_egp,
    e.monthly_recurring_revenue_egp,
    e.one_time_fees_egp,
    e.contract_months,
    e.includes_devices,
    e.device_count,
    e.device_value_egp,
    e.contract_value_egp,
    e.credit_check_passed,
    e.expected_delivery_date
FROM telecomservice.onelakedatabase.silver_telecom_events e
WHERE e.event_type = 'corporate_order';
