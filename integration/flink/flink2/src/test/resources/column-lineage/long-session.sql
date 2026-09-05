-- Copyright 2018-2026 contributors to the OpenLineage project
-- SPDX-License-Identifier: Apache-2.0
-- Test fixture: each marker separates one statement, not a general SQL script parser.
-- The harness registers lineage_catalog, sets Asia/Shanghai and substitutes values data IDs.
-- statement
USE CATALOG lineage_catalog;
-- statement
CREATE DATABASE commerce;
-- statement
USE commerce;
-- statement
CREATE TEMPORARY FUNCTION add_fee
AS 'io.openlineage.flink.listener.ColumnLineageLongSessionE2ETest$AddFee';
-- statement
CREATE TEMPORARY TABLE RawOrders (
  order_id BIGINT,
  customer_id BIGINT,
  payload STRING,
  fee BIGINT
) WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '${ordersData}');
-- statement
CREATE TEMPORARY TABLE Customers (
  customer_id BIGINT,
  tier STRING
) WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '${customersData}');
-- statement
CREATE TEMPORARY TABLE OrderDetail (
  order_id BIGINT,
  tier STRING,
  net_amount BIGINT
) WITH ('connector' = 'values');
-- statement
CREATE TEMPORARY TABLE TierSummary (
  tier STRING,
  total_amount BIGINT,
  order_count BIGINT
) WITH ('connector' = 'values', 'sink-insert-only' = 'false');
-- statement
CREATE TEMPORARY VIEW ParsedOrders AS
SELECT order_id, customer_id, fee,
       CAST(JSON_VALUE(payload, '$.amount') AS BIGINT) AS amount,
       JSON_VALUE(payload, '$.status') AS order_status
FROM RawOrders;
-- statement
CREATE TEMPORARY VIEW PaidOrders AS
SELECT order_id, customer_id, add_fee(amount, fee) AS net_amount
FROM ParsedOrders
WHERE order_status = 'paid' AND amount IS NOT NULL;
-- statement
CREATE TEMPORARY VIEW EnrichedOrders AS
SELECT o.order_id, c.tier, o.net_amount
FROM PaidOrders o
JOIN Customers c ON o.customer_id = c.customer_id
WHERE c.tier <> 'blocked';
-- statement
INSERT INTO OrderDetail
SELECT order_id, tier, net_amount FROM EnrichedOrders;
-- statement
INSERT INTO TierSummary
SELECT tier, SUM(net_amount), COUNT(order_id)
FROM EnrichedOrders
GROUP BY tier;
