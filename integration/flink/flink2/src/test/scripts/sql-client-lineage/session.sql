-- Copyright 2018-2026 contributors to the OpenLineage project
-- SPDX-License-Identifier: Apache-2.0

SET 'execution.target' = 'local';
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'rest.bind-address' = '127.0.0.1';
SET 'rest.bind-port' = '0';
SET 'table.dml-sync' = 'true';
SET 'execution.job-status-changed-listeners' = 'io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory';
SET 'openlineage.transport.type' = 'file';
SET 'openlineage.transport.location' = '__ROOT__/events.jsonl';
SET 'openlineage.flink.disableCheckpointTracking' = 'true';
CREATE DATABASE lineage_acceptance;
USE lineage_acceptance;
CREATE TABLE Orders (order_id BIGINT, customer_id BIGINT, amount BIGINT, fee BIGINT)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/orders.csv', 'format'='csv');
CREATE TABLE Customers (customer_id BIGINT, tier STRING)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/customers.csv', 'format'='csv');
CREATE TABLE Detail (order_id BIGINT, tier STRING, net_amount BIGINT)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/detail', 'format'='csv');
CREATE TABLE Summary (tier STRING, total_amount BIGINT, order_count BIGINT)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/summary', 'format'='csv');
CREATE TEMPORARY VIEW Enriched AS
SELECT o.order_id, c.tier, o.amount + o.fee AS net_amount
FROM Orders o JOIN Customers c ON o.customer_id = c.customer_id
WHERE c.tier <> 'blocked';
EXECUTE STATEMENT SET
BEGIN
INSERT INTO Detail SELECT order_id, tier, net_amount FROM Enriched;
INSERT INTO Summary SELECT tier, SUM(net_amount), COUNT(order_id) FROM Enriched GROUP BY tier;
END;
