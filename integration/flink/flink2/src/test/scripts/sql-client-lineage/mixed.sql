-- Copyright 2018-2026 contributors to the OpenLineage project
-- SPDX-License-Identifier: Apache-2.0
SET 'execution.target' = 'local';
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'table.dml-sync' = 'true';
SET 'execution.job-status-changed-listeners' = 'io.openlineage.flink.listener.OpenLineageJobStatusChangedListenerFactory';
SET 'openlineage.transport.type' = 'file';
SET 'openlineage.transport.location' = '__ROOT__/events.jsonl';
SET 'openlineage.flink.disableCheckpointTracking' = 'true';
CREATE DATABASE lineage_acceptance;
USE lineage_acceptance;
CREATE TABLE Numbers (`value` BIGINT)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/numbers.csv', 'format'='csv');
CREATE TABLE OtherNumbers (`value` BIGINT)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/other-numbers.csv', 'format'='csv');
CREATE TABLE Good (`value` BIGINT)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/good', 'format'='csv');
CREATE TABLE Unsupported (`value` BIGINT)
WITH ('connector'='filesystem', 'path'='file://__ROOT__/unsupported', 'format'='csv');
EXECUTE STATEMENT SET
BEGIN
INSERT INTO Good SELECT `value` + 1 FROM Numbers;
INSERT INTO Unsupported SELECT n.`value` FROM Numbers n WHERE EXISTS (SELECT 1 FROM OtherNumbers r WHERE r.`value` = n.`value`);
END;
