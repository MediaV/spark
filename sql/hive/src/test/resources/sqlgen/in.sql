-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT id FROM parquet_t0 WHERE id IN (1, 2, 3)
--------------------------------------------------------------------------------
SELECT `gen_attr` AS `id` FROM (SELECT `gen_attr` FROM (SELECT `id` AS `gen_attr` FROM `default`.`parquet_t0`) AS gen_subquery_0 WHERE (CAST(`gen_attr` AS BIGINT) IN (CAST(1 AS BIGINT), CAST(2 AS BIGINT), CAST(3 AS BIGINT)))) AS parquet_t0