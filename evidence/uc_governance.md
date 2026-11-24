# Unity Catalog governance evidence

Captured 2026-11-24 19:30 UTC via SQL on warehouse.

## Tables in the governed schema (with comments)

```sql
SELECT table_name, table_type, comment FROM main.information_schema.tables WHERE table_schema = 'meridian' ORDER BY table_name
```

| comment | table_name | table_type |
|---|---|---|
| Raw asset registry export | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_bronze_machines_1 | MANAGED |
| Raw maintenance-manual sections (grounding for GenAI repair plans) | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_bronze_manuals_1 | MANAGED |
| Raw MES production-order export (next 7 days) | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_bronze_production_orders_1 | MANAGED |
| Raw shift roster from HR (contains PII) | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_bronze_technicians_1 | MANAGED |
| Raw hourly sensor rollups from the plant historian (JSON lines, one file/day) | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_bronze_telemetry_1 | MANAGED |
| Raw CMMS work-order export | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_bronze_work_orders_1 | MANAGED |
| Historical unplanned failures with cost of downtime (parts + labor + lost contribution) | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_gold_failure_events_1 | MANAGED |
| Per machine per day: availability, unplanned downtime, output, sensor health | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_gold_machine_daily_kpis_1 | MANAGED |
| Asset registry: one row per CNC machine | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_silver_machines_1 | MANAGED |
| Maintenance-manual sections keyed by machine type + failure mode | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_silver_manuals_1 | MANAGED |
| MES production orders for the next 7 days, with OEM line-down penalty terms | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_silver_production_orders_1 | MANAGED |
| Maintenance technicians. Phone/e-mail masked (PII); rows filtered by plant. | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_silver_technicians_1 | MANAGED |
| Validated hourly machine telemetry. Drops null ids and physically impossible temperatures. | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_silver_telemetry_1 | MANAGED |
| CMMS work orders (corrective + preventive) with typed costs and downtime | __materialization_mat_6404647c_70df_4524_849f_9543729fdd94_silver_work_orders_1 | MANAGED |
| Raw asset registry export | bronze_machines | STREAMING_TABLE |
| Raw maintenance-manual sections (grounding for GenAI repair plans) | bronze_manuals | STREAMING_TABLE |
| Raw MES production-order export (next 7 days) | bronze_production_orders | STREAMING_TABLE |
| Raw shift roster from HR (contains PII) | bronze_technicians | STREAMING_TABLE |
| Raw hourly sensor rollups from the plant historian (JSON lines, one file/day) | bronze_telemetry | STREAMING_TABLE |
| Raw CMMS work-order export | bronze_work_orders | STREAMING_TABLE |
|  | event_log_6404647c_70df_4524_849f_9543729fdd94 | MANAGED |
| Historical unplanned failures with cost of downtime (parts + labor + lost contribution) | gold_failure_events | MATERIALIZED_VIEW |
| Per machine per day: availability, unplanned downtime, output, sensor health | gold_machine_daily_kpis | MATERIALIZED_VIEW |
| Hourly ML features per machine (rolling stats + drift vs own 14-day baseline). Produced by notebook 02. | gold_machine_features_hourly | MANAGED |
| One row per CNC machine: P(failure 48h) from UC model machine_failure_48h@champion, likely failure mode + reason, next OEM order, and $ exposure. Ranked by prio | gold_machine_risk | MANAGED |
|  | gold_model_backtest | MANAGED |
|  | gold_repair_plans | MANAGED |
| Asset registry: one row per CNC machine | silver_machines | MATERIALIZED_VIEW |
| Maintenance-manual sections keyed by machine type + failure mode | silver_manuals | MATERIALIZED_VIEW |
| MES production orders for the next 7 days, with OEM line-down penalty terms | silver_production_orders | MATERIALIZED_VIEW |
| Maintenance technicians. Phone/e-mail masked (PII); rows filtered by plant. | silver_technicians | MATERIALIZED_VIEW |
| Validated hourly machine telemetry. Drops null ids and physically impossible temperatures. | silver_telemetry | STREAMING_TABLE |
| CMMS work orders (corrective + preventive) with typed costs and downtime | silver_work_orders | MATERIALIZED_VIEW |

## Column masks attached (PII)

```sql
SELECT table_name, column_name, mask_name FROM main.information_schema.column_masks WHERE table_schema = 'meridian'
```

| column_name | mask_name | table_name |
|---|---|---|
| phone | main.meridian.mask_pii | silver_technicians |
| email | main.meridian.mask_pii | silver_technicians |

## Row filters attached (plant scoping)

```sql
SELECT table_name, filter_name, target_columns FROM main.information_schema.row_filters WHERE table_schema = 'meridian'
```

| filter_name | table_name | target_columns |
|---|---|---|
| main.meridian.plant_filter | silver_technicians | plant_id |

## Governance functions

```sql
SELECT routine_name, comment FROM main.information_schema.routines WHERE routine_schema = 'meridian' ORDER BY 1
```

| comment | routine_name |
|---|---|
| Masks technician PII unless caller is in admins or meridian_hr | mask_pii |
| Row filter: admins / corporate reliability see all plants; meridian_plant_<id> groups see their plant | plant_filter |

## Mask definition (non-admin, non-HR callers get the ELSE branch)

```sql
SELECT routine_name, routine_definition FROM main.information_schema.routines WHERE routine_schema = 'meridian' AND routine_name = 'mask_pii'
```

| routine_definition | routine_name |
|---|---|
| CASE   WHEN is_account_group_member('admins') OR is_member('admins') OR is_account_group_member('meridian_hr') THEN val   ELSE concat('***', right(val, 4)) END | mask_pii |

## Mask in action — this capture runs as a workspace admin, so `phone`/`email` come back clear; the last two columns apply the mask's non-HR branch to the same rows

```sql
SELECT technician_id, plant_id, phone, email, concat('***', right(phone, 4)) AS phone_as_seen_by_non_hr, concat('***', right(email, 4)) AS email_as_seen_by_non_hr FROM main.meridian.silver_technicians ORDER BY technician_id LIMIT 4
```

| email | email_as_seen_by_non_hr | phone | phone_as_seen_by_non_hr | plant_id | technician_id |
|---|---|---|---|---|---|
| diego.cantú@meridian-precision.example | ***mple | +52 81 3615 7312 | ***7312 | MTY | T-MTY-1 |
| paola.salinas@meridian-precision.example | ***mple | +52 81 5371 2391 | ***2391 | MTY | T-MTY-2 |
| ricardo.ortega@meridian-precision.example | ***mple | +52 81 7906 4896 | ***4896 | MTY | T-MTY-3 |
| fernanda.medina@meridian-precision.example | ***mple | +52 81 2298 7596 | ***7596 | MTY | T-MTY-4 |

## Grants on the schema (app service principal = read-only)

```sql
SHOW GRANTS ON SCHEMA main.meridian
```

| ActionType | ObjectKey | ObjectType | Principal |
|---|---|---|---|
| SELECT | main.meridian | SCHEMA | <app-sp-client-id> |
| USE SCHEMA | main.meridian | SCHEMA | <app-sp-client-id> |

## Lineage captured by Unity Catalog (system.access.table_lineage)

```sql
SELECT DISTINCT source_table_name, target_table_name, entity_type FROM system.access.table_lineage
         WHERE target_table_schema = 'meridian' AND target_table_catalog = 'main' AND source_table_name IS NOT NULL
           AND event_date >= current_date() - 2 ORDER BY target_table_name, source_table_name
```

| entity_type | source_table_name | target_table_name |
|---|---|---|
| PIPELINE | silver_machines | gold_failure_events |
| PIPELINE | silver_work_orders | gold_failure_events |
| PIPELINE | silver_machines | gold_machine_daily_kpis |
| PIPELINE | silver_telemetry | gold_machine_daily_kpis |
| PIPELINE | bronze_machines | silver_machines |
| PIPELINE | bronze_manuals | silver_manuals |
| PIPELINE | bronze_production_orders | silver_production_orders |
| PIPELINE | bronze_technicians | silver_technicians |
| PIPELINE | bronze_telemetry | silver_telemetry |
| PIPELINE | bronze_work_orders | silver_work_orders |

## Registered model in Unity Catalog

- name: `main.meridian.machine_failure_48h`
- description: P(unplanned failure within 48h) per CNC machine-hour. Features: rolling sensor stats + drift vs own baseline. Owner: Reliability Engineering.
- aliases: [('champion', 4)]

| version | status | run_id | created |
|---|---|---|---|
| 1 | READY | f8674314a03f420aab727feca86e3473 | 2026-11-24 19:04 |
| 2 | READY | e129287a8b73432b81552dda212a88c0 | 2026-11-24 19:10 |
| 3 | READY | ac937083b7534abcbefbe3bda045b275 | 2026-11-24 19:18 |
| 4 | READY | 454a8e031ac24c3f91cdce09dbd29c4d | 2026-11-24 19:26 |

