# Lakeflow pipeline evidence

pipeline_id `6404647c-70df-4524-849f-9543729fdd94`

## Data-quality expectations (latest update)

| dataset | expectation | failed | passed |
|---|---|---|---|
| main.meridian.silver_telemetry | non_negative_vibration | 0 | 172800 |
| main.meridian.silver_telemetry | plausible_temp | 20 | 172780 |
| main.meridian.silver_telemetry | valid_machine | 20 | 172780 |
| main.meridian.silver_telemetry | valid_ts | 0 | 172800 |
| main.meridian.silver_work_orders | valid_wo | 0 | 346 |

The 20 null machine ids and 20 impossible 999 °C readings seeded by the generator are dropped by the `valid_machine` / `plausible_temp` expectations.

## Event timeline (most recent first)

- 2026-11-24T19:24:06.791Z · update_progress ·  · Update 92a49e is COMPLETED.
- 2026-11-24T19:24:06.729Z · flow_progress · main.meridian.gold_machine_daily_kpis · Flow 'main.meridian.gold_machine_daily_kpis' has COMPLETED.
- 2026-11-24T19:24:04.233Z · flow_progress · main.meridian.gold_machine_daily_kpis · Flow 'main.meridian.gold_machine_daily_kpis' is RUNNING.
- 2026-11-24T19:24:04.161Z · flow_progress · main.meridian.gold_machine_daily_kpis · Flow 'main.meridian.gold_machine_daily_kpis' is STARTING.
- 2026-11-24T19:24:04.158Z · planning_information · main.meridian.gold_machine_daily_kpis · Flow 'main.meridian.gold_machine_daily_kpis' has been planned to be executed as COMPLETE_RECOMPUTE.
- 2026-11-24T19:24:03.920Z · flow_progress · main.meridian.gold_machine_daily_kpis · Flow 'main.meridian.gold_machine_daily_kpis' is PLANNING.
- 2026-11-24T19:24:03.915Z · flow_progress · main.meridian.gold_failure_events · Flow 'main.meridian.gold_failure_events' has COMPLETED.
- 2026-11-24T19:24:02.981Z · flow_progress · main.meridian.silver_telemetry · Flow 'main.meridian.silver_telemetry' has COMPLETED.
- 2026-11-24T19:24:02.946Z · flow_progress · main.meridian.silver_telemetry · Completed a streaming update of 'main.meridian.silver_telemetry'.
- 2026-11-24T19:24:01.676Z · flow_progress · main.meridian.silver_production_orders · Flow 'main.meridian.silver_production_orders' has COMPLETED.
- 2026-11-24T19:24:01.647Z · flow_progress · main.meridian.silver_manuals · Flow 'main.meridian.silver_manuals' has COMPLETED.
- 2026-11-24T19:24:01.419Z · flow_progress · main.meridian.gold_failure_events · Flow 'main.meridian.gold_failure_events' is RUNNING.
- 2026-11-24T19:24:01.358Z · flow_progress · main.meridian.gold_failure_events · Flow 'main.meridian.gold_failure_events' is STARTING.
- 2026-11-24T19:24:01.351Z · planning_information · main.meridian.gold_failure_events · Flow 'main.meridian.gold_failure_events' has been planned to be executed as COMPLETE_RECOMPUTE.
- 2026-11-24T19:24:01.079Z · flow_progress · main.meridian.gold_failure_events · Flow 'main.meridian.gold_failure_events' is PLANNING.
- 2026-11-24T19:24:00.038Z · flow_progress · main.meridian.silver_telemetry · Flow 'main.meridian.silver_telemetry' is RUNNING.
- 2026-11-24T19:24:00.015Z · flow_progress · main.meridian.silver_work_orders · Flow 'main.meridian.silver_work_orders' has COMPLETED.
- 2026-11-24T19:23:59.562Z · flow_progress · main.meridian.silver_telemetry · Flow 'main.meridian.silver_telemetry' is STARTING.
- 2026-11-24T19:23:59.231Z · flow_progress · main.meridian.silver_machines · Flow 'main.meridian.silver_machines' has COMPLETED.
- 2026-11-24T19:23:59.202Z · flow_progress · main.meridian.silver_technicians · Flow 'main.meridian.silver_technicians' has COMPLETED.
- 2026-11-24T19:23:57.407Z · flow_progress · main.meridian.silver_manuals · Flow 'main.meridian.silver_manuals' is RUNNING.
- 2026-11-24T19:23:57.396Z · flow_progress · main.meridian.silver_production_orders · Flow 'main.meridian.silver_production_orders' is RUNNING.
- 2026-11-24T19:23:57.373Z · flow_progress · main.meridian.silver_production_orders · Flow 'main.meridian.silver_production_orders' is STARTING.
- 2026-11-24T19:23:57.371Z · planning_information · main.meridian.silver_production_orders · Flow 'main.meridian.silver_production_orders' has been planned to be executed as COMPLETE_RECOMPUTE.
- 2026-11-24T19:23:57.360Z · flow_progress · main.meridian.silver_manuals · Flow 'main.meridian.silver_manuals' is STARTING.
- 2026-11-24T19:23:57.353Z · planning_information · main.meridian.silver_manuals · Flow 'main.meridian.silver_manuals' has been planned to be executed as COMPLETE_RECOMPUTE.
- 2026-11-24T19:23:57.075Z · flow_progress · main.meridian.silver_production_orders · Flow 'main.meridian.silver_production_orders' is PLANNING.
- 2026-11-24T19:23:57.071Z · flow_progress · main.meridian.silver_manuals · Flow 'main.meridian.silver_manuals' is PLANNING.
- 2026-11-24T19:23:57.052Z · flow_progress · main.meridian.bronze_telemetry · Flow 'main.meridian.bronze_telemetry' has COMPLETED.
- 2026-11-24T19:23:56.652Z · flow_progress · main.meridian.bronze_telemetry · Completed a streaming update of 'main.meridian.bronze_telemetry'.
- 2026-11-24T19:23:55.786Z · flow_progress · main.meridian.silver_technicians · Flow 'main.meridian.silver_technicians' is RUNNING.
- 2026-11-24T19:23:55.735Z · flow_progress · main.meridian.silver_work_orders · Flow 'main.meridian.silver_work_orders' is RUNNING.
- 2026-11-24T19:23:55.733Z · flow_progress · main.meridian.silver_machines · Flow 'main.meridian.silver_machines' is RUNNING.
- 2026-11-24T19:23:55.703Z · flow_progress · main.meridian.silver_machines · Flow 'main.meridian.silver_machines' is STARTING.
- 2026-11-24T19:23:55.698Z · flow_progress · main.meridian.silver_work_orders · Flow 'main.meridian.silver_work_orders' is STARTING.
- 2026-11-24T19:23:55.689Z · flow_progress · main.meridian.silver_technicians · Flow 'main.meridian.silver_technicians' is STARTING.
- 2026-11-24T19:23:55.687Z · planning_information · main.meridian.silver_machines · Flow 'main.meridian.silver_machines' has been planned to be executed as COMPLETE_RECOMPUTE.
- 2026-11-24T19:23:55.686Z · planning_information · main.meridian.silver_technicians · Flow 'main.meridian.silver_technicians' has been planned to be executed as COMPLETE_RECOMPUTE.
- 2026-11-24T19:23:55.670Z · planning_information · main.meridian.silver_work_orders · Flow 'main.meridian.silver_work_orders' has been planned to be executed as COMPLETE_RECOMPUTE.
- 2026-11-24T19:23:55.050Z · flow_progress · main.meridian.silver_technicians · Flow 'main.meridian.silver_technicians' is PLANNING.
- 2026-11-24T19:23:55.046Z · flow_progress · main.meridian.silver_machines · Flow 'main.meridian.silver_machines' is PLANNING.
- 2026-11-24T19:23:55.040Z · flow_progress · main.meridian.silver_work_orders · Flow 'main.meridian.silver_work_orders' is PLANNING.
- 2026-11-24T19:23:54.893Z · flow_progress · main.meridian.bronze_production_orders · Flow 'main.meridian.bronze_production_orders' has COMPLETED.
- 2026-11-24T19:23:54.808Z · flow_progress · main.meridian.bronze_production_orders · Completed a streaming update of 'main.meridian.bronze_production_orders'.
- 2026-11-24T19:23:54.154Z · flow_progress · main.meridian.bronze_manuals · Flow 'main.meridian.bronze_manuals' has COMPLETED.
- 2026-11-24T19:23:54.148Z · flow_progress · main.meridian.bronze_manuals · Completed a streaming update of 'main.meridian.bronze_manuals'.
- 2026-11-24T19:23:54.144Z · flow_progress · main.meridian.bronze_machines · Flow 'main.meridian.bronze_machines' has COMPLETED.
- 2026-11-24T19:23:54.141Z · flow_progress · main.meridian.bronze_work_orders · Flow 'main.meridian.bronze_work_orders' has COMPLETED.
- 2026-11-24T19:23:54.135Z · flow_progress · main.meridian.bronze_technicians · Flow 'main.meridian.bronze_technicians' has COMPLETED.
- 2026-11-24T19:23:53.907Z · flow_progress · main.meridian.bronze_technicians · Completed a streaming update of 'main.meridian.bronze_technicians'.
- 2026-11-24T19:23:53.900Z · flow_progress · main.meridian.bronze_machines · Completed a streaming update of 'main.meridian.bronze_machines'.
- 2026-11-24T19:23:53.777Z · flow_progress · main.meridian.bronze_work_orders · Completed a streaming update of 'main.meridian.bronze_work_orders'.
- 2026-11-24T19:23:50.956Z · flow_progress · main.meridian.bronze_production_orders · Flow 'main.meridian.bronze_production_orders' is RUNNING.
- 2026-11-24T19:23:50.481Z · flow_progress · main.meridian.bronze_production_orders · Flow 'main.meridian.bronze_production_orders' is STARTING.
- 2026-11-24T19:23:50.340Z · flow_progress · main.meridian.bronze_manuals · Flow 'main.meridian.bronze_manuals' is RUNNING.
- 2026-11-24T19:23:49.861Z · flow_progress · main.meridian.bronze_manuals · Flow 'main.meridian.bronze_manuals' is STARTING.
- 2026-11-24T19:23:49.728Z · flow_progress · main.meridian.bronze_machines · Flow 'main.meridian.bronze_machines' is RUNNING.
- 2026-11-24T19:23:49.255Z · flow_progress · main.meridian.bronze_machines · Flow 'main.meridian.bronze_machines' is STARTING.
- 2026-11-24T19:23:49.110Z · flow_progress · main.meridian.bronze_work_orders · Flow 'main.meridian.bronze_work_orders' is RUNNING.
- 2026-11-24T19:23:48.638Z · flow_progress · main.meridian.bronze_work_orders · Flow 'main.meridian.bronze_work_orders' is STARTING.
