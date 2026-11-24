# Genie Agent evidence — real Conversation API answers

Space `<genie-space-id>` ("Meridian Reliability — Downtime & Risk"), definition in [`genie/meridian_reliability_space.json`](../genie/meridian_reliability_space.json).
Captured 2026-11-24 19:33 UTC.

## Q: Which machines should we fix first?

status: **COMPLETED**

**Genie:** You want to see a list of machines that should be fixed first, focusing on those with a medium or high risk of failure. The list includes each machine's priority rank, failure probability as a percentage, risk category, likely failure mode, associated OEM customer, buffer hours for finished goods, and the estimated financial exposure if the machine fails. The results are ordered by priority rank to highlight the most urgent cases.

```sql
SELECT `priority_rank`, `machine_id`, `plant_name`, ROUND(`failure_prob_48h` * 100, 1) AS `failure_prob_pct`, `risk_band`, `likely_failure_mode`, `oem_customer`, `finished_goods_buffer_hours`, ROUND(`exposure_usd`) AS `exposure_usd` FROM `main`.`meridian`.`gold_machine_risk` WHERE `risk_band` <> 'LOW' ORDER BY `priority_rank`
```

| priority_rank | machine_id | plant_name | failure_prob_pct | risk_band | likely_failure_mode | oem_customer | finished_goods_buffer_hours | exposure_usd |
|---|---|---|---|---|---|---|---|---|
| 1 | M-SAL-07 | Saltillo | 100.0 | ACT NOW | SPINDLE_BEARING | Orion Motors | 12.0 | 441995.0 |
| 2 | M-QRO-03 | Querétaro | 79.1 | ACT NOW | SERVO_AXIS | Vanta EV | 35.0 | 20916.0 |
| 3 | M-MTY-12 | Monterrey | 97.6 | ACT NOW | COOLANT_PUMP | Kestrel Trucks | 16.0 | 16882.0 |

**Genie:** You should fix the **3 at-risk machines** in this priority order, based on the ranking shown.

- **#1 M-SAL-07** (**Saltillo**) — **ACT NOW**, **100.0%** failure probability in 48 hours, likely issue **Spindle Bearing**, exposure **$441,995**
- **#2 M-QRO-03** (**Querétaro**) — **ACT NOW**, **79.1%** failure probability, likely issue **Servo Axis**, exposure **$20,916**
- **#3 M-MTY-12** (**Monterrey**) — **ACT NOW**, **97.6%** failure probability, likely issue **Coolant Pump**, exposure **$16,882**

The top priority is clearly **M-SAL-07**, with far higher exposure than the other two machines.

## Q: Which plant lost the most hours to unplanned downtime in the last 90 days?

status: **COMPLETED**

**Genie:** You want to find out which plant had the highest total unplanned downtime hours in the last 90 days.

```sql
WITH `plant_downtime` AS (
  SELECT
    `plant_name`,
    SUM(`unplanned_down_hours`) AS `unplanned_down_hours`
  FROM `main`.`meridian`.`gold_machine_daily_kpis`
  WHERE `day` > (
    SELECT date_sub(MAX(`day`), 90)
    FROM `main`.`meridian`.`gold_machine_daily_kpis`
  )
    AND `plant_name` IS NOT NULL
    AND `unplanned_down_hours` IS NOT NULL
  GROUP BY `plant_name`
),
`ranked_plants` AS (
  SELECT
    `plant_name`,
    `unplanned_down_hours`,
    RANK() OVER (ORDER BY `unplanned_down_hours` DESC) AS `rnk`
  FROM `plant_downtime`
)
SELECT
  `plant_name`,
  `unplanned_down_hours`
FROM `ranked_plants`
WHERE `rnk` = 1
```

| plant_name | unplanned_down_hours |
|---|---|
| Monterrey | 487 |

**Genie:** **Monterrey** lost the most unplanned downtime in the last 90 days, with **487 hours**. Based on the result shown, **Monterrey** is the top plant for unplanned downtime over that period.

**Genie:** Would you prefer to see the plant with the most unplanned downtime hours for the last 30 or 60 days instead?

## Q: What is the total cost of spindle bearing failures by plant?

status: **COMPLETED**

**Genie:** You want to see the total cost of failures caused by spindle bearing issues, broken down by each plant.

```sql
SELECT `plant_id`, ROUND(SUM(`total_cost_usd`)) AS `total_cost_usd`
FROM `main`.`meridian`.`gold_failure_events`
WHERE `failure_mode` = 'SPINDLE_BEARING'
  AND `plant_id` IS NOT NULL
GROUP BY `plant_id`
ORDER BY `total_cost_usd` DESC
```

| plant_id | total_cost_usd |
|---|---|
| MTY | 922081.0 |
| QRO | 725548.0 |
| SAL | 658927.0 |

**Genie:** The total cost of **Spindle Bearing** failures is highest at **MTY (Monterrey)** with **$922,081**, followed by **QRO (Querétaro)** at **$725,548** and **SAL (Saltillo)** at **$658,927**. Across the 3 plants shown, **MTY** has the highest cost and **SAL** the lowest.

## Q: Which OEM customers are exposed to a line-down risk in the next 48 hours, and how much?

status: **COMPLETED**

**Genie:** You want to see which OEM customers face a risk of line-down in the next 48 hours and the total exposure amount for each customer.

```sql
SELECT `oem_customer`, ROUND(SUM(`exposure_usd`)) AS `exposure_usd` FROM `main`.`meridian`.`gold_machine_risk` WHERE `risk_band` IN ('ACT NOW', 'WATCH') AND `oem_customer` IS NOT NULL GROUP BY `oem_customer` ORDER BY `exposure_usd` DESC
```

| oem_customer | exposure_usd |
|---|---|
| Orion Motors | 441995.0 |
| Vanta EV | 20916.0 |
| Kestrel Trucks | 16882.0 |

**Genie:** **3 OEM customers** are exposed to line-down risk in the next 48 hours, led by **Orion Motors** at **$441,995**, followed by **Vanta EV** at **$20,916** and **Kestrel Trucks** at **$16,882**. The exposure is heavily concentrated in **Orion Motors**, which is far higher than the other two customers.

## Q: How much net value did the failure model deliver in the backtest, and how many failures did it catch?

status: **COMPLETED**

**Genie:** You want to see the total net value delivered by the failure model in the backtest and the total number of failures it caught.

```sql
SELECT ROUND(SUM(`net_value_usd`)) AS `net_value_usd`, SUM(`caught`) AS `failures_caught` FROM `main`.`meridian`.`gold_model_backtest`
```

| net_value_usd | failures_caught |
|---|---|
| 467698.0 | 30 |

**Genie:** In the backtest, the failure model delivered **$467,698** in net value and caught **30** failures. Based on the single backtest result shown, that equals **30** total failures caught across the data included in the query.

## Q: Show me the technicians' phone numbers

status: **COMPLETED**

**Genie:** I can’t answer that from this database because there is no table or column for technician phone numbers. The closest available field is `silver_work_orders`.`technician_id`, but no contact details are provided.

