# Sales Funnel Drop-off Analysis

> **Where do customers drop off — and what does it cost?**

A pure SQL analytics project processing **109 million real e-commerce events** to identify funnel drop-off, revenue concentration and channel performance across a 2-month window of an Eastern European electronics retailer.

Built entirely in **PostgreSQL 18.3** with no Python — raw data ingestion, a 5-step preprocessing pipeline, funnel analysis, week-over-week trend tracking, revenue concentration and brand efficiency — visualised across 3 interactive Tableau Public dashboards and a standalone HTML dashboard.

---

## Live Dashboards

### Overview
> Funnel drop-off by stage · KPI tiles · Preprocessing obstacle log

[![Overview Dashboard](https://img.shields.io/badge/Tableau_Public-Overview-1D9E75?style=for-the-badge&logo=tableau&logoColor=white)](https://public.tableau.com/app/profile/anirudh.raghavendra/viz/SalesFunnel1_17772564713410/Overview)

### Performance
> Weekly revenue trends · Category scatter · Brand efficiency · Day of week conversion · Cart abandonment trend

[![Performance Dashboard](https://img.shields.io/badge/Tableau_Public-Performance-185FA5?style=for-the-badge&logo=tableau&logoColor=white)](https://public.tableau.com/app/profile/anirudh.raghavendra/viz/SalesFunnel2/Performance)

### Revenue & Insights
> Price tier revenue · Pareto analysis · High value user segments · 5 business recommendations

[![Revenue & Insights Dashboard](https://img.shields.io/badge/Tableau_Public-Revenue_%26_Insights-BF3A0E?style=for-the-badge&logo=tableau&logoColor=white)](https://public.tableau.com/app/profile/anirudh.raghavendra/viz/SalesFunnel3/RevenueInsights)

---

## The Most Surprising Finding

Cart abandonment is **not a price problem.**

Converted and abandoned users had virtually identical average cart prices — **$209.70 vs $208.06**. A difference of $1.64. The barrier is engagement depth. Converted users viewed 40% more products and had twice as many cart interactions before purchasing.

---

## Key Findings

| Metric | Value |
|---|---|
| Overall conversion rate | 12.90% |
| Biggest drop-off stage | View → Cart — 80.69% |
| Users lost at view → cart | 4,095,942 |
| Total revenue analysed | $315,077,167 |
| Singles Day revenue share | 21.53% of 9-week total |
| Top 10% buyers revenue share | 49.83% |
| Apple revenue per viewer | $83.56 vs Samsung $49.42 |
| Best converting day | Sunday 13.26% vs Friday 7.61% |
| Cart abandonment rate | 46.38% — price not the cause |

---

## Dataset

**E-commerce Behaviour Data from Multi-Category Store**
Source: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

| File | Size | Rows |
|---|---|---|
| events_oct.csv | 8.7GB | 42,481,998 |
| events_nov.csv | 5.5GB | 67,395,246 |
| **Combined** | **14.2GB** | **109,877,244** |

> Raw CSV files are not included in this repository due to size.
> See `data/raw/DATA_SOURCE.md` for download instructions.

---

## Tech Stack

- **Database:** PostgreSQL 18.3
- **Query tool:** pgAdmin 4
- **SQL techniques:** CTEs, window functions, LAG/LEAD, PERCENTILE_CONT, IQR outlier detection, DATE_TRUNC, ROW_NUMBER, CASE statements
- **Visualisation:** Tableau Public (3 dashboards) + standalone HTML/CSS/JS dashboard
- **Version control:** Git / GitHub

---

## Project Structure

```
Sales Funnel/
│
├── data/
│   ├── raw/
│   │   └── DATA_SOURCE.md          ← Kaggle download instructions
│   └── exports/                    ← 25 clean CSV files for Tableau
│       ├── funnel_conversion.csv
│       ├── dropoff_by_stage.csv
│       ├── dropoff_by_category.csv
│       ├── dropoff_by_brand.csv
│       ├── cart_abandonment_profile.csv
│       ├── weekly_dropoff_trend.csv
│       ├── category_performance.csv
│       ├── category_quadrant.csv
│       ├── brand_performance.csv
│       ├── hourly_conversion.csv
│       ├── weekend_vs_weekday.csv
│       ├── wow_core_metrics.csv
│       ├── wow_rolling_averages.csv
│       ├── wow_by_category.csv
│       ├── cumulative_revenue.csv
│       ├── wow_cart_abandonment.csv
│       ├── revenue_by_price_tier.csv
│       ├── category_efficiency.csv
│       ├── brand_efficiency.csv
│       ├── high_value_users.csv
│       ├── revenue_concentration.csv
│       ├── overall_conversion_summary.csv
│       ├── conversion_by_price_tier.csv
│       ├── conversion_by_day.csv
│       └── final_summary_kpis.csv
│
├── schema/
│   ├── 01_create_tables.sql        ← Creates all 4 database tables
│   └── 02_load_data.sql            ← Loads Oct + Nov CSVs via staging
│
├── preprocessing/
│   ├── 03_null_handling.sql        ← Fills 50.7M nulls, drops 12 rows
│   ├── 04_deduplication.sql        ← Removes 130,750 duplicate events
│   ├── 05_date_normalisation.sql   ← UTC conversion, 7 derived columns
│   ├── 06_outlier_detection.sql    ← IQR price outlier flagging
│   └── 07_master_clean.sql         ← vw_clean_events view + indexes
│
├── analysis/
│   ├── 08_funnel_conversion.sql    ← 7-stage funnel, conversion rates
│   ├── 09_dropoff_analysis.sql     ← Stage, category, brand drop-off
│   ├── 10_traffic_source.sql       ← Category and brand performance
│   ├── 11_week_over_week.sql       ← LAG/LEAD trend analysis
│   └── 12_roi_by_channel.sql       ← Revenue efficiency, Pareto
│
├── reports/
│   └── 13_final_summary.sql        ← Executive summary, recommendations
│
├── tableau/
│   └── sales_funnel_dashboard_final.html  ← Standalone interactive dashboard
│
└── README.md                       ← This file
```

---

## Preprocessing Pipeline

The preprocessing pipeline is the core technical work of this project — not the charts.

| Step | Script | Action | Rows Affected |
|---|---|---|---|
| Null handling | 03 | category_code filled | 35,413,780 |
| Null handling | 03 | brand filled | 15,331,243 |
| Null handling | 03 | sessionless rows dropped | 12 |
| Deduplication | 04 | cart event flooding removed | 126,996 |
| Deduplication | 04 | page refresh duplicates removed | 3,669 |
| Deduplication | 04 | payment page duplicates removed | 85 |
| Date normalisation | 05 | 7 derived date columns added | 109,819,981 |
| Date normalisation | 05 | DST boundary verified clean | 0 |
| Outlier detection | 06 | price outliers flagged (>$797.83) | 9,617,652 |
| Outlier detection | 06 | zero price rows flagged | 256,657 |
| Master clean | 07 | vw_clean_events view created | 109,819,981 |
| Indexes | 07 | 6 indexes added | query time 60min→5sec |

---

## The 7-Stage Funnel

Derived from 3 raw event types (view, cart, purchase):

| Stage | Users | Conversion | Drop-off |
|---|---|---|---|
| 1 — Exposure | 5,076,814 | — | — |
| 2 — View | 5,076,318 | 99.99% | 0.01% |
| 3 — Repeat view | 3,225,600 | 63.54% | 36.46% |
| 4 — Cart | 980,376 | 19.31% | 80.69% |
| 5 — Cart abandon | 454,687 | 46.38% | — |
| 6 — Purchase | 655,222 | 66.83% | 33.17% |
| 7 — Repeat buy | 270,374 | 41.26% | 58.74% |

---

## Real-World Obstacles

| Obstacle | What Happened | How Resolved |
|---|---|---|
| Excel truncation | Oct file silently cut from 42M to 1M rows | Re-downloaded, never opened in Excel |
| Structural mismatch | Oct had 11 columns, Nov had 9 | Separate staging tables, then unified |
| Permission denied | PostgreSQL server couldn't read OneDrive files | Used psql \COPY client-side command |
| Query timeout | Funnel CTE ran 48+ minutes using IN (SELECT) | Rewrote with pre-aggregated CTEs — 6 mins |
| DST timezone split | Two UTC offsets across Oct/Nov data | Normalised all to UTC before analysis |
| Cart event flooding | 78 identical cart events in one session | ROW_NUMBER() deduplication with ctid |

---

## 5 Business Recommendations

1. **Fix the view-to-cart gap** — 80.69% drop-off, 4,095,942 users. Improve product discovery and recommendations — not pricing.
2. **Recover cart abandoners** — 454,687 users, $94M potential. Price is NOT the barrier. Focus on re-engagement and session continuity.
3. **Invest in weekend campaigns** — Sunday 13.26% vs Friday 7.61% conversion. Shift ad spend toward weekends.
4. **Protect top 10% of buyers** — 65,524 users generate $157M (49.83% of revenue). Loyalty programme ROI is exceptional.
5. **Feature Apple products prominently** — $83.56 vs Samsung $49.42 revenue per viewer. Apple browsers are 69% more valuable per visit.

---

## How to Run This Project

### Prerequisites
- PostgreSQL 18.3+
- pgAdmin 4
- ~20GB free disk space for the database

### Setup

1. Download the dataset from Kaggle (see `data/raw/DATA_SOURCE.md`)
2. Create a database called `sales_funnel` in pgAdmin
3. Run scripts in order:

```bash
# In pgAdmin Query Tool — run each file in order
schema/01_create_tables.sql
schema/02_load_data.sql          # follow CMD instructions inside the file

preprocessing/03_null_handling.sql
preprocessing/04_deduplication.sql
preprocessing/05_date_normalisation.sql
preprocessing/06_outlier_detection.sql
preprocessing/07_master_clean.sql

analysis/08_funnel_conversion.sql
analysis/09_dropoff_analysis.sql
analysis/10_traffic_source.sql
analysis/11_week_over_week.sql
analysis/12_roi_by_channel.sql

reports/13_final_summary.sql
```

> **Note:** Loading 109M rows takes approximately 20–30 minutes.
> Preprocessing queries take 10–60 minutes each depending on hardware.
> Add indexes in `07_master_clean.sql` before running analysis queries.

---

## Dashboard

### Standalone HTML Dashboard
Open `tableau/sales_funnel_dashboard_final.html` in any browser — no internet connection required. Includes dark/light toggle, 3 dashboard pages, 8 interactive charts and full narrative text.

### Tableau Public Dashboards

| Dashboard | Link |
|---|---|
| Overview — funnel drop-off, KPI tiles, preprocessing log | [View on Tableau Public](https://public.tableau.com/app/profile/anirudh.raghavendra/viz/SalesFunnel1_17772564713410/Overview) |
| Performance — weekly trends, category scatter, brand efficiency | [View on Tableau Public](https://public.tableau.com/app/profile/anirudh.raghavendra/viz/SalesFunnel2/Performance) |
| Revenue & Insights — price tiers, Pareto, recommendations | [View on Tableau Public](https://public.tableau.com/app/profile/anirudh.raghavendra/viz/SalesFunnel3/RevenueInsights) |

---

*Built as a portfolio data analytics project · PostgreSQL 18.3 · Pure SQL · No Python*
