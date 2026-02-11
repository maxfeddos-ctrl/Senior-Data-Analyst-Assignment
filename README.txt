### Files & Execution Order

1. Python files for Data Generation & Loading

01_generate_raw_data.py - Creates 6 months of synthetic data
02_load_to_sqlite.py - Loads CSVs into SQLite database

2. SQL Queries for transformations to Stagingn and Analytics

04_transform_to_staging.sql - Raw → Staging (cleaning, enrichment)
05_transform_to_analytics.sql - Staging → Analytics (window functions, CTEs)

Requirements.txt - Python dependencies



### Database Schema: Three-Layer Architecture

```
timedoctor_analytics.db
│
├── RAW LAYER (6 tables)
│   ├── raw_dim_accounts
│   ├── raw_dim_users
│   ├── raw_dim_applications
│   ├── raw_dim_projects
│   ├── raw_dim_tasks
│   └── raw_fact_activity_sessions
│
├── STAGING LAYER (2 tables)
│   ├── staging_enriched_sessions
│   └── staging_daily_user_metrics
│
└── ANALYTICS LAYER (4 tables)
    ├── analytics_user_activity_metrics    ← For Tableau
    ├── analytics_app_productivity         ← For Tableau
    ├── analytics_retention_cohorts        ← For Tableau
    └── analytics_project_allocation       ← For Tableau



### Core Metrics

Engagement

- Active Users (L7D) - The number of unique users who have been active at least once in the last 7 days.

- Engagement Tier Distribution - A breakdown of users grouped by how frequently they’ve been active in the past 7 days (e.g., Highly Active, Moderately Active, Low Activity, Inactive).

Retention Metric

- Weekly Retention Rate - The percentage of users who return and remain active in the weeks following their first week of activity.

- Account Churn Risk

Productivity Metrics

- Avg Productive Hours per User - The average number of hours users spend doing productive work.

- Utilization Rate - The percentage of available working hours that were actually used.