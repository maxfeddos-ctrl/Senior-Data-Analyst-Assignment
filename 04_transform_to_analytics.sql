-- Create final analytics tables with window functions and aggregations
-- This is what Tableau will connect to
-- Run after staging layer is done

DROP TABLE IF EXISTS analytics_user_activity_metrics;
DROP TABLE IF EXISTS analytics_app_productivity;
DROP TABLE IF EXISTS analytics_retention_cohorts;

-- Main user metrics table - has everything we need for the dashboard
CREATE TABLE analytics_user_activity_metrics AS
SELECT 
    d.date,
    d.user_id,
    d.email,
    d.first_name || ' ' || d.last_name AS full_name,
    d.role,
    d.department,
    d.account_id,
    d.company_name,
    d.industry,
    d.subscription_tier,
    
    -- session counts
    d.total_sessions,
    d.valid_sessions,
    d.unique_apps_used,
    
    -- time metrics
    d.total_hours,
    d.productive_hours,
    d.neutral_hours,
    d.unproductive_hours,
    d.active_hours,
    d.idle_hours,
    
    -- activity
    d.total_keystrokes,
    d.total_clicks,
    d.avg_activity_pct,
    d.idle_ratio_pct,
    d.productivity_pct,
    
    -- flags
    d.is_active_day,
    d.is_high_idle_day,
    
    -- WoW comparison using LAG
    LAG(d.total_hours, 7) OVER (PARTITION BY d.user_id ORDER BY d.date) AS total_hours_last_week,
    ROUND(CASE 
        WHEN LAG(d.total_hours, 7) OVER (PARTITION BY d.user_id ORDER BY d.date) > 0
        THEN ((d.total_hours - LAG(d.total_hours, 7) OVER (PARTITION BY d.user_id ORDER BY d.date)) / 
              LAG(d.total_hours, 7) OVER (PARTITION BY d.user_id ORDER BY d.date) * 100)
        ELSE NULL END, 2) AS wow_change_pct,
    
    -- rolling averages
    ROUND(AVG(d.total_hours) OVER (
        PARTITION BY d.user_id ORDER BY d.date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS total_hours_7d_avg,
    ROUND(AVG(d.total_hours) OVER (
        PARTITION BY d.user_id ORDER BY d.date 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS total_hours_30d_avg,
    
    -- percentile ranking within account
    ROUND(PERCENT_RANK() OVER (
        PARTITION BY d.account_id, d.date 
        ORDER BY d.productive_hours), 3) AS productivity_percentile,
    
    -- rank by productive hours
    RANK() OVER (
        PARTITION BY d.account_id, d.date 
        ORDER BY d.productive_hours DESC) AS productivity_rank,
    
    -- active days in last 7
    (SELECT COUNT(DISTINCT d2.date)
     FROM staging_daily_user_metrics d2
     WHERE d2.user_id = d.user_id
       AND d2.date >= DATE(d.date, '-6 days')
       AND d2.date <= d.date
       AND d2.is_active_day = 1) AS active_days_l7d,
    
    -- engagement tier
    CASE 
        WHEN (SELECT COUNT(DISTINCT d2.date) FROM staging_daily_user_metrics d2
              WHERE d2.user_id = d.user_id AND d2.date >= DATE(d.date, '-6 days')
                AND d2.date <= d.date AND d2.is_active_day = 1) >= 5 
        THEN 'Highly Active'
        WHEN (SELECT COUNT(DISTINCT d2.date) FROM staging_daily_user_metrics d2
              WHERE d2.user_id = d.user_id AND d2.date >= DATE(d.date, '-6 days')
                AND d2.date <= d.date AND d2.is_active_day = 1) >= 3 
        THEN 'Moderately Active'
        WHEN (SELECT COUNT(DISTINCT d2.date) FROM staging_daily_user_metrics d2
              WHERE d2.user_id = d.user_id AND d2.date >= DATE(d.date, '-6 days')
                AND d2.date <= d.date AND d2.is_active_day = 1) >= 1 
        THEN 'Low Activity'
        ELSE 'Inactive'
    END AS engagement_tier,
    
    -- performance tier
    CASE 
        WHEN PERCENT_RANK() OVER (PARTITION BY d.account_id, d.date ORDER BY d.productive_hours) >= 0.90 
            THEN 'Top Performer'
        WHEN PERCENT_RANK() OVER (PARTITION BY d.account_id, d.date ORDER BY d.productive_hours) >= 0.70 
            THEN 'Above Average'
        WHEN PERCENT_RANK() OVER (PARTITION BY d.account_id, d.date ORDER BY d.productive_hours) >= 0.30 
            THEN 'Average'
        ELSE 'Below Average'
    END AS performance_tier,
    
    -- active in L7D flag
    CASE WHEN (SELECT COUNT(DISTINCT d2.date) FROM staging_daily_user_metrics d2
               WHERE d2.user_id = d.user_id AND d2.date >= DATE(d.date, '-6 days')
                 AND d2.date <= d.date AND d2.is_active_day = 1) > 0 
        THEN 1 ELSE 0 END AS is_active_l7d
    
FROM staging_daily_user_metrics d
WHERE d.date >= DATE('now', '-90 days');

CREATE INDEX idx_analytics_user_date ON analytics_user_activity_metrics(user_id, date);
CREATE INDEX idx_analytics_date ON analytics_user_activity_metrics(date);
CREATE INDEX idx_analytics_account ON analytics_user_activity_metrics(account_id);

-- App productivity breakdown
CREATE TABLE analytics_app_productivity AS
SELECT 
    s.date,
    s.account_id,
    s.company_name,
    s.app_id,
    s.app_name,
    s.app_category,
    s.productivity_classification,
    
    COUNT(*) AS session_count,
    COUNT(DISTINCT s.user_id) AS unique_users,
    ROUND(SUM(s.duration_hours), 2) AS total_hours,
    ROUND(AVG(s.duration_minutes), 1) AS avg_session_minutes,
    
    -- percent of total time
    ROUND(SUM(s.duration_hours) / 
          SUM(SUM(s.duration_hours)) OVER (PARTITION BY s.account_id, s.date) * 100, 2) AS pct_of_total_time,
    
    -- rankings
    ROW_NUMBER() OVER (
        PARTITION BY s.account_id, s.date, s.productivity_classification 
        ORDER BY SUM(s.duration_hours) DESC) AS rank_in_classification,
    ROW_NUMBER() OVER (
        PARTITION BY s.account_id, s.date 
        ORDER BY SUM(s.duration_hours) DESC) AS overall_rank
    
FROM staging_enriched_sessions s
WHERE s.is_valid_session = 1
  AND s.date >= DATE('now', '-90 days')
GROUP BY s.date, s.account_id, s.company_name, s.app_id, 
         s.app_name, s.app_category, s.productivity_classification;

CREATE INDEX idx_analytics_app_date ON analytics_app_productivity(account_id, date);

-- Retention cohorts - this is the tricky one with self-joins
CREATE TABLE analytics_retention_cohorts AS
WITH user_cohorts AS (
    -- each user's first week
    SELECT 
        user_id,
        account_id,
        DATE(MIN(date), 'weekday 0', '-6 days') AS cohort_week
    FROM staging_daily_user_metrics
    WHERE is_active_day = 1
    GROUP BY user_id, account_id
),
weekly_activity AS (
    -- all weeks user was active
    SELECT DISTINCT
        user_id,
        account_id,
        DATE(date, 'weekday 0', '-6 days') AS activity_week
    FROM staging_daily_user_metrics
    WHERE is_active_day = 1
),
cohort_data AS (
    -- join to get weeks since cohort
    SELECT 
        c.cohort_week,
        c.account_id,
        c.user_id,
        w.activity_week,
        CAST((JULIANDAY(w.activity_week) - JULIANDAY(c.cohort_week)) / 7 AS INTEGER) AS weeks_since_cohort
    FROM user_cohorts c
    INNER JOIN weekly_activity w ON c.user_id = w.user_id AND c.account_id = w.account_id
),
cohort_sizes AS (
    SELECT cohort_week, account_id, COUNT(DISTINCT user_id) AS cohort_size
    FROM cohort_data
    WHERE weeks_since_cohort = 0
    GROUP BY cohort_week, account_id
)
SELECT 
    cd.cohort_week,
    cd.account_id,
    cd.weeks_since_cohort,
    COUNT(DISTINCT cd.user_id) AS retained_users,
    cs.cohort_size,
    ROUND(COUNT(DISTINCT cd.user_id) * 100.0 / cs.cohort_size, 2) AS retention_pct,
    
    -- week over week drop
    ROUND(COUNT(DISTINCT cd.user_id) * 100.0 / cs.cohort_size - 
          LAG(COUNT(DISTINCT cd.user_id) * 100.0 / cs.cohort_size) OVER (
              PARTITION BY cd.cohort_week, cd.account_id ORDER BY cd.weeks_since_cohort), 2) AS retention_drop_pct
    
FROM cohort_data cd
INNER JOIN cohort_sizes cs ON cd.cohort_week = cs.cohort_week AND cd.account_id = cs.account_id
WHERE cd.weeks_since_cohort <= 12
GROUP BY cd.cohort_week, cd.account_id, cd.weeks_since_cohort, cs.cohort_size
ORDER BY cd.cohort_week, cd.account_id, cd.weeks_since_cohort;

CREATE INDEX idx_cohorts_week ON analytics_retention_cohorts(cohort_week);

-- quick check
SELECT 'analytics_user_activity_metrics' AS tbl, COUNT(*) AS rows FROM analytics_user_activity_metrics
UNION ALL
SELECT 'analytics_app_productivity', COUNT(*) FROM analytics_app_productivity
UNION ALL
SELECT 'analytics_retention_cohorts', COUNT(*) FROM analytics_retention_cohorts;

-- done!
