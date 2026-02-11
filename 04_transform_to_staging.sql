-- Transform raw data to staging layer
-- This joins all the dimension tables and does basic cleanup
-- Run this in DBeaver after loading the raw tables

DROP TABLE IF EXISTS staging_enriched_sessions;
DROP TABLE IF EXISTS staging_daily_user_metrics;

-- First pass - enrich sessions with all the dimension data
CREATE TABLE staging_enriched_sessions AS
SELECT 
    -- session stuff
    s.session_id,
    s.user_id,
    s.account_id,
    s.app_id,
    s.project_id,
    s.date,
    
    -- user info
    u.email,
    u.first_name,
    u.last_name,
    u.role,
    u.department,
    u.status AS user_status,
    
    -- account info
    a.company_name,
    a.industry,
    a.subscription_tier,
    a.company_size,
    
    -- app info
    app.app_name,
    app.category AS app_category,
    app.productivity_classification,
    
    -- project info (can be null)
    p.project_name,
    p.status AS project_status,
    p.budgeted_hours AS project_budget_hours,
    
    -- time metrics
    s.start_timestamp,
    DATETIME(s.start_timestamp, '+' || s.duration_minutes || ' minutes') AS end_timestamp,
    s.duration_minutes,
    ROUND(s.duration_minutes / 60.0, 2) AS duration_hours,
    
    -- activity
    s.keyboard_strokes,
    s.mouse_clicks,
    s.activity_percentage,
    
    -- calculated fields
    ROUND(s.duration_minutes / 60.0 * (s.activity_percentage / 100.0), 2) AS active_hours,
    ROUND(s.duration_minutes / 60.0 * (1 - s.activity_percentage / 100.0), 2) AS idle_hours,
    ROUND((1 - s.activity_percentage / 100.0) * 100, 2) AS idle_ratio_pct,
    
    -- data quality flags
    CASE WHEN DATE(s.start_timestamp) > s.date THEN 1 ELSE 0 END AS is_late_arrival,
    CASE WHEN s.duration_minutes < 1 THEN 1 ELSE 0 END AS is_too_short,
    CASE WHEN s.duration_minutes > 480 THEN 1 ELSE 0 END AS is_too_long,
    CASE 
        WHEN s.duration_minutes >= 1 
         AND s.duration_minutes <= 480 
         AND DATE(s.start_timestamp) <= s.date
        THEN 1 ELSE 0 
    END AS is_valid_session,
    CASE WHEN (1 - s.activity_percentage / 100.0) > 0.30 THEN 1 ELSE 0 END AS is_high_idle,
    CASE WHEN CAST(STRFTIME('%w', s.date) AS INTEGER) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend
    
FROM raw_fact_activity_sessions s
LEFT JOIN raw_dim_users u ON s.user_id = u.user_id
LEFT JOIN raw_dim_accounts a ON s.account_id = a.account_id
LEFT JOIN raw_dim_applications app ON s.app_id = app.app_id
LEFT JOIN raw_dim_projects p ON s.project_id = p.project_id;

-- index the important stuff
CREATE INDEX idx_staging_sessions_user_date ON staging_enriched_sessions(user_id, date);
CREATE INDEX idx_staging_sessions_account_date ON staging_enriched_sessions(account_id, date);
CREATE INDEX idx_staging_sessions_valid ON staging_enriched_sessions(is_valid_session);

-- now aggregate to daily user level
CREATE TABLE staging_daily_user_metrics AS
SELECT 
    date,
    user_id,
    email,
    first_name,
    last_name,
    role,
    department,
    account_id,
    company_name,
    industry,
    subscription_tier,
    
    -- counts
    COUNT(*) AS total_sessions,
    SUM(is_valid_session) AS valid_sessions,
    COUNT(DISTINCT app_id) AS unique_apps_used,
    COUNT(DISTINCT CASE WHEN project_id IS NOT NULL THEN project_id END) AS unique_projects,
    
    -- time breakdowns (only valid sessions)
    ROUND(SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END), 2) AS total_hours,
    ROUND(SUM(CASE WHEN is_valid_session = 1 AND productivity_classification = 'productive' 
        THEN duration_hours ELSE 0 END), 2) AS productive_hours,
    ROUND(SUM(CASE WHEN is_valid_session = 1 AND productivity_classification = 'neutral' 
        THEN duration_hours ELSE 0 END), 2) AS neutral_hours,
    ROUND(SUM(CASE WHEN is_valid_session = 1 AND productivity_classification = 'unproductive' 
        THEN duration_hours ELSE 0 END), 2) AS unproductive_hours,
    ROUND(SUM(CASE WHEN is_valid_session = 1 THEN active_hours ELSE 0 END), 2) AS active_hours,
    ROUND(SUM(CASE WHEN is_valid_session = 1 THEN idle_hours ELSE 0 END), 2) AS idle_hours,
    
    -- activity
    SUM(CASE WHEN is_valid_session = 1 THEN keyboard_strokes ELSE 0 END) AS total_keystrokes,
    SUM(CASE WHEN is_valid_session = 1 THEN mouse_clicks ELSE 0 END) AS total_clicks,
    ROUND(AVG(CASE WHEN is_valid_session = 1 THEN activity_percentage END), 2) AS avg_activity_pct,
    
    -- ratios
    ROUND(CASE 
        WHEN SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END) > 0 
        THEN SUM(CASE WHEN is_valid_session = 1 THEN idle_hours ELSE 0 END) / 
             SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END) * 100
        ELSE 0 END, 2) AS idle_ratio_pct,
        
    ROUND(CASE 
        WHEN SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END) > 0 
        THEN SUM(CASE WHEN is_valid_session = 1 AND productivity_classification = 'productive' 
            THEN duration_hours ELSE 0 END) / 
             SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END) * 100
        ELSE 0 END, 2) AS productivity_pct,
    
    -- flags
    CASE WHEN SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END) > 0 
        THEN 1 ELSE 0 END AS is_active_day,
    CASE WHEN SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END) > 0 
         AND (SUM(CASE WHEN is_valid_session = 1 THEN idle_hours ELSE 0 END) / 
              SUM(CASE WHEN is_valid_session = 1 THEN duration_hours ELSE 0 END)) > 0.30
        THEN 1 ELSE 0 END AS is_high_idle_day
    
FROM staging_enriched_sessions
GROUP BY date, user_id, email, first_name, last_name, role, department,
         account_id, company_name, industry, subscription_tier;

CREATE INDEX idx_daily_metrics_user_date ON staging_daily_user_metrics(user_id, date);
CREATE INDEX idx_daily_metrics_date ON staging_daily_user_metrics(date);

-- check what we got
SELECT 
    'staging_enriched_sessions' AS table_name,
    COUNT(*) AS rows,
    SUM(is_valid_session) AS valid_rows
FROM staging_enriched_sessions

UNION ALL

SELECT 
    'staging_daily_user_metrics',
    COUNT(*),
    SUM(is_active_day)
FROM staging_daily_user_metrics;
