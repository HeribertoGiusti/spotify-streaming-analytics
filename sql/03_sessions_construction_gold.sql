-- ================================================================
-- FILE: 03_sessions_construction.sql
-- PURPOSE: Construct listening sessions from individual plays
-- CREATES: Table `spotify_analytics.sessions`
-- ================================================================
-- BUSINESS LOGIC:
--   Session = continuous listening period with gaps <30 minutes
--   Based on EDA findings: 95% of plays occur within 30-min windows
--
-- METHODOLOGY:
--   1. Calculate time gap between consecutive plays using LAG()
--   2. Flag new session starts (gap >30 min or first play)
--   3. Assign unique session_id using cumulative SUM()
--   4. Aggregate plays into session-level metrics
--
-- KEY METRICS CALCULATED:
--   - session_duration_seconds: Total time from first to last play
--   - tracks_played: Number of tracks in session
--   - unique_artists: Artist variety within session
--   - total_minutes_played: Actual listening time (sum of durations)
--   - shuffle_usage_pct: % of plays with shuffle on
--   - skip_rate: % of plays skipped
--
-- ADVANCED SQL TECHNIQUES DEMONSTRATED:
--   - LAG() window function for time series analysis
--   - Cumulative SUM() with UNBOUNDED PRECEDING for ID generation
--   - DATETIME_DIFF() for temporal calculations
--   - APPROX_QUANTILES() for choosing right thresholds
--   - Multiple CTEs for complex logic breakdown
--   - QUALIFY clause for window function filtering
--
-- TABLEAU USAGE:
--   - Base table for: Network graph, engagement analysis, temporal patterns
--   - Enables session-level filtering in dashboards
--
-- DEPENDENCIES:
--   Input: `spotify_analytics.streaming_history_clean`
--   Output: `spotify_analytics.sessions`
--
-- LAST UPDATED: 2026-03-01
-- ================================================================

-- Create sessions table
CREATE OR REPLACE TABLE `robotic-door-487416-b4.spotify_analytics.sessions` AS

-- ============================================================
-- CTE 1: Calculates time gaps between consecutive plays
-- ============================================================
WITH time_gaps AS (
  SELECT
   *,
   DATETIME_DIFF(
     play_start_MST,
     LAG(play_end_MST) OVER (ORDER BY play_start_MST, play_end_MST, track_uri),
     SECOND) AS seconds_since_last_play
  FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
),   

-- ============================================================
-- CTE 2: Identifies session boundaries
-- ============================================================
session_starts AS (
  SELECT
    *,
    CASE
      WHEN seconds_since_last_play IS NULL THEN 1  -- First play EVER
      WHEN seconds_since_last_play > 1800 THEN 1  -- New session threshold based on EDA findings
      WHEN seconds_since_last_play < -1800 THEN 1  -- To handle severe overlap cases due to API bug (Identified in /docs/problems_session_construction.md)
      ELSE 0
    END AS new_session_flag
  FROM time_gaps
),   

-- ============================================================
-- CTE 3: Assigns unique session IDs
-- ============================================================
plays_with_session_id AS (
  SELECT
    *,
    SUM(new_session_flag) OVER(
      ORDER BY play_start_MST, play_end_MST, track_uri
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id
  FROM session_starts
),

-- ============================================================
-- CTE 4: Calculates session-level aggregations
-- ============================================================
session_aggregations AS (
  SELECT
    session_id,

    -- Temporal boundaries
    MIN(play_start_MST) AS session_start,
    MAX(play_end_MST) AS session_end,
    DATETIME_DIFF(MAX(play_end_MST), MIN(play_start_MST), SECOND) AS session_duration_seconds,
    ROUND(DATETIME_DIFF(MAX(play_end_MST), MIN(play_start_MST), SECOND) / 60.0, 2) AS session_duration_minutes,

    -- Play counts
    COUNT(*) AS tracks_played,
    COUNT(DISTINCT artist_name) AS unique_artists,
    COUNT(DISTINCT album_name) AS unique_albums,
    COUNT(DISTINCT device) AS devices_used,

    -- Actual listening time
    ROUND(SUM(minutes_played), 2) AS total_minutes_played,
    ROUND(AVG(minutes_played), 2) AS avg_track_duration,

    -- Behavioral metrics
    ROUND(COUNTIF(shuffle_flag = TRUE) * 100.0 / COUNT(*), 2) AS shuffle_usage_pct,
    ROUND(COUNTIF(skipped_flag = TRUE) * 100.0 / COUNT(*), 2) AS skip_rate,
    ROUND(COUNTIF(completion_category = 'Complete (>2.5m)') * 100.0 / COUNT(*),
      2) AS completion_rate,
    COUNTIF(offline_flag = TRUE) AS offline_plays,
    ROUND(COUNT(DISTINCT artist_name) * 1.0 / COUNT(*), 2) AS diversity_score,  -- Diversity of artists
    ROUND((COUNT(DISTINCT artist_name) * 1.0 / COUNT(*)) * LEAST(COUNT(*) / 10.0, 1.0), 2) AS weighted_diversity_score,  -- Penalizes short sessions

    -- Most used device in the session
    APPROX_TOP_COUNT(device, 1)[OFFSET(0)].value AS primary_device

  FROM plays_with_session_id
  GROUP BY session_id
),

-- ============================================================
-- CTE 5: Classifies sessions by characteristics
-- ============================================================
session_classification AS (
  SELECT
    *,

    -- Time components
    DATE(session_start) AS session_date,
    EXTRACT(YEAR FROM session_start) AS session_year,
    EXTRACT(MONTH FROM session_start) AS session_month,
    EXTRACT(DAYOFWEEK FROM session_start) AS session_day_of_week,
    EXTRACT(HOUR FROM session_start) AS session_start_hour,

    -- Session category by length
    CASE
      WHEN session_duration_minutes < 2.5 THEN 'Single Track (0-2.5 minutes)'
      WHEN session_duration_minutes < 15 THEN 'Short (2.5-15 minutes)'
      WHEN session_duration_minutes < 60 THEN 'Medium (15-60 minutes)'
      WHEN session_duration_minutes < 180 THEN 'Long (1-3 hours)'
      ELSE 'Marathon (>3 hours)'
    END AS session_length_category,

    -- Session category by behavior (The logic for the thresholds is outlined in /docs/problems_session_construction.md)
    CASE
      WHEN skip_rate > 33.33 AND shuffle_usage_pct > 90 THEN 'Active Search'
      WHEN shuffle_usage_pct > 90 THEN 'Discovery Mode'
      WHEN weighted_diversity_score > 0.74 THEN 'Variety Seeker'
      WHEN weighted_diversity_score < 0.20 AND completion_rate > 75 THEN 'Album Listening'
      WHEN weighted_diversity_score < 0.20 THEN 'Deep Dive on Artist'
      ELSE 'Balanced Session'
    END AS session_type,
    
    -- Engagement level
    CASE
      WHEN completion_rate > 80 AND skip_rate < 20 THEN 'High Engagement'
      WHEN completion_rate > 60 THEN 'Medium Engagement'
      ELSE 'Low Engagement'
    END AS engagement_level

  FROM session_aggregations
)

-- ============================================================
-- FINAL SELECT: All session metrics
-- ============================================================
SELECT 
  session_id,
  session_start,
  session_end,
  session_date,
  session_year,
  session_month,
  session_day_of_week,
  session_start_hour,
  
  -- Duration metrics
  session_duration_seconds,
  session_duration_minutes,
  total_minutes_played,
  session_length_category,

  -- Platforms
  primary_device,
  devices_used,

  -- Content metrics
  tracks_played,
  unique_artists,
  unique_albums,
  avg_track_duration,
  
  -- Behavioral metrics
  diversity_score,
  weighted_diversity_score,
  shuffle_usage_pct,
  skip_rate,
  completion_rate,
  offline_plays,
  
  -- Classifications
  session_type,
  engagement_level

FROM session_classification
ORDER BY session_start;

-- ================================================================
-- DATA QUALITY VERIFICATION
-- ================================================================

-- Verify 1: Total sessions created
SELECT 
  'Total Sessions' AS metric,
  COUNT(*) AS value
FROM `robotic-door-487416-b4.spotify_analytics.sessions`;

-- Verify 2: Session distribution by length
SELECT 
  session_length_category,
  COUNT(*) AS session_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
  ROUND(AVG(tracks_played), 1) AS avg_tracks,
  ROUND(AVG(total_minutes_played), 1) AS avg_minutes
FROM `robotic-door-487416-b4.spotify_analytics.sessions`
GROUP BY session_length_category
ORDER BY 
  CASE session_length_category
    WHEN 'Single Track' THEN 1
    WHEN 'Short (0-15 min)' THEN 2
    WHEN 'Medium (15-60 min)' THEN 3
    WHEN 'Long (1-3 hours)' THEN 4
    ELSE 5
  END;

-- Verify 3: Session types distribution
SELECT 
  session_type,
  COUNT(*) AS session_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
  ROUND(AVG(diversity_score), 3) AS avg_diversity,
  ROUND(AVG(skip_rate), 2) AS avg_skip_rate
FROM `robotic-door-487416-b4.spotify_analytics.sessions`
GROUP BY session_type
ORDER BY session_count DESC;

-- Verify 4: Engagement levels
SELECT 
  engagement_level,
  COUNT(*) AS session_count,
  ROUND(AVG(completion_rate), 2) AS avg_completion_rate,
  ROUND(AVG(skip_rate), 2) AS avg_skip_rate,
  ROUND(AVG(tracks_played), 1) AS avg_tracks
FROM `robotic-door-487416-b4.spotify_analytics.sessions`
GROUP BY engagement_level
ORDER BY 
  CASE engagement_level
    WHEN 'High Engagement' THEN 1
    WHEN 'Medium Engagement' THEN 2
    ELSE 3
  END;

-- Verify 5: Sessions over time
SELECT 
  session_year,
  session_month,
  COUNT(*) AS sessions,
  ROUND(AVG(tracks_played), 1) AS avg_tracks_per_session,
  ROUND(AVG(session_duration_minutes), 1) AS avg_duration_minutes
FROM `robotic-door-487416-b4.spotify_analytics.sessions`
GROUP BY session_year, session_month
ORDER BY session_year, session_month;

-- Verify 6: Top session characteristics
SELECT * FROM (
  SELECT
    'Longest Session' AS metric,
    session_id,
    session_start,
    tracks_played,
    session_duration_minutes,
    ROUND(session_duration_minutes / 60.0, 1) AS session_duration_hours,
    unique_artists
FROM `robotic-door-487416-b4.spotify_analytics.sessions`
ORDER BY session_duration_minutes DESC
LIMIT 1
)

UNION ALL

SELECT * FROM (
    SELECT
    'Most Tracks in Session',
    session_id,
    session_start,
    tracks_played,
    session_duration_minutes,
    ROUND(session_duration_minutes / 60.0, 1) AS session_duration_hours,
    unique_artists
FROM `robotic-door-487416-b4.spotify_analytics.sessions`
ORDER BY tracks_played DESC
LIMIT 3
)

UNION ALL

SELECT * FROM (
  SELECT
    'Most Diverse Session (weighted)',
    session_id,
    session_start,
    tracks_played,
    session_duration_minutes,
    ROUND(session_duration_minutes / 60.0, 1) AS session_duration_hours,
    unique_artists
FROM `robotic-door-487416-b4.spotify_analytics.sessions`
WHERE tracks_played >= 10
ORDER BY weighted_diversity_score DESC
LIMIT 3
);

-- Verify 7: Validates no orphaned plays (all plays assigned to a session)
SELECT 
  'Play Count Validation' AS check_type,
  (SELECT COUNT(*) FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`) AS total_plays_in_clean_table,
  (SELECT SUM(tracks_played) FROM `robotic-door-487416-b4.spotify_analytics.sessions`) AS total_plays_in_sessions,
  (SELECT COUNT(*) FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`) - 
  (SELECT SUM(tracks_played) FROM `robotic-door-487416-b4.spotify_analytics.sessions`) AS difference
;