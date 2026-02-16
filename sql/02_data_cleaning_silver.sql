-- ================================================================
-- FILE: 02_data_cleaning_silver.sql
-- PURPOSE: Create clean "silver" layer table
-- CREATES: Table `spotify_analytics.streaming_history_clean`
-- ================================================================
-- SCOPE: Analysis focuses exclusively on music listening patterns, since music plays constitute the vast majority of plays. 
--
-- BUSINESS RATIONALE:
--   EDA identified duplicate records that could distort analytics, alongside other anomalies. These are here removed.
--   Also, "defensive programming" conditions were applied on the WHERE clause because, even though EDA showed no current problem, there could be one if the project scales and new data is added.
--   This silver table ensures data quality for any downstream analysis.
--
-- DEDUPLICATION LOGIC:
--   Removes exact duplicates based on:
--   - Same ts
--   - Same track URI
--   - Same play duration
--   - Keeps first occurrence based on row partition order
--   - Using URI instead of track name allows for handling for tracks with identical names by different artists, or different versions with same title
--
-- ADDITIONAL CLEANING:
--   - Excludes December 2020 anomalies (>24 hours/day)
--   - Filters out impossible values
--   - Standardizes NULL handling
--
-- DEPENDENCIES:
--   Input: `spotify_analytics.streaming_history_raw`
--   Output: `spotify_analytics.streaming_history_clean`
--
-- LAST UPDATED: 2025-02-15
-- ================================================================

-- Create clean silver table for music only
CREATE OR REPLACE TABLE `robotic-door-487416-b4.spotify_analytics.streaming_history_clean` AS

WITH music_only AS (
  SELECT *
  FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
  WHERE 1=1
    -- Music only filters
    AND episode_name IS NULL
    AND episode_show_name IS NULL
    AND master_metadata_track_name IS NOT NULL  -- Must have track name
    AND spotify_track_uri IS NOT NULL  -- Must have track URI
    
    -- Data quality filters
    AND ts IS NOT NULL
    AND ms_played > 0
    AND DATE(ts) >= '2013-09-23'  -- First ever listening session in UTC  
    
    -- Exclude December 2020 anomalies identified in EDA
    AND NOT (DATE(ts) BETWEEN '2020-12-07' AND '2020-12-08')
),

deduplicated AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        ts,                    -- Exact timestamp
        spotify_track_uri,     -- Unique track identifier (most precise)
        ms_played              -- Distinguish replays with different duration
      ORDER BY ts
    ) AS row_num
  FROM music_only
)

SELECT 
  -- Core fields for final silver table
  DATETIME(ts, "UTC-7") AS dt_MST,  -- This is because I've spent most of my time in -7 MST
  ms_played,
  
  -- Music metadata
  master_metadata_track_name AS track_name,
  master_metadata_album_artist_name AS artist_name,
  master_metadata_album_album_name AS album_name,
  spotify_track_uri AS track_uri,
  
  -- Playback context
  reason_start,
  reason_end,
  shuffle AS shuffle_flag,
  skipped AS skipped_flag,
  offline AS offline_flag,
  offline_timestamp AS offline_ts_UTC,
  incognito_mode AS incognito_flag,
  
  -- Derived temporal fields
  DATE(DATETIME(ts, "UTC-7")) AS play_date,
  EXTRACT(YEAR FROM DATETIME(ts, "UTC-7")) AS play_year,
  EXTRACT(MONTH FROM DATETIME(ts, "UTC-7")) AS play_month,
  EXTRACT(DAYOFWEEK FROM DATETIME(ts, "UTC-7")) AS play_day_week,
  EXTRACT(HOUR FROM DATETIME(ts, "UTC-7")) AS play_hour,
  
  -- Time of day classification
  CASE 
    WHEN EXTRACT(HOUR FROM DATETIME(ts, "UTC-7")) BETWEEN 0 AND 5 THEN 'Night'
    WHEN EXTRACT(HOUR FROM DATETIME(ts, "UTC-7")) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM DATETIME(ts, "UTC-7")) BETWEEN 12 AND 17 THEN 'Afternoon'
    ELSE 'Evening'
  END AS time_of_day,
  
  -- Duration metrics
  ROUND(ms_played / 1000.0, 2) AS seconds_played,
  ROUND(ms_played / 60000.0, 2) AS minutes_played,
  ROUND(ms_played / 3600000.0, 2) AS hours_played,
  
  -- Engagement metrics (helpful for later analysis)
  CASE 
    WHEN ms_played < 30000 THEN 'Skipped (<30s)'
    WHEN ms_played < 150000 THEN 'Partial (30s-2.5m)'
    ELSE 'Complete (>2.5m)'
  END AS completion_category

FROM deduplicated
WHERE row_num = 1  -- Keeps only first occurrence of each duplicate group
ORDER BY dt_MST;


-- ================================================================
-- DATA QUALITY VERIFICATION
-- ================================================================

-- 1. Verify NO duplicates remain
SELECT 
  'Duplicate Check' AS check_type,
  COUNT(*) - COUNT(DISTINCT CONCAT(
    CAST(dt_MST AS STRING),
    track_uri,
    CAST(ms_played AS STRING)
  )) AS remaining_duplicates
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`;

-- 2. Verify NO podcasts leaked through
SELECT 
  'Podcast Leakage Check' AS check_type,
  COUNT(*) AS podcast_count
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
WHERE track_name IS NULL 
   OR track_uri IS NULL;

-- 3. Date range verification
SELECT 
  'Date Range' AS metric,
  MIN(dt_MST) AS earliest_play,
  MAX(dt_MST) AS latest_play,
  DATE_DIFF(DATE(MAX(dt_MST)), DATE(MIN(dt_MST)), DAY) AS days_span,
  COUNT(DISTINCT DATE(dt_MST)) AS active_days
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`;

-- 4. Top 10 artists (as sanity check)
SELECT 
  'Top 10 Artists' AS section,
  artist_name,
  COUNT(*) AS plays,
  ROUND(SUM(minutes_played), 1) AS total_minutes,
  ROUND(AVG(minutes_played), 2) AS avg_minutes_per_play
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY artist_name
ORDER BY plays DESC
LIMIT 10;

-- 5. Completion rate distribution
SELECT 
  completion_category,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY completion_category
ORDER BY 
  CASE completion_category
    WHEN 'Skip (<30s)' THEN 1
    WHEN 'Partial (30s-1.5m)' THEN 2
    WHEN 'Most (1.5-3m)' THEN 3
    ELSE 4
  END;

-- 6. Temporal distribution check
SELECT 
  time_of_day,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY time_of_day
ORDER BY 
  CASE time_of_day
    WHEN 'Night' THEN 1
    WHEN 'Morning' THEN 2
    WHEN 'Afternoon' THEN 3
    ELSE 4
  END;