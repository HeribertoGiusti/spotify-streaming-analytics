-- ================================================================
-- FILE: 02_data_cleaning_silver.sql
-- PURPOSE: Create clean "silver" layer table
-- CREATES: Table `spotify_analytics.streaming_history_clean`
-- ================================================================
-- SCOPE: Analysis focuses exclusively on music listening patterns, since music plays constitute the vast majority of plays. 
--
-- BUSINESS RATIONALE:
--   EDA identified duplicate records that could distort analytics, alongside other anomalies. These are here removed.
--   Also, "defensive programming" conditions were applied on the first WHERE clause because, even though EDA showed no current problem, there could be one if the project scales and new data is added.
--   This silver table ensures data quality for any downstream analysis.
--
-- DEDUPLICATION LOGIC:
--   Removes exact duplicates based on:
--   - Same ts, same track URI and same play duration
--   - Keeps first occurrence based on row partition order
--   - Using URI instead of track name allows for handling for tracks with identical names by different artists, or different versions with same title
--
-- ADDITIONAL CLEANING:
--   - Focuses on music by standardizing NULL handling
--   - Filters out impossible time values
--   - Excludes December 2020 anomaly: >24 hours/day
--   - Omits tracks with immediate skip because of 'ts' register bug they create
--   - Leaves out tracks that display a severe overlap in their ending times in the same device
--
-- DEPENDENCIES:
--   Input: `spotify_analytics.streaming_history_raw`
--   Output: `spotify_analytics.streaming_history_clean`
--
-- LAST UPDATED: 2025-03-01
-- ================================================================

-- Creates clean silver table for music only
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
    AND ms_played >= 1000  -- Leaves out immediate skips (ts register bug identified in /docs/problems_session_construction.md)
    AND DATE(ts) >= '2013-09-23'  -- First ever listening session in UTC

    -- Exclude December 2020 anomalies identified in EDA
    AND NOT (DATE(ts) BETWEEN '2020-12-07' AND '2020-12-08')
),

deduplicated AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        ts,                    -- Exact ending timestamp
        spotify_track_uri,     -- Unique track identifier (most precise)
        ms_played              -- Distinguish replays with different duration
      ORDER BY ts
    ) AS row_num
  FROM music_only
)

SELECT 
  -- Time information
  DATETIME_TRUNC(DATETIME_SUB(DATETIME(ts,'America/Mazatlan'), INTERVAL ms_played MILLISECOND), SECOND) as play_start_MST,  -- This is because I've spent most of my time in -7 MST
  DATETIME(ts, 'America/Mazatlan') AS play_end_MST,
  ms_played,

  -- Music metadata
  master_metadata_track_name AS track_name,
  master_metadata_album_artist_name AS artist_name,
  master_metadata_album_album_name AS album_name,
  spotify_track_uri AS track_uri,

  -- Device
  platform AS device,
  
  -- Playback context
  reason_start,
  reason_end,
  shuffle AS shuffle_flag,
  skipped AS skipped_flag,
  offline AS offline_flag,
  offline_timestamp AS offline_ts_UTC,
  incognito_mode AS incognito_flag,
  
  -- Derived temporal fields
  DATE(ts, 'America/Mazatlan') AS play_date,
  EXTRACT(YEAR FROM ts AT TIME ZONE 'America/Mazatlan') AS play_year,
  EXTRACT(MONTH FROM ts AT TIME ZONE 'America/Mazatlan') AS play_month,
  FORMAT_DATE('%A', DATE(ts, 'America/Mazatlan')) AS play_day_week,
  EXTRACT(HOUR FROM ts AT TIME ZONE 'America/Mazatlan') AS play_hour,
  -- Time of day classification
  CASE 
    WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/Mazatlan') BETWEEN 0 AND 5 THEN 'Night'
    WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/Mazatlan') BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM ts AT TIME ZONE 'America/Mazatlan') BETWEEN 12 AND 17 THEN 'Afternoon'
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
ORDER BY TIMESTAMP_SUB(ts, INTERVAL ms_played MILLISECOND), ts, spotify_track_uri;  -- Opted for milliseconds + URI tiebreaker due to repeated plays in same second (Identified in /docs/problems_session_construction.md)


-- ================================================================
-- DATA QUALITY VERIFICATION
-- ================================================================

-- 1. Verify NO duplicates remain
SELECT 
  'Duplicate Check' AS check_type,
  COUNT(*) - COUNT(DISTINCT CONCAT(
    CAST(play_end_MST AS STRING),
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
  MIN(play_start_MST) AS earliest_play,
  MAX(play_start_MST) AS latest_play,
  DATE_DIFF(DATE(MAX(play_start_MST)), DATE(MIN(play_start_MST)), DAY) AS days_span,
  COUNT(DISTINCT DATE(play_start_MST)) AS active_days
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

-- 7. Records removed
SELECT 
  'Data Cleaning Summary' AS metric,
  (SELECT COUNT(*) FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw` WHERE episode_name IS NULL) AS raw_music_plays,
  COUNT(*) AS clean_plays,
  (SELECT COUNT(*) FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw` WHERE episode_name IS NULL) - COUNT(*) AS plays_removed,
  ROUND(
    ((SELECT COUNT(*) FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw` WHERE episode_name IS NULL) - COUNT(*)) * 100.0 /
    (SELECT COUNT(*) FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw` WHERE episode_name IS NULL)
    , 2) AS pct_removed
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`;

-- 8. Remaining overlaps
WITH overlaps AS (
  SELECT 
    play_start_MST,
    device,
    LAG(play_end_MST) OVER (ORDER BY play_start_MST, play_end_MST, track_uri) AS prev_end,
    LAG(device) OVER (ORDER BY play_start_MST, play_end_MST, track_uri) AS prev_device,
    TIMESTAMP_DIFF(
      play_start_MST,
      LAG(play_end_MST) OVER (ORDER BY play_start_MST, play_end_MST, track_uri),
      SECOND
    ) AS gap_seconds
  FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
)
SELECT 
  'Remaining Overlaps Analysis' AS check_type,
  COUNT(*) AS total_transitions,
  COUNTIF(gap_seconds < 0) AS total_overlaps,
  COUNTIF(gap_seconds < 0 AND device != prev_device) AS cross_device_overlaps,
  COUNTIF(gap_seconds < 0 AND device = prev_device) AS same_device_overlaps,
  COUNTIF(gap_seconds < -1800 AND device = prev_device) AS severe_same_device_overlaps,
  ROUND(COUNTIF(gap_seconds < 0) * 100.0 / COUNT(*), 2) AS pct_general_overlap,
  ROUND(COUNTIF(gap_seconds < 0 AND device != prev_device) * 100.0 / COUNT(*), 2) AS pct_cross_overlaps,
  ROUND(COUNTIF(gap_seconds < 0 AND device = prev_device) * 100.0 / COUNT(*), 2) AS pct_same_overlaps,
  ROUND(COUNTIF(gap_seconds < -1800 AND device = prev_device) * 100.0 / COUNT(*), 2) AS pct_severe_same_overlaps
FROM overlaps
WHERE prev_end IS NOT NULL;

-- 9. Specific examples
WITH overlaps AS (
  SELECT 
    play_start_MST,
    play_end_MST,
    device,
    track_name,
    LAG(play_start_MST) OVER (ORDER BY play_start_MST) AS prev_start,
    LAG(play_end_MST) OVER (ORDER BY play_start_MST) AS prev_end,
    LAG(device) OVER (ORDER BY play_start_MST) AS prev_device,
    LAG(track_name) OVER (ORDER BY play_start_MST) AS prev_track,
    TIMESTAMP_DIFF(
      play_start_MST,
      LAG(play_end_MST) OVER (ORDER BY play_start_MST),
      SECOND
    ) AS gap_seconds
  FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
)
SELECT *
FROM overlaps
WHERE gap_seconds < -1800 
  AND device = prev_device
ORDER BY gap_seconds ASC
LIMIT 20;

-- 10. Top devices after cleaning
SELECT 
  device,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY device
ORDER BY plays DESC
LIMIT 10;