-- ================================================================
-- FILE: 01_exploratory_analysis.sql
-- PURPOSE: Exploratory Data Analysis (EDA) on Spotify streaming history
-- OUTPUT: Queries for analysis only, not creating tables
-- ================================================================
-- This file contains queries to understand:
--   - Data quality and completeness
--   - Temporal coverage and distribution
--   - Content variety (artists, tracks, podcasts)
--   - Platform and usage patterns
--   - Behavioral metrics (skips, shuffle, offline)
-- ================================================================

-- ================================================================
-- SECTION 1: DATA OVERVIEW & QUALITY
-- ================================================================

-- 1.1 Total records and date range
SELECT 
  COUNT(*) AS total_records,
  TIMESTAMP_SUB(MIN(ts), INTERVAL 7 HOUR) AS earliest_play, 
  TIMESTAMP_SUB(MAX(ts), INTERVAL 7 HOUR) AS latest_play,
  DATE_DIFF(DATE(MAX(ts)), DATE(MIN(ts)), DAY) AS days_of_data,
  ROUND(COUNT(*) / DATE_DIFF(DATE(MAX(ts)), DATE(MIN(ts)), DAY), 1) AS avg_plays_per_day
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`;

-- 1.2 Null value analysis by column
SELECT 
  COUNTIF(incognito_mode IS NULL) AS null_incognito,
  COUNTIF(offline IS NULL) AS null_offline,
  COUNTIF(shuffle IS NULL) AS null_shuffle,
  COUNTIF(reason_start IS NULL) AS null_reason_start,
  COUNTIF(audiobook_chapter_title IS NULL) AS null_audiobook_title,
  COUNTIF(skipped IS NULL) AS null_skipped,
  COUNTIF(audiobook_chapter_uri IS NULL) AS null_audiobook_uri,
  COUNTIF(ip_addr IS NULL) AS null_ip_addr,
  COUNTIF(episode_show_name IS NULL) AS null_show_name,
  COUNTIF(episode_name IS NULL) AS null_episode_name,
  COUNTIF(offline_timestamp IS NULL) AS null_offline_timestamp,
  COUNTIF(reason_end IS NULL) AS null_reason_end,
  COUNTIF(spotify_episode_uri IS NULL) AS null_episode_uri,
  COUNTIF(spotify_track_uri IS NULL) AS null_track_uri,
  COUNTIF(audiobook_uri IS NULL) AS null_audiobook_uri,
  COUNTIF(master_metadata_album_album_name IS NULL) AS null_album_name,
  COUNTIF(master_metadata_album_artist_name IS NULL) AS null_artist_name,
  COUNTIF(master_metadata_track_name IS NULL) AS null_track_name,
  COUNTIF(ts IS NULL) AS null_ts,
  COUNTIF(conn_country IS NULL) AS null_country,
  COUNTIF(ms_played IS NULL) AS null_ms_played,
  COUNTIF(audiobook_title IS NULL) AS null_audiobook_title,
  COUNTIF(platform IS NULL) AS null_platform, 
  -- Calculate percentages
  ROUND(COUNTIF(master_metadata_track_name IS NULL) * 100.0 / COUNT(*), 2) AS pct_null_track_name,
  ROUND(COUNTIF(episode_name IS NULL) * 100.0 / COUNT(*), 2) AS pct_null_episode_name
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`;

-- 1.3 Content type distribution (Music vs Podcasts)
SELECT 
  CASE 
    WHEN episode_name IS NOT NULL THEN 'Podcast'
    WHEN master_metadata_track_name IS NOT NULL THEN 'Music'
    ELSE 'Unknown'
  END AS content_type,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
  ROUND(SUM(ms_played) / 60000, 0) AS total_minutes,
  ROUND(AVG(ms_played) / 60000, 2) AS avg_minutes_per_play
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
GROUP BY content_type
ORDER BY plays DESC;

-- 1.4 Duplicate detection
SELECT 
  'Exact duplicates (all fields)' AS check_type,
  COUNT(*) - COUNT(DISTINCT CONCAT(
    CAST(ts AS STRING), 
    master_metadata_track_name,
    CAST(ms_played AS STRING)
  )) AS duplicate_count
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
UNION ALL
SELECT 
  'Same track played at exact same timestamp',
  COUNT(*) - COUNT(DISTINCT CONCAT(CAST(ts AS STRING), spotify_track_uri))
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`;


-- ================================================================
-- SECTION 2: TEMPORAL PATTERNS
-- ================================================================

-- 2.1 Plays by year and month
SELECT 
  EXTRACT(YEAR FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) AS year,
  EXTRACT(MONTH FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) AS month,
  COUNT(*) AS plays,
  COUNT(DISTINCT DATE(TIMESTAMP_SUB(ts, INTERVAL 7 HOUR))) AS active_days,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT DATE(ts)), 1) AS avg_plays_per_day,
  ROUND(SUM(ms_played) / 60000 / 60, 1) AS total_hours
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
GROUP BY year, month
ORDER BY year, month;

-- 2.2 Plays by day of week
SELECT 
  CASE EXTRACT(DAYOFWEEK FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR))
    WHEN 1 THEN 'Monday'
    WHEN 2 THEN 'Tuesday'
    WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday'
    WHEN 5 THEN 'Friday'
    WHEN 6 THEN 'Saturday'
    WHEN 7 THEN 'Sunday'
  END AS day_of_week,
  EXTRACT(DAYOFWEEK FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) AS day_num,
  COUNT(*) AS plays,
  ROUND(AVG(ms_played) / 60000, 2) AS avg_minutes_per_play
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
GROUP BY day_of_week, day_num
ORDER BY day_num;

-- 2.3 Plays by hour of day
SELECT 
  EXTRACT(HOUR FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) AS hour,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
  ROUND(SUM(ms_played) / 60000, 0) AS total_minutes
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
GROUP BY hour
ORDER BY hour;

-- 2.4 Listening heatmap: Day of week × Hour
SELECT 
  EXTRACT(DAYOFWEEK FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) AS day_num,
  EXTRACT(HOUR FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) AS hour,
  COUNT(*) AS plays
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
GROUP BY day_num, hour
ORDER BY day_num, hour;


-- ================================================================
-- SECTION 3: CONTENT ANALYSIS
-- ================================================================

-- 3.1 Top 20 artists by plays
SELECT 
  master_metadata_album_artist_name AS artist,
  COUNT(*) AS plays,
  COUNT(DISTINCT master_metadata_track_name) AS unique_tracks,
  ROUND(SUM(ms_played) / 60000 / 60, 1) AS total_hours,
  ROUND(AVG(ms_played) / 60000, 2) AS avg_minutes_per_play,
  -- Skip rate
  ROUND(COUNTIF(skipped = TRUE) * 100.0 / COUNT(*), 1) AS skip_rate_pct
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE master_metadata_album_artist_name IS NOT NULL
GROUP BY artist
ORDER BY plays DESC
LIMIT 20;

-- 3.2 Top 20 tracks by plays
SELECT 
  master_metadata_track_name AS track,
  master_metadata_album_artist_name AS artist,
  COUNT(*) AS plays,
  ROUND(AVG(ms_played) / 60000, 2) AS avg_minutes_per_play,
  TIMESTAMP_SUB(MIN(ts), INTERVAL 7 HOUR) AS first_played,
  TIMESTAMP_SUB(MAX(ts), INTERVAL 7 HOUR) AS last_played,
  DATE_DIFF(DATE(MAX(ts)), DATE(MIN(ts)), DAY) AS days_span
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE master_metadata_track_name IS NOT NULL
GROUP BY track, artist
ORDER BY plays DESC
LIMIT 20;

-- 3.3 Artist discovery timeline (first play date)
SELECT 
  master_metadata_album_artist_name AS artist,
  MIN(DATE(TIMESTAMP_SUB(ts, INTERVAL 7 HOUR))) AS first_discovered,
  COUNT(*) AS total_plays,
  COUNT(DISTINCT DATE(TIMESTAMP_SUB(ts, INTERVAL 7 HOUR))) AS days_played
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE master_metadata_album_artist_name IS NOT NULL
GROUP BY artist
ORDER BY first_discovered
LIMIT 20;


-- ================================================================
-- SECTION 4: PLATFORM & DEVICE ANALYSIS
-- ================================================================

-- 4.1 Plays by platform
SELECT 
  platform,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
  ROUND(SUM(ms_played) / 60000 / 60, 1) AS total_hours,
  ROUND(AVG(ms_played) / 60000, 2) AS avg_minutes_per_play
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE platform IS NOT NULL
GROUP BY platform
ORDER BY plays DESC;

-- 4.2 Platform usage by time of day
SELECT 
  platform,
  CASE 
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) BETWEEN 6 AND 11 THEN 'Morning (6-11)'
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) BETWEEN 12 AND 17 THEN 'Afternoon (12-17)'
    WHEN EXTRACT(HOUR FROM TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) BETWEEN 18 AND 23 THEN 'Evening (18-23)'
    ELSE 'Night (0-5)'
  END AS time_of_day,
  COUNT(*) AS plays
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE platform IS NOT NULL
GROUP BY platform, time_of_day
ORDER BY platform, 
  CASE time_of_day
    WHEN 'Morning (6-11)' THEN 1
    WHEN 'Afternoon (12-17)' THEN 2
    WHEN 'Evening (18-23)' THEN 3
    ELSE 4
  END;

-- 4.3 Country distribution
SELECT 
  conn_country AS country,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE conn_country IS NOT NULL
GROUP BY country
ORDER BY plays DESC;


-- ================================================================
-- SECTION 5: BEHAVIORAL METRICS
-- ================================================================

-- 5.1 Play duration distribution
SELECT 
  CASE 
    WHEN ms_played < 30000 THEN '< 30 seconds'
    WHEN ms_played < 60000 THEN '30-60 seconds'
    WHEN ms_played < 120000 THEN '1-2 minutes'
    WHEN ms_played < 180000 THEN '2-3 minutes'
    WHEN ms_played < 300000 THEN '3-5 minutes'
    ELSE '> 5 minutes'
  END AS duration_bucket,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE ms_played > 0
GROUP BY duration_bucket
ORDER BY 
  CASE duration_bucket
    WHEN '< 30 seconds' THEN 1
    WHEN '30-60 seconds' THEN 2
    WHEN '1-2 minutes' THEN 3
    WHEN '2-3 minutes' THEN 4
    WHEN '3-5 minutes' THEN 5
    ELSE 6
  END;

-- 5.2 Skip behavior analysis
SELECT 
  skipped,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
  ROUND(AVG(ms_played) / 60000, 2) AS avg_minutes_per_play
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE skipped IS NOT NULL
GROUP BY skipped;

-- 5.3 Shuffle mode usage
SELECT 
  shuffle,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total,
  COUNT(DISTINCT master_metadata_album_artist_name) AS unique_artists
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE shuffle IS NOT NULL
GROUP BY shuffle;

-- 5.4 Offline vs Online listening
SELECT 
  offline,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE offline IS NOT NULL
GROUP BY offline;

-- 5.5 Incognito mode usage
SELECT 
  incognito_mode,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE incognito_mode IS NOT NULL
GROUP BY incognito_mode;

-- 5.6 Reason for playback start
SELECT 
  reason_start,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE reason_start IS NOT NULL
GROUP BY reason_start
ORDER BY plays DESC;

-- 5.7 Reason for playback end
SELECT 
  reason_end,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE reason_end IS NOT NULL
GROUP BY reason_end
ORDER BY plays DESC;


-- ================================================================
-- SECTION 6: ADVANCED INSIGHTS
-- ================================================================

-- 6.1 Listening intensity by date (to identify binge days)
SELECT 
  DATE(TIMESTAMP_SUB(ts, INTERVAL 7 HOUR)) AS date,
  COUNT(*) AS plays,
  ROUND(SUM(ms_played) / 60000 / 60, 2) AS hours_listened,
  COUNT(DISTINCT master_metadata_album_artist_name) AS unique_artists
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
GROUP BY date
ORDER BY hours_listened DESC
LIMIT 20;

-- 6.2 Artist loyalty score (plays vs unique tracks ratio)
-- Higher ratio = more repetition of same tracks
SELECT 
  master_metadata_album_artist_name AS artist,
  COUNT(*) AS total_plays,
  COUNT(DISTINCT master_metadata_track_name) AS unique_tracks,
  ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT master_metadata_track_name), 2) AS loyalty_score,
  CASE 
    WHEN COUNT(*) * 1.0 / COUNT(DISTINCT master_metadata_track_name) > 5 THEN 'High Repetition'
    WHEN COUNT(*) * 1.0 / COUNT(DISTINCT master_metadata_track_name) > 2 THEN 'Medium Repetition'
    ELSE 'High Variety'
  END AS listening_pattern
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE master_metadata_album_artist_name IS NOT NULL
GROUP BY artist
HAVING COUNT(*) >= 10
ORDER BY loyalty_score DESC
LIMIT 30;

-- 6.3 Session length estimation (time gaps between plays)
WITH gaps AS (
  SELECT 
    ts,
    TIMESTAMP_DIFF(
      ts,
      LAG(ts) OVER (ORDER BY ts),
      MINUTE
    ) AS minutes_since_last
  FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
)
SELECT 
  CASE 
    WHEN minutes_since_last IS NULL THEN 'First play'
    WHEN minutes_since_last <= 5 THEN '0-5 min gap'
    WHEN minutes_since_last <= 15 THEN '5-15 min gap'
    WHEN minutes_since_last <= 30 THEN '15-30 min gap'
    WHEN minutes_since_last <= 60 THEN '30-60 min gap'
    WHEN minutes_since_last <= 180 THEN '1-3 hour gap'
    ELSE '> 3 hour gap'
  END AS gap_bucket,
  COUNT(*) AS occurrences,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM gaps
GROUP BY gap_bucket
ORDER BY 
  CASE gap_bucket
    WHEN 'First play' THEN 0
    WHEN '0-5 min gap' THEN 1
    WHEN '5-15 min gap' THEN 2
    WHEN '15-30 min gap' THEN 3
    WHEN '30-60 min gap' THEN 4
    WHEN '1-3 hour gap' THEN 5
    ELSE 6
  END;

-- 6.4 Track completion rate estimate
-- Assumes average song is 3 to 4 minutes (180,000 to 240,000 ms)
SELECT 
  CASE 
    WHEN ms_played < 30000 THEN 'Skipped immediately (<30s)'
    WHEN ms_played < 90000 THEN 'Partial listen (30s - 1.5min)'
    WHEN ms_played < 150000 THEN 'Most of track (1.5 - 2.5min)'
    WHEN ms_played >= 150000 THEN 'Completed (>2.5min)'
  END AS completion_category,
  COUNT(*) AS plays,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct_of_total
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE master_metadata_track_name IS NOT NULL  -- Music only
  AND ms_played > 0
GROUP BY completion_category
ORDER BY 
  CASE completion_category
    WHEN 'Skipped immediately (<30s)' THEN 1
    WHEN 'Partial listen (30s - 1.5min)' THEN 2
    WHEN 'Most of track (1.5 - 2.5min)' THEN 3
    ELSE 4
  END;


-- ================================================================
-- SECTION 7: DATA QUALITY CHECKS
-- ================================================================

-- 7.1 Check for impossible values
SELECT 
  'Negative play duration' AS issue,
  COUNT(*) AS count
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE ms_played < 0
UNION ALL
SELECT 
  'Play duration > 1 hour',
  COUNT(*)
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE ms_played > 3600000
  AND master_metadata_track_name IS NOT NULL
UNION ALL
SELECT 
  'Future timestamps',
  COUNT(*)
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE ts > CURRENT_TIMESTAMP()
UNION ALL
SELECT 
  'Very old timestamps (pre-September 2013)',
  COUNT(*)
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE ts < '2013-09-01';

-- 7.2 Missing critical fields
SELECT 
  'Records with no track name AND no episode name' AS issue,
  COUNT(*) AS count
FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_raw`
WHERE master_metadata_track_name IS NULL 
  AND episode_name IS NULL;