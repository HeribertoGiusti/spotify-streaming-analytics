-- VIEW 1: artist_stats

SELECT 
  artist_name,
  
  -- Volume metrics
  COUNT(*) AS total_plays,
  COUNT(DISTINCT track_name) AS unique_tracks,
-- COUNT(DISTINCT session_id) AS sessions_featured,
  
  -- Listening time
  ROUND(SUM(minutes_played), 2) AS total_minutes,
  ROUND(SUM(hours_played), 2) AS total_hours,
  ROUND(AVG(minutes_played), 2) AS avg_track_duration,
  
  -- Temporal patterns
  MIN(play_date) AS first_play_date,
  MAX(play_date) AS last_play_date,
  DATE_DIFF(MAX(play_date), MIN(play_date), DAY) AS days_span,
  APPROX_TOP_COUNT(time_of_day, 1)[OFFSET(0)].value AS preferred_time,

  -- Engagement metrics
  ROUND(AVG(CASE WHEN skipped_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS skip_rate,
  ROUND(AVG(CASE WHEN completion_category = 'Complete (>2.5m)' THEN 1.0 ELSE 0.0 END) * 100, 2) AS completion_rate,
  ROUND(AVG(CASE WHEN shuffle_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS shuffle_usage_pct,
  
  -- Device preferences
  APPROX_TOP_COUNT(device, 1)[OFFSET(0)].value AS primary_device,
  COUNT(DISTINCT device) AS devices_used,
  
  -- Recency
  DATE_DIFF(CURRENT_DATE(), MAX(play_date), DAY) AS days_since_last_play

FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY artist_name
HAVING total_plays >= 5  -- Filters noise
ORDER BY total_plays DESC;

--------------------------------

-- VIEW 2: track_stats

SELECT 
  track_name,
  artist_name,
  album_name,
  
  -- Play metrics
  COUNT(*) AS play_count,
-- COUNT(DISTINCT session_id) AS sessions_played_in,
  
  -- Listening time
  ROUND(SUM(minutes_played), 2) AS total_minutes_listened,
  ROUND(AVG(minutes_played), 2) AS avg_play_duration,
  
  -- Temporal patterns
  MIN(play_date) AS first_played,
  MAX(play_date) AS last_played,
  DATE_DIFF(MAX(play_date), MIN(play_date), DAY) AS listening_span_days,
  APPROX_TOP_COUNT(time_of_day, 1)[OFFSET(0)].value AS most_common_time,

  -- Engagement metrics
  ROUND(AVG(CASE WHEN skipped_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS skip_rate,
  ROUND(AVG(CASE WHEN completion_category = 'Complete (>2.5m)' THEN 1.0 ELSE 0.0 END) * 100, 2) AS completion_rate,
  ROUND(AVG(CASE WHEN shuffle_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS shuffle_usage_pct,
  
  -- Recency
  DATE_DIFF(CURRENT_DATE(), MAX(play_date), DAY) AS days_since_last_play

FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY track_name, artist_name, album_name
HAVING play_count >= 3
ORDER BY play_count DESC;

--------------------------------

-- VIEW 3: temporal_patterns

SELECT 
  play_year,
  play_month,
  play_day_week,
  play_hour,
  time_of_day,
  
  -- Volume
  COUNT(*) AS total_plays,
-- COUNT(DISTINCT session_id) AS total_sessions,
  COUNT(DISTINCT artist_name) AS unique_artists,
  COUNT(DISTINCT track_name) AS unique_tracks,
  
  -- Listening time
  ROUND(SUM(minutes_played), 2) AS total_minutes,
  ROUND(AVG(minutes_played), 2) AS avg_track_duration,
  ROUND(AVG(COUNT(*)) OVER (PARTITION BY play_year, play_month, play_day_week), 1) AS avg_plays_per_hour,
  
  -- Engagement metrics
  ROUND(AVG(CASE WHEN skipped_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS skip_rate,
  ROUND(AVG(CASE WHEN completion_category = 'Complete (>2.5m)' THEN 1.0 ELSE 0.0 END) * 100, 2) AS completion_rate,
  ROUND(AVG(CASE WHEN shuffle_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS shuffle_usage_pct

FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY play_year, play_month, play_day_week, play_hour, time_of_day
ORDER BY play_year, play_month, play_day_week, play_hour;

--------------------------------

-- VIEW 4: session_behavior

SELECT 
  session_year,
  session_month,
  session_day_of_week,
  session_start_hour,
  session_length_category,
  session_type,
  engagement_level,
  
  -- Counts
  COUNT(*) AS session_count,
  SUM(tracks_played) AS total_tracks,
  
  -- Averages
  ROUND(AVG(tracks_played), 1) AS avg_tracks_per_session,
  ROUND(AVG(session_duration_minutes), 1) AS avg_duration_minutes,
  ROUND(AVG(unique_artists), 1) AS avg_unique_artists,
  
  -- Behavior
  ROUND(AVG(diversity_score), 3) AS avg_diversity,
  ROUND(AVG(weighted_diversity_score), 3) AS avg_weighted_diversity,
  ROUND(AVG(skip_rate), 2) AS avg_skip_rate,
  ROUND(AVG(completion_rate), 2) AS avg_completion_rate,
  ROUND(AVG(shuffle_usage_pct), 2) AS avg_shuffle_usage,
  
  -- Device
  APPROX_TOP_COUNT(primary_device, 1)[OFFSET(0)].value AS most_common_device

FROM `robotic-door-487416-b4.spotify_analytics.sessions`
GROUP BY 
  session_year, 
  session_month, 
  session_day_of_week, 
  session_start_hour,
  session_length_category,
  session_type,
  engagement_level
ORDER BY session_year, session_month;

--------------------------------

-- VIEW 5: yearly_trends

SELECT 
  play_year,
  
  -- Volume
  COUNT(*) AS total_plays,
  COUNT(DISTINCT artist_name) AS unique_artists,
  COUNT(DISTINCT track_name) AS unique_tracks,
-- COUNT(DISTINCT session_id) AS total_sessions,
  
  -- Listening time
  ROUND(SUM(hours_played), 2) AS total_hours,
  ROUND(AVG(minutes_played), 2) AS avg_track_duration,
  
  -- Engagement metrics
  ROUND(AVG(CASE WHEN skipped_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS skip_rate,
  ROUND(AVG(CASE WHEN completion_category = 'Complete (>2.5m)' THEN 1.0 ELSE 0.0 END) * 100, 2) AS completion_rate,
  ROUND(AVG(CASE WHEN shuffle_flag THEN 1.0 ELSE 0.0 END) * 100, 2) AS shuffle_usage_pct,
  
  -- YoY growth
  ROUND(
    (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY play_year)) * 100.0 / 
    LAG(COUNT(*)) OVER (ORDER BY play_year),
    2
  ) AS pct_yoy_play_count,
  
  ROUND(
    (SUM(hours_played) - LAG(SUM(hours_played)) OVER (ORDER BY play_year)) * 100.0 / 
    LAG(SUM(hours_played)) OVER (ORDER BY play_year),
    2
  ) AS pct_yoy_hours_played

FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
GROUP BY play_year
ORDER BY play_year;

--------------------------------

-- VIEW 6: top_content

WITH artist_rankings AS (
  SELECT 
    'All Time' AS time_period,
    CAST(NULL AS INT64) AS year,
    'Artist' AS content_type,
    artist_name AS name,
    CAST(NULL AS STRING) AS track_name,
    COUNT(*) AS play_count,
    ROUND(SUM(hours_played), 2) AS total_hours,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM `robotic-door-487416-b4.spotify_analytics.streaming_history_clean`
  GROUP BY artist_name
  QUALIFY rank <= 50
),

yearly_artist_rankings AS (
  SELECT 
    'Yearly' AS time_period,
    play_year AS year,
    'Artist' AS content_type,
    artist_name AS name,
    CAST(NULL AS STRING) AS track_name,
    COUNT(*) AS play_count,
    ROUND(SUM(hours_played), 2) AS total_hours,
    ROW_NUMBER() OVER (PARTITION BY play_year ORDER BY COUNT(*) DESC) AS rank
  FROM `spotify_analytics.streaming_history_clean`
  GROUP BY play_year, artist_name
  QUALIFY rank <= 20
),

track_rankings AS (
  SELECT 
    'All Time' AS time_period,
    CAST(NULL AS INT64) AS year,
    'Track' AS content_type,
    artist_name AS name,
    track_name,
    COUNT(*) AS play_count,
    ROUND(SUM(hours_played), 2) AS total_hours,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM `spotify_analytics.streaming_history_clean`
  GROUP BY artist_name, track_name
  QUALIFY rank <= 50
)

SELECT * FROM artist_rankings
UNION ALL
SELECT * FROM yearly_artist_rankings
UNION ALL
SELECT * FROM track_rankings
ORDER BY time_period, year, content_type, rank;