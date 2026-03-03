# Data Pipeline Architecture

## Overview

This document describes the end-to-end data pipeline for the Spotify Streaming Analytics project, from 17 raw JSON files to 6 dynamic dashboards in Tableau.

---

## Pipeline Stages
```
Raw JSON Files → Raw JSONL Files → BigQuery Raw Table → Silver Table → Sessions Table → Analytical Views → Tableau
```

---

## Stage 1: Data Ingestion (Bronze Layer)

### Source Data
- **Format**: JSON files from Spotify Extended Streaming History
- **Location**: Local files exported from Spotify account after request
- **Time Range**: September 2013 - February 2026
- **Total Records**: 266,106 plays (music + podcasts)

### Schema
```sql
streaming_history_raw
├── ts (TIMESTAMP)                    -- UTC timestamp of play end
├── ms_played (INT64)                 -- Milliseconds played
├── master_metadata_track_name (STRING)
├── master_metadata_album_artist_name (STRING)
├── master_metadata_album_album_name (STRING)
├── spotify_track_uri (STRING)
├── episode_name (STRING)             -- Podcast episodes
├── episode_show_name (STRING)        -- Podcast shows
├── platform (STRING)                 -- Device used
├── reason_start (STRING)             -- Play trigger reason
├── reason_end (STRING)               -- Play end reason
├── shuffle (BOOLEAN)
├── skipped (BOOLEAN)
├── offline (BOOLEAN)
├── offline_timestamp (INT64)
├── incognito_mode (BOOLEAN)
```

### Ingestion Process
1. Export JSON files from Spotify
2. Upload to Google Cloud Storage
3. Transform all to JSONL with Python in Cloud Shell
4. Load as table to BigQuery using schema auto-detection

---

## Stage 2: Data Cleaning (Silver Layer)

### Purpose
To transform raw data into analysis-ready format with quality filters and enrichments.

### Transformations

#### 2.1 Filtering
```sql
Music only:
- episode_name IS NULL
- episode_show_name IS NULL
- master_metadata_track_name IS NOT NULL
- spotify_track_uri IS NOT NULL

Quality filters:
- ts IS NOT NULL
- ms_played >= 1000 (excludes ultra-fast skips with API bugs)
- DATE(ts) >= '2013-09-23' (first valid listening date)
- Exclude December 7-8, 2020 (anomalous API behavior)
```

#### 2.2 Deduplication
```sql
PARTITION BY:
- ts (exact end timestamp)
- spotify_track_uri (unique track identifier)
- ms_played (distinguish replays)

METHOD: ROW_NUMBER() OVER (...) WHERE row_num = 1
```

#### 2.3 Timezone Conversion
```sql
FROM: UTC (ts field)
TO: America/Mazatlan (MST -7:00)

CALCULATED FIELDS:
- play_start_MST = TIMESTAMP_SUB(ts, INTERVAL ms_played MILLISECOND)
- play_end_MST = ts (converted to MST)
```

#### 2.4 Derived Fields
```sql
Temporal:
- play_date, play_year, play_month, play_day_week, play_hour
- time_of_day (Night/Morning/Afternoon/Evening)

Duration:
- seconds_played = ms_played / 1000
- minutes_played = ms_played / 60000
- hours_played = ms_played / 3600000

Engagement:
- completion_category:
  * Skipped (<30s)
  * Partial (30s-2.5m)
  * Complete (>2.5m)
```

### Output Schema
```sql
streaming_history_clean
├── play_start_MST (DATIME)
├── play_end_MST (DATETIME)
├── ms_played (INT64)
├── track_name (STRING)
├── artist_name (STRING)
├── album_name (STRING)
├── track_uri (STRING)
├── device (STRING)
├── reason_start (STRING)
├── reason_end (STRING)
├── shuffle_flag (BOOLEAN)
├── skipped_flag (BOOLEAN)
├── offline_flag (BOOLEAN)
├── offline_ts_UTC (INT64)
├── incognito_flag (BOOLEAN)
├── play_date (DATE)
├── play_year (INT64)
├── play_month (INT64)
├── play_day_week (STRING)
├── play_hour (INT64)
├── time_of_day (STRING)
├── seconds_played (FLOAT64)
├── minutes_played (FLOAT64)
├── hours_played (FLOAT64)
├── completion_category (STRING)
```

### Data Quality Results
- **Input**: 266,106 raw plays
- **Output**: 238,742 clean only-music plays
- **Removed**: 27,316 plays (10.27%)

### SQL Script
- **File**: `sql/02_data_cleaning_silver.sql`
- **Action**: CREATE OR REPLACE TABLE
- **Dependencies**: streaming_history_raw

---

## Stage 3: Session Construction

### Purpose
To aggregate individual plays into listening sessions based on temporal gaps, and to classify them according to some logics.

### Session Logic

#### 3.1 Gap Calculation
```sql
Gap = play_start_MST - LAG(play_end_MST)

ORDER BY:
1. play_start_MST
2. play_end_MST
3. spotify_track_uri
```

#### 3.2 Session Boundary Detection
```sql
New session IF:
- Gap IS NULL (first play ever)
- Gap > 1800 seconds (30 minutes as found significant on EDA)
- Gap < -1800 seconds (to handle severe overlaps caused by API bug)
```

#### 3.3 Session ID Assignment
```sql
session_id = SUM(new_session_flag) OVER (
  ORDER BY play_start_MST, play_end_MST, track_uri
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

#### 3.4 Session Aggregations
```sql
Per Session:
- session_start, session_end
- session_duration_seconds, session_duration_minutes
- tracks_played, unique_artists, unique_albums, devices_used
- total_minutes_played, avg_track_duration
- shuffle_usage_pct, skip_rate, completion_rate, offline plays
- diversity_score = unique_artists / tracks_played
- weighted_diversity_score = diversity × MIN(tracks_played/10, 1.0)
- primary_device = APPROX_TOP_COUNT(device, 1)
```

#### 3.5 Session Classification

**Percentile-Based Thresholds:**
```
Metric analysis results:
- skip_rate: P50=0%, P75=33.33%, P90=66.67%
- shuffle: P50=100%, P75=100%, P90=100% (bimodal)
- diversity: P50=0.86, P75=1.0, P90=1.0
- weighted_div: P50=0.40, P75=0.74, P90=0.90
- completion: P50=50%, P75=75%, P90=100%
```

**Session Types:**
```sql
1. Active Search (8.6%):
   - skip_rate > 33.33% (P75) AND shuffle > 90% (P50+)
   - Behavior: Aggressively searching for "the right song"
   
2. Discovery Mode (48.4%):
   - shuffle > 90%
   - Behavior: Trust shuffle algorithm, passive discovery
   
3. Variety Seeker (6.5%):
   - weighted_diversity > 0.74 (P75)
   - Behavior: Intentional curation of diverse artists
   
4. Album Listening (4.6%):
   - weighted_diversity < 0.20 (P25) AND completion > 75% (P75)
   - Behavior: Attentive album/playlist listening
   
5. Deep Dive on Artist (7.3%):
   - weighted_diversity < 0.20 (P25)
   - Behavior: Artist saved catalog exploration
   
6. Balanced Session (24.6%):
   - Default: Balanced behavior without extremes
```

### Output Schema
```sql
sessions
├── session_id (INT64)
├── session_start (DATEIME)
├── session_end (DATETIME)
├── session_date (DATE)
├── session_year (INT64)
├── session_month (INT64)
├── session_day_of_week (INT64)
├── session_start_hour (INT64)
├── session_duration_seconds (INT64)
├── session_duration_minutes (FLOAT64)
├── total_minutes_played (FLOAT64)
├── session_length_category (STRING)
├── primary_device (STRING)
├── devices_used (INT64)
├── tracks_played (INT64)
├── unique_artists (INT64)
├── unique_albums (INT64)
├── avg_track_duration (FLOAT64)
├── diversity_score (FLOAT64)
├── weighted_diversity_score (FLOAT64)
├── shuffle_usage_pct (FLOAT64)
├── skip_rate (FLOAT64)
├── completion_rate (FLOAT64)
├── offline_plays (INT64)
├── session_type (STRING)
├── engagement_level (STRING)
```

### Validation Results
- **Total Sessions**: 17,714
- **Intra-session gaps >30 min**: 0 ✅
- **Max intra-session gap**: 30 minutes ✅

### SQL Script
- **File**: `sql/03_sessions_construction.sql`
- **Action**: CREATE OR REPLACE TABLE
- **Dependencies**: streaming_history_clean

---

## Stage 4: Analytical Views (Gold Layer)

### Purpose
To aggregate virtual tables (Views) optimized for Tableau visualization and analysis.

### Views Created

#### 4.1 Artist Statistics (`view_artist_stats`)
```sql
Aggregations per artist:
- total_plays, unique_tracks
- total_minutes, total_hours, avg_track_duration
- first_play_date, last_play_date, days_span
- skip_rate, completion_rate, shuffle_usage_pct
- primary_device, devices_used
- preferred_time, days_since_last_play

Filter: total_plays >= 5 (removes noise)
```

#### 4.2 Track Statistics (`view_track_stats`)
```sql
Aggregations per track, album and artist:
- play_count, total_minutes_listened, avg_play_duration
- first_played, last_played, listening_span_days
- skip_rate, completion_rate, shuffle_usage_pct
- most_common_time, days_since_last_play

Filter: play_count >= 3 (removes noise)
```

#### 4.3 Temporal Patterns (`view_temporal_patterns`)
```sql
Aggregations by play_year, play_month, play_day_week, play_hour, time_of_day:
- total_plays, unique_artists, unique_tracks
- total_minutes, avg_track_duration, avg_plays_per_hour
- skip_rate, completion_rate, shuffle_usage_pct
```

#### 4.4 Session Analytics (`view_session_behavior`)
```sql
Aggregations by session dimensions (session_year, session_month, session_day_of_week, session_start_hour, session_length_category, session_type, engagement_level):
- session_count, total_tracks
- avg_tracks_per_session, avg_duration_minutes, avg_unique_artists
- avg_diversity, avg_weighted_diversity, avg_skip_rate, avg_completion_rate, avg_shuffle_usage
- most_common_device
```

#### 4.5 Yearly Trends (`view_yearly_trends`)
```sql
Aggregations by year:
- total_plays, unique_artists, unique_tracks
- total_hours, avg_track_duration
- skip_rate, completion_rate, shuffle_usage_pct
- pct_yoy_play_count, pct_yoy_hours_played
```

#### 4.6 Top Content (`view_top_content`)
```sql
Dynamic rankings:
- All-time top artists (top 50)
- Yearly top artists (top 20 per year)
- All-time top tracks (top 50)
- Yearly top tracks (top 20 per year)

Columns:
- time_period, year, content_type, artist_name, track_name
- play_count, total_hours, rank
```

### SQL Script
- **File**: `sql/04_analytical_views_gold.sql`
- **Action**: CREATE OR REPLACE VIEW (6 views)
- **Dependencies**: streaming_history_clean, sessions

---

## Stage 5: Export to Tableau

### Export Method
```
BigQuery Sessions Table and Views → Tableau Desktop 'Google BigQuery' Connector
```

### Export Process
1. **BigQuery Console**:
   - Revise each final view
   - Repeat for all 6 views

2. **Tableau Desktop**:
   - Workbook → Data → Connect → Google BigQuery
   - Select and extract each table, one by one
   - Create visualizations and dashboards
   - Update views after optimizations
   - Homologate visualizations and dashboards formats
   - Take snapshots for project documentation

### Views Exported
```
✅ view_artist_stats (2,873 rows)
✅ view_track_stats (9,263 rows)
✅ view_temporal_patterns (14,625 rows)
✅ view_session_behavior (17,063 rows)
✅ view_yearly_trends (14 rows)
✅ view_top_content (660 rows)
```

---

## Performance Considerations

### BigQuery Optimization
- **Partitioning**: Not implemented (tables <1GB)
- **Clustering**: Not implemented (query patterns don't benefit)
- **Views vs Tables**: Views used for optimal performance
- **Query cost**: Cents of a dollar per full pipeline execution

### Tableau Optimization
- **Extracts**: Live connections are not necessary nor possible
- **Aggregation**: Pre-aggregated in views (gold layer)
- **Calculations**: All calculated fields are done in BigQuery
- **Filters**: No date or any other filters in extract settings

---

## Future Enhancements

### Potential Improvements
1. **Automated exports**: Schedule BigQuery connector to scan for new data every few months. It would require to ask Spotify for my streaming history more often, and automate the creation of the JSONL file
2. **Additional metrics**: 
   - Genre analysis (requires Spotify API integration)
   - Mood/energy patterns (requires audio features and ML)
3. **Comparative analysis**: Compare to Spotify global averages

### Scalability
Current pipeline supports:
- **Volume**: Up to 10M plays without modifications
- **Velocity**: Manual refresh (quarterly or so updates)
- **Variety**: Music only (podcast support requires separate pipeline)

---

## Documentation References

- Data quality issues: `docs/exploratory_findings.md`
- Session construction problems: `docs/problems_session_construction.md`
- Project README: `README.md`

---

## Contact & Maintenance

**Author**: Heriberto Giusti
**Mail**: heribertogiusti@gmail.com
**Last Updated**: March 2026
**Update Frequency**: When new Spotify data is requested