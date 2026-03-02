# Data Quality Issues Log

This document tracks data quality problems identified during the analytical 'sessions' table construction, as well as their root causes and resolutions.

---

## Overview

| Issue ID |  Severity | Discovered |  Status  |
|----------|-----------|------------|----------|
|  DQ-001  |   High    | 2026-02-17 | Resolved |
|  DQ-002  |  Medium   | 2026-02-17 | Resolved |
|  DQ-003  |   Low     | 2026-02-17 | Resolved |
|  DQ-004  | Very High | 2026-02-17 | Resolved |
|  DQ-005  | Very High | 2026-02-28 | Resolved |

---

## Active Issues

### DQ-001: Negative session_duration_minutes 
**Discovered:** Verify 5 - Sessions over time  
**Severity:** High
**Status:** ✅ Resolved  

**Description:**  
Many sessions showed negative values in `session_duration_minutes`.

**Root Cause Analysis:**  
SELECT 
  session_id,
  session_start,
  session_end,
  session_duration_minutes
FROM `spotify_analytics.sessions`
WHERE session_duration_minutes < 0
ORDER BY session_duration_minutes ASC
LIMIT 10;

**Impact:**  
- Affects `session_length_category` classification
- Distorts average duration calculations
- Would affect Tableau visualizations

**Proposed Resolution:**
In CTE session_aggregations it was found that the TIMESTAMP_DIFF parameters were backwards, substracting track play starts from their play ends.

ROUND(TIMESTAMP_DIFF(MAX(play_end_MST), MIN(play_start_MST), SECOND) / 60.0, 2) AS session_duration_minutes

**Resolution Changes:**
Now all the cases of `session_length_category` are shown and there are no negative average session durations.

---

### DQ-002: Missing engagement_level categories
**Discovered:** Verify 3 - Session types distribution  
**Severity:** Medium  
**Status:** ✅ Resolved   

**Description:**  
Some sessions fall into NULL categories in `engagement_level` field, and only 'Low Engagement' is shown.

**Root Cause Analysis:**  
SELECT 
  engagement_level,
  COUNT(*) AS session_count,
  ROUND(AVG(completion_rate), 2) AS avg_completion_rate,
  ROUND(AVG(skip_rate), 2) AS avg_skip_rate,
  ROUND(AVG(tracks_played), 1) AS avg_tracks
FROM `spotify_analytics.sessions`
GROUP BY engagement_level
ORDER BY 
  CASE engagement_level
    WHEN 'High Engagement' THEN 1
    WHEN 'Medium Engagement' THEN 2
    ELSE 3
  END;

**Impact:**  
Session type classification is imprecise.

**Proposed Resolution:**  
The lineage was traced from `completion_category` (silver table) --> `completion_rate` (sessions table) --> `engagement_level`, and a typo was found for the "Complete (>2.5m)" category, which caused the data to be poorly extracted.

ROUND(COUNTIF(completion_category = 'Complete (>2.5m)') * 100.0 / COUNT(*),
      2) AS completion_rate,

**Resolution Changes:**
Now all the completion categories are shown.

---

### DQ-003: Incongruent top session characteristics
**Discovered:** Verify 6 - Top session charecteristics
**Severity:** Low  
**Status:** ✅ Resolved   

**Description:**  
The metric of 'Most Diverse Session' didn't make sense, because it showed a total of 1 unique artists.

**Root Cause Analysis:**  
ROUND(COUNT(DISTINCT artist_name) * 1.0 / COUNT(*), 2) AS diversity_score

This doesn't discriminate if a session is extremely short (e.g. 1 track).

**Impact:**  
Could distort later analysis and visualizations.

**Proposed Resolution:**  
A new metric was introduced which penalizes short sessions.

ROUND((COUNT(DISTINCT artist_name) * 1.0 / COUNT(*)) * LEAST(COUNT(*) / 10.0, 1.0), 2) AS weighted_diversity_score

**Resolution Changes:**
Now the diversity score is more realistic because it takes into consideration if many tracks with many artists were played.

---

### DQ-004: Gaps with more than 30 minutes inside a session
**Discovered:** SILVER: 8. Remaining overlaps + SESSIONS: Verify 7 - Validates no orphaned plays
**Severity:** Very High  
**Status:** ✅ Resolved  

**Description:**
For the originally calculated 18,169 sessions, there was a total of 778 with a gap under 30 minutes between them, which is inconsistent with the treshold created for defining a new session.

**Root Cause:**
After an exhaustive analysis, there were a handful of issues detected:
  a) There is a bug on the Spotify API when registering play ending times for tracks that are very rapidly skipped.
  b) There is another bug for registering play ending times in tracks following long ones (4-5 minutes).
  c) There are many overlaps, some with negative values, for continuous same-device sessions.

For problem a) it was found that when a track was skipped in less than a second, there were many cases of severe overlap (>30 minutes). A comparison was made between 0.5, 1, 2, 3, 5 and 8 seconds for chosing an adequate threshold to filter those cases out.

For b) it was found that when a long track ended, a lot of times the next track started before the previous end was recorded.

For c) it was found that same-device overlaps were significantly more common than cross-device ones. The later cases are somehow expected for multi-device sessions, and also there were a lot less, so there was no attempt to mitigate them. The hypothesis was that the API errors caused all the overlaps, but a coding logic was also found later: after using the new ms_played threshold and defining a new session every 30 minutes, there were still same-device sessions with overlap and it was found that the ORDER BY in 'time_gaps' CTE was allowing some duplicate records.

**Proposed Resolution:**  
a) WHERE ms_played >= 1000 in silver table first CTE. The chosen threshold was 1 second because a bigger one removed many more records while not reducing overlaps in a significant way.

b) It couldn't be filtered without losing +40% of data.

c) Three-level tiebreaker in silver table final SELECT: ORDER BY TIMESTAMP_SUB(ts, INTERVAL ms_played), ts, spotify_track_uri
    + Same ordering logic in session table CTEs 'time_gaps' and 'plays_with_session_id'
    + Second level precision plus < and > filtering, instead of <= and >=, for seconds_since_last_play in CTE 'session_starts'

**Resolution Changes:**  
- Before: 778 sessions with less than 30 minutes between them + many consecutive songs with severe overlap.
- After:
  * All tracks assigned to exactly one session.
  * 6.5% of track registers were removed with the `ms_played` filter, accounting for 10.6% of general severe overlaps. Cross-device overlaps were left unchecked because they are to be expected since two or more devices can be streaming simultaneously, while Same-device overlaps were minimized. 
  * The remaining severe same-device overlaps were filtered out in the 'seconds_since_last_play' CTE, for any gap higher than 30 minutes, which is the limit for a session.

| check_type | total_transitions | total_overlaps	| cross_device_overlaps |	same_device_ov |	severe_same_device_ov |
|------------|-------------------|----------------|-----------------------|----------------|------------------------|
|  Overlaps  |      238741       |    	77072     |       	1414          |	    75658      |           0            |

---

### DQ-005: Session types not representative of data distribution 
**Severity:** Very High  
**Status:** ✅ Resolved 
**Discovered:**
SELECT 
  -- Percentiles of skip_rate
  APPROX_QUANTILES(skip_rate, 100)[OFFSET(10)] AS skip_rate_p10,
  APPROX_QUANTILES(skip_rate, 100)[OFFSET(25)] AS skip_rate_p25,
  APPROX_QUANTILES(skip_rate, 100)[OFFSET(50)] AS skip_rate_p50,
  APPROX_QUANTILES(skip_rate, 100)[OFFSET(75)] AS skip_rate_p75,
  APPROX_QUANTILES(skip_rate, 100)[OFFSET(90)] AS skip_rate_p90,
  
  -- Percentiles of shuffle_usage_pct
  APPROX_QUANTILES(shuffle_usage_pct, 100)[OFFSET(10)] AS shuffle_p10,
  APPROX_QUANTILES(shuffle_usage_pct, 100)[OFFSET(25)] AS shuffle_p25,
  APPROX_QUANTILES(shuffle_usage_pct, 100)[OFFSET(50)] AS shuffle_p50,
  APPROX_QUANTILES(shuffle_usage_pct, 100)[OFFSET(75)] AS shuffle_p75,
  APPROX_QUANTILES(shuffle_usage_pct, 100)[OFFSET(90)] AS shuffle_p90,
  
  -- Percentiles of diversity_score
  APPROX_QUANTILES(diversity_score, 100)[OFFSET(10)] AS diversity_p10,
  APPROX_QUANTILES(diversity_score, 100)[OFFSET(25)] AS diversity_p25,
  APPROX_QUANTILES(diversity_score, 100)[OFFSET(50)] AS diversity_p50,
  APPROX_QUANTILES(diversity_score, 100)[OFFSET(75)] AS diversity_p75,
  APPROX_QUANTILES(diversity_score, 100)[OFFSET(90)] AS diversity_p90,
  
  -- Percentiles of weighted_diversity_score
  APPROX_QUANTILES(weighted_diversity_score, 100)[OFFSET(10)] AS weighted_div_p10,
  APPROX_QUANTILES(weighted_diversity_score, 100)[OFFSET(25)] AS weighted_div_p25,
  APPROX_QUANTILES(weighted_diversity_score, 100)[OFFSET(50)] AS weighted_div_p50,
  APPROX_QUANTILES(weighted_diversity_score, 100)[OFFSET(75)] AS weighted_div_p75,
  APPROX_QUANTILES(weighted_diversity_score, 100)[OFFSET(90)] AS weighted_div_p90,
  
  -- Percentiles of completion_rate
  APPROX_QUANTILES(completion_rate, 100)[OFFSET(10)] AS completion_p10,
  APPROX_QUANTILES(completion_rate, 100)[OFFSET(25)] AS completion_p25,
  APPROX_QUANTILES(completion_rate, 100)[OFFSET(50)] AS completion_p50,
  APPROX_QUANTILES(completion_rate, 100)[OFFSET(75)] AS completion_p75,
  APPROX_QUANTILES(completion_rate, 100)[OFFSET(90)] AS completion_p90

FROM `robotic-door-487416-b4.spotify_analytics.sessions`;

**Description:**
During the construction of the field 'session_type', the categories and thresholds of them were firstly speculated through the personal knowledge of my different types of listening behavior. However, an iteration had to be made once other problems with the data were fixed, in order to know the most precise and adequate way to classify the listening sessions.

**Root Cause:**
A discrepancy between the theoretical and the real data is to be expected in these cases, and this is why some categories were, in my interpretation, less represented than others.

**Proposed Resolution:**
An statistical analysis of the data distribution for the sessions, in the many metrics available, revealed important insigths. Some thresholds were found to be either very restrictive (not capturing sufficient information) or very broad (capturing too much and thus making the category system useless). An adjustment of the thresholds in order to capture the bottom and the top percentiles is proposed.

|  10% of data |  25% of data |  50% of data |  75% of data |	 90% of data |
|--------------|--------------|--------------|--------------|--------------|
| skip_rate_p10| skip_rate_p25| skip_rate_p50| skip_rate_p75| skip_rate_p90|
|       0      |     	 0      |	      0	     |    33.33     |    66.67     |
| shuffle_p10	 | shuffle_p25  | shuffle_p50	 | shuffle_p75	| shuffle_p90  |
|       0	     |    33.33     |    	 100	   |     100	    |     100      |
|diversity_p10 |diversity_p25 |diversity_p50 |diversity_p75 |diversity_p90 |
|     0.27	   |      0.6     |  	  0.86     |   	  1       |      1       |
|weight_div_p10|weight_div_p25|weight_div_p50|weight_div_p75|weight_div_p90|
|      0.1	   |      0.2	    |      0.4	   |    0.74	    |     0.9      | 
|completion_p10|completion_p25|completion_p50|completion_p75|completion_p90|
|       0      |     32.26    |      50      |    	75      |	    100      |

**Resolution Changes:**  
- Active Search: Sessions with very high shuffle usage and also high skip rate, were I was trying to find a specific song or genre that pleased my ears in that moment. The previous skip rate threshold included less than 10% of the sessions, so it had to be lowered, but the shuffle one raised for compensation.
  * Before: skip > 50 AND shuffle > 70
  * After: skip > 33.33 AND shuffle > 90

- Discovery Mode: Sessions with very high shuffle mode on, but where I didn't skipped and instead trusted the algorithm. It was found that my listening behavior is very bimodal, that is to say, a few sessions are completely devoid of shuffle usage, but most have most of their songs played in that mode. As with the previous condition, it had to be raised in order to more clearly distinguish the two types of shuffle usage (non-existent or very high).
  * Before: shuffle > 80
  * After: shuffle > 90

- Variety Seeker: These sessions are distinguished by the high diversity of the artists in them. The weighted_diversity score was designed in order to penalize very short sessions with few artists, were the score can be high but irrelevant. It was found that this metric followed a Normal distribution, so the threshold was just slightly altered in order to fit it more properly and capture exactly the top quartile of sessions.
  * Before: weighted_diversity > 0.7
  * After: weighted_diversity > 0.74

- Album Listening: A metric that captures sessions that are mostly or completely from one artist, and the completion of its songs is very high, which suggests that the listener is playing an entire album. The original diversity score was replaced with the weighted one and the threshold was fixed so that it captures the botom 25% of the data, that is, the less diverse sessions.
  * Before: diversity < 0.3 AND completion > 80
  * After: weighted_diversity < 0.20 AND completion > 75

- Deep Dive on Artist: This metric is also for sessions focused on one artist, but skipped more often probably because I played them from my 'Saved tracks' of that artist, either on shuffle or sequential mode.
  * Before: diversity < 0.3
  * After: weighted_diversity < 0.20

- Balanced Session: Captures the rest of the sessions that weren't included in the previous categories, that is to say, the ones that display average behaviors.
  * Before and After: It was part of the ELSE clause and remained so.

---

## Lessons Learned

1. **Play start not included:** The original data only measures a track's ending timestamp, so it had to be calculated up to the millisecond to allow for a complete analysis.
2. **API lag problem:** There were many cases were the ending time of a song overlapped with the start of another one, which is a technical registering problem that cannot be avoided but can be minimized.
3. **Timestamp ordering:** Because of the lag issues, the ordering for the session construction window functions had to be millisecond specific and include the track URI, so there are no ties.
4. **Types of overlapping:** It was found that the small amount of cross-device overlaps are to be expected, and that severe same-device ones can be handled with the corresponding conditions. Most importantly: the remaining same- and cross-device overlaps don't interfere with the sessions construction because of the applied length conditions.
5. **My listening profile:** Several characteristics of my listening behavior became evident, like that at least half of the sessions have an extremely high artist diversity (0.86); also at least half of them have a 0 skip rate, which means I can be seen as a "committed explorer". Other discoveries were that the shuffle behavior is bimodal, with some sessions having 0% of it while the majority having nearly 100%, with few in between. 