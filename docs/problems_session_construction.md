# Data Quality Issues Log

This document tracks data quality problems identified during the analytical 'sessions' table construction, as well as their root causes and resolutions.

---

## Overview

| Issue ID |  Severity | Discovered |  Status  |
|----------|-----------|------------|----------|
|  DQ-001  |   High    | 2026-02-17 | Resolved |
|  DQ-002  |  Medium   | 2026-02-17 | Resolved |
|  DQ-003  |   Low     | 2025-02-17 | Resolved |
|  DQ-004  | Very High | 2025-02-17 | Resolved |

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

## Lessons Learned

1. **Play start not included:** The original data only measures a track's ending timestamp, so it had to be calculated up to the millisecond to allow for a complete analysis.
2. **API lag problem:** There were many cases were the ending time of a song overlapped with the start of another one, which is a technical registering problem that cannot be avoided but can be minimized.
3. **Timestamp ordering:** Because of the lag issues, the ordering for the session construction window functions had to be millisecond specific and include the track URI, so there are no ties.
4. **Types of overlapping:** It was found that the small amount of cross-device overlaps are to be expected, and that severe same-device ones can be handled with the corresponding conditions. Most importantly: the remaining same- and cross-device overlaps don't interfere with the sessions construction because of the applied length conditions.