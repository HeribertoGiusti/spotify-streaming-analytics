# Exploratory Data Analysis - Key Findings

*Analysis Date: February 15, 2026*  
*Data Source: `spotify_analytics.streaming_history_raw`*

---

## Executive Summary

[Analysis of Spotify streaming history reveals a strong preference for discovery-driven listening, with 72% of songs being played using shuffle mode. Session continuity is remarkably high, with 95% of consecutive plays occurring within 30 minutes, indicating deep engagement. However, data quality issues were identified: duplicate records across key columns, and anomalous listening volumes in December 2020 (exceeding 24 hours/day) require correction before creating analytical tables to ensure accurate insights.]

---

## 1. Data Overview & Quality

### 1.1 Total Records and Date Range

**Results:**
- Total Records: [266,106]
- Date Range: [22-09-2013] to [10-02-2026]
- Days of Data: [4,523]
- Average Plays per Day: [58.8]

**Interpretation:**
[Over the span of 12 years I've played more than a quarter of a million songs, nearly 60 per day. This is consistent with my lifetime love for music.]

### 1.2 Null Value Analysis

**Results:**
- Track Names: [0.02%] null
- Episode Names: [99.98%] null
- Audiobook Names: [100%] null

**Interpretation:**
- [The proportion of null values across the playing categories is as expected.]

### 1.3 Content Type Distribution

**Results:**
| Content Type |  Plays  | Total Minutes | Avg Duration |
|--------------|---------|---------------|--------------|
| Music        | 266,085 |    559,233    |    2.1 min   |
| Podcast      |      48 |        589    |  12.26 min   |

**Interpretation:**
[Although the amount of music playing is predominant over podcasts, the average time of the sessions is higher in the second ones. This is as expected.]

### 1.4 Duplicate Detection

**Results:**
- Exact duplicates in all relevant fields: [1,741] duplicates
- Same track played at exact same timestamp: [3,690] duplicates

**Interpretation:**
[This is an important discovery since it affects the data quality. Both categories of duplicates will be removed from the analytical tables.]

---

## 2. Temporal Patterns

### 2.1 Plays by Year and Month

**Results:**
[On a surface level analysis, it seems that, aside from the first year, every month since then has had most of its days active with music.]

### 2.2 Plays by Day of Week

**Results:**
|  Day        |   Plays  |
|-------------|----------|
|  Monday     |  36,498  |
|  Tuesday    |  39,677  |
|  Wednesday  |  38,533  |
|  Thursday   |  36,595  |
|  Friday     |  38,347  |
|  Saturday   |  39,756  |
|  Sunday     |  36,700  |

**Interpretation:**
[There seems to be no major difference in plays throughout the week.]

### 2.3 Plays by Hour of Day

**Top 3 Hours:**
1. 05:00 PM MST - [18,906] plays
2. 06:00 PM MST - [17,948] plays
3. 12:00 PM MST - [16,935] plays

**Bottom 3 Hours:**
22. 02:00 AM MST - [1,364] plays
23. 03:00 AM MST - [1,292] plays
24. 04:00 AM MST - [1,060] plays

**Interpretation:**
[The results show that I listen to more music at the end of the working day, and I almost never do it after midnight.]

---

## 3. Content Analysis

### 3.1 Top 10 Artists by Plays

**Results:**
| Rank | Artist                | Plays  | Hours | Skip Rate |
|------|-----------------------|--------|-------|-----------|
| 1    | Muse                  | 10,175 | 345.1 |   24.1%   |
| 2    | Jorge Drexler         |  8,107 | 282.6 |    7.8%   |
| 3    | John Mayer            |  6,857 | 229.3 |   14.7%   |
| 4    | Nach                  |  5,557 | 286.0 |   33.4%   |
| 5    | Red Hot Chili Peppers |  4,341 | 159.3 |   18.6%   |
| 6    | indigo la End         |  3,762 | 202.7 |   18.8%   |
| 7    | Jamie Cullum          |  3,437 | 109.9 |   30.2%   |
| 8    | The Beatles           |  3,168 |  73.4 |   17.9%   |
| 9    | Pink Floyd            |  2,634 |  90.1 |   23.4%   |
| 10   | System of a Down      |  2,543 |  64.3 |   26.4%   |

**Interpretation:**
- Top 10 represent [19]% of total music listening, which is not that high and suggests that the plays are somewhat distributed across many artists.
- The skip rate is unexpectedly high in most of the top artists, which suggests that many of their songs have been progressively ignored because I've listened to them until exhaustion.

### 3.2 Top 10 Tracks by Plays

**Results:**
| Rank | Track              | Artist        | Plays | Days Since Discovery |
|------|--------------------|---------------|-------|----------------------|
| 1    | Todo se transforma | Jorge Drexler |  725  |         3,502        |
| 2    | Guitarra y vos     | Jorge Drexler |  525  |         3,289        |
| 3    | La Vuelta Al Mundo | Calle 13      |  522  |         4,362        |
| 4    | Eco                | Jorge Drexler |  522  |         3,305        |
| 5    | Flight Mode        | FAIR GAME     |  478  |         1,961        |
| 6    | Insomnia           | Team Astro    |  459  |         1,879        |
| 7    | lean back and ...  | Sweeps        |  451  |         1,960        |
| 8    | Two Fingers        | Jake Bugg     |  450  |         4,142        |
| 9    | Les eaux de mars   | Stacey Kent   |  447  |         3,639        |
| 10   | Defector           | Muse          |  437  |         3,897        |

**Interpretation:**
- Some of Jorge Drexler's songs have been played an inordinate amount of times, probably most of them near the year of discovery.
- The songs of ranked artists 1, 3, 4 and 5 are not represented in the top 10, which suggests that their plays have been more homogeneously distributed across all their songs.

---

## 4. Platform & Device Usage

### 4.1 Plays by Platform & Device

**Results:**
- Over the years I have played music in a total of 48 different devices, mostly on Android smartphones and then on Desktop, but there are Apple devices as well.

---

## 5. Behavioral Metrics

### 5.1 Play Duration Distribution

**Results:**
| Duration      |  Plays | % of Total |
|---------------|--------|------------|
| <30 seconds   | 96,963 |    37.51   |
| 30-60 seconds |  9,432 |     3.65   |
| 1-2 minutes   | 18,349 |     7.10   |
| 2-3 minutes   | 31,710 |    12.27   |
| 3-5 minutes   | 85,059 |    32.90   |
| >5 minutes    | 17,007 |     6.58   |

**Interpretation:**
- It seems adecuate that a third of the songs have been skipped, and that another third have been played from 3 to 5 minutes, which is the average duration of a song.

### 5.2 Skip Behavior Analysis

**Results:**
- Overall Skip Rate: [19.44]%
- The average minutes per played when skip are 0.76.

**Interpretation:**
- The skip rate seems kind of high, with about a fifth of the total reproductions. However, the time to decide for skipping is also high, with almost a minute before doing it.

### 5.3 Shuffle Mode Usage

**Results:**
- Shuffle ON: [72.78]% of plays
- Shuffle OFF: [27.22]% of plays

**Interpretation:**
[I'm a user that's heavily influenced by discovery and unpredictability, instead of a strict control of my listening sequence.]

### 5.4 Online vs Offline Listening

**Results:**
- Offline plays: [10.73]%

**Interpretation:**
[Since I spend most of my days in places with WiFi, most probably these plays were when doing some trip abroad.]

---

## 6. Advanced Insights

### 6.1 Listening Intensity by Date

**Result and Interpretation:**
- Days 07-12-2020 and 08-12-2020 exhibit a strange listening behavior, since their amount of hours listened to that day where 26.7 and 17.4, respectively. It would be worthile investigating the cause of this: maybe it was a technical error, maybe it's due to the duplicates, and/or maybe I left Spotify streaming unatended.
- Days 3, 4 and 5 show a more normal but still very high listening behavior, with 14.0, 13.7 and 12.5 hours of music played, and this is most probably just because I binged on music that day.

### 6.2 One-Hit Wonder Score

**Result:**
| Artist        | Plays | Unique Tracks |   OHW Score   |
|---------------|-------|---------------|---------------|
| Sambomaster   |  271  |       1       |     271.0     |
| Marcus Miller |  240  |       1       |     240.0     |
| Peppermoth    |  239  |       1       |     239.0     |

**Interpretation:**
[Sambomaster's Seishun Kyousoukyoku opened the fascinating world of japanese rock to me with its appearance in Naruto, but I haven't explored other things by the artist since. On the other hand, Miller's For the Love of Freedom and Peppermoth's Carousel are obscure tracks that make me feel a deep sense of wonder and relaxation, and thus I return to them with consistency.]

### 6.3 Session Length Estimation

**Time Gap Distribution:**
- 0-5 min: [86.4]% (continuous listening)
- 5-30 min: [6.8]% (short breaks)
- 30+ min: [6.8]% (new sessions)

**Interpretation:**
[The new sessions treshold was put at 30 minutes because nearly 95% of the plays occur within that period. When a couple of songs are played further apart, it's most probably a different long session altogether.]

---

## 7. Data Quality Final Checks

### 7.1 Impossible Values

**Results:**
| Issue                  | Count |
|------------------------|-------|
| Negative play duration |   0   |
| Play duration >1 hour  |   3   |
| Future timestamps      |   0   |
| Very old timestamps    |   0   |

**Interpretation:**
[There doesn't seem to be anomalous data points. In the case of the longer play durations, it is most probably due to a very long track like a whole concert.]

### 7.2 Missing Critical Fields

**Results:**
- Records with no track name AND no episode name: [0] missing

**Interpretation:**
[The data is complete.]

---

## 8. Surprising Discoveries

1. [An unexpected discovery was that 72.3% of my songs begin playing with the shuffle mode on, which indicates a marked preference for newness and diversity.]
2. [A curious pattern is that, when I begin a music session, nearly 95% of the times I take none or very short breaks between songs, with only about 5% of contiguous song being played more than 30 minutes apart.]
3. [An interesting finding is that several rows are duplicated on the most important columns, which may generate distortions on the analytical tables if not corrected.]
4. [There's also a potential technical problem on days 7 and 8 of December 2020, because the total amount of listening hours are very high, with one day surpassing the 24 hour limit.]

---

## 9. Key Decisions for Analytical Queries

Based on exploratory findings:

✅ **Session Threshold:** 30 minutes  
✅ **Track Completion:** >2.5 minutes played  
✅ **Content Separation:** Just focus on music

---

## 10. Data Limitations & Caveats

- [Aside from the duplicated registers, there seems to be no other data problems.]