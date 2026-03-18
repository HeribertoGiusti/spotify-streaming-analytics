# 🎵 Spotify Streaming Analytics Portfolio

> Advanced data analytics project combining Python scripting, BigQuery transformation and querying techniques, and Tableau dynamic visualizations, for replicating a professional scenario were a data pipeline needs to be constructed in order to generate some business intelligence.

[![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Tableau](https://img.shields.io/badge/Tableau-E97627?style=flat&logo=tableau&logoColor=white)](https://www.tableau.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)

## 📊 Project Overview

Analysis of my Spotify streaming history to uncover listening patterns and behavioral insights using advanced SQL techniques.

**Key Focus Areas:**
- 🎯 User engagement and retention metrics
- 📈 Temporal pattern analysis (hourly, daily, seasonal trends)
- 🎼 Content performance and discovery patterns
- 🔄 Session construction and listening behavior

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| **Data Storage** | Google Cloud Storage (GCS) |
| **Data Warehouse** | BigQuery |
| **Analytics** | SQL (Window Functions, CTEs, Subqueries) |
| **Scripting** | Python 3.x |
| **Visualization** | Tableau |
| **Version Control** | GitHub |

## 📁 Project Structure
```
spotify-streaming-analytics/
├── sql/                                     # BigQuery SQL queries
│   ├── 01_exploratory_analysis_bronze.sql     # Initial data exploration (EDA)
|   ├── 02_data_cleaning_silver.sql            # Creation of clean table
|   ├── 03_sessions_construction_gold.sql      # Creation of sessions table
│   └── 04_analytical_views_gold.sql           # Advanced transformations
├── python/                                  # Data processing script
│   └── convert_to_jsonl.py                    # JSON to JSONL converter
├── docs/                                    # Documentation
│   ├── data_pipeline.md                       # Data ingestion pipeline
│   ├── exploratory_findings.md                # Results from SQL EDA
|   └── problems_session_construction.md       # Logs for problems resolution
└── README.md                                # This file
```

## 🚀 Quick Start

### Prerequisites
- Google Cloud Platform account
- BigQuery dataset access
- Tableau Public
- Python 3.8+

### Data Pipeline

1. **Data Ingestion**: Spotify JSON exports → Cloud Storage
2. **Transformation**: JSON arrays → JSONL format
3. **Loading**: JSONL → BigQuery table
4. **Analytics**: SQL transformations → Analytical tables
5. **Visualization**: BigQuery → Tableau dashboards

See [Data Pipeline Documentation](docs/data_pipeline.md) for details.

## 🎨 Tableau Dashboard

[alt text](imagen.png)

## 🔍 SQL Techniques Demonstrated

- **Window Functions**: ROW_NUMBER(), RANK(), LAG(), LEAD(), NTILE()
- **CTEs**: Multi-level Common Table Expressions for complex logic
- **Subqueries**: Scalar and multi-line subqueries for comparisons
- **Aggregations**: Advanced grouping with PARTITION BY
- **Date/Time Functions**: Temporal analysis and session construction
- **JSON Parsing**: Handling nested data structures

## 👤 Author

**Heriberto Giusti**
- LinkedIn: https://www.linkedin.com/in/heriberto-giusti-4389b2298/
- Email: heribertogiusti@gmail.com

---

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Spotify for providing personal data export functionality
- Google Cloud Platform for BigQuery and storage infrastructure
- Tableau Public for enabling their free platform
