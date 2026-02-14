# 🎵 Spotify Streaming Analytics Portfolio

> Advanced data analytics project demonstrating BigQuery expertise, SQL window functions, and business intelligence with Tableau.

[![BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Tableau](https://img.shields.io/badge/Tableau-E97627?style=flat&logo=tableau&logoColor=white)](https://www.tableau.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white)](https://www.postgresql.org/)

## 📊 Project Overview

Analysis of Spotify streaming history to uncover listening patterns, content engagement drivers, and behavioral insights using advanced analytics techniques.

**Key Focus Areas:**
- 🎯 User engagement and retention metrics
- 📈 Temporal pattern analysis (hourly, daily, seasonal trends)
- 🎼 Content performance and discovery patterns
- 🔄 Session construction and listening behavior
- 📱 Platform and device usage analysis

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| **Data Storage** | Google Cloud Storage (GCS) |
| **Data Warehouse** | BigQuery |
| **Analytics** | SQL (Window Functions, CTEs, Subqueries) |
| **Scripting** | Python 3.x |
| **Visualization** | Tableau Desktop |
| **Version Control** | Git & GitHub |

## 📁 Project Structure
```
spotify-streaming-analytics/
├── sql/                              # BigQuery SQL queries
│   ├── 01_exploratory_analysis.sql   # Initial data exploration
│   └── 02_analytics_transformations.sql # Advanced transformations
├── python/                           # Data processing scripts
│   └── convert_to_jsonl.py          # JSON to JSONL converter
├── docs/                            # Documentation
│   ├── data_pipeline.md            # Data ingestion pipeline
│   └── technical_documentation.md   # Technical deep dive
├── tableau/                         # Tableau assets
│   └── screenshots/                # Dashboard images
├── presentation/                    # Executive presentations
└── README.md                       # This file
```

## 🚀 Quick Start

### Prerequisites
- Google Cloud Platform account
- BigQuery dataset access
- Tableau Desktop (for visualization)
- Python 3.8+

### Data Pipeline

1. **Data Ingestion**: Spotify JSON exports → Cloud Storage
2. **Transformation**: JSON arrays → JSONL format (newline-delimited)
3. **Loading**: JSONL → BigQuery table
4. **Analytics**: SQL transformations → analytical tables
5. **Visualization**: BigQuery → Tableau dashboards

See [Data Pipeline Documentation](docs/data_pipeline.md) for details.

## 📈 Key Insights & Metrics

*Coming soon - Analysis in progress*

### Sample Metrics Calculated:
- Session duration and continuity analysis
- Content completion rates and skip behavior
- Artist/track retention curves
- Discovery vs. repetition patterns
- Platform-specific engagement metrics

## 🎨 Tableau Dashboard

*Dashboard screenshots and Tableau Public link coming soon*

## 🔍 SQL Techniques Demonstrated

- **Window Functions**: ROW_NUMBER(), RANK(), LAG(), LEAD(), NTILE()
- **CTEs**: Multi-level Common Table Expressions for complex logic
- **Subqueries**: Scalar and multi-line subqueries for comparisons
- **Aggregations**: Advanced grouping with PARTITION BY
- **Date/Time Functions**: Temporal analysis and session construction
- **JSON Parsing**: Handling nested data structures

## 📚 Documentation

- [Data Pipeline](docs/data_pipeline.md) - Complete data flow from source to warehouse
- [Technical Documentation](docs/technical_documentation.md) - SQL queries and methodology

## 👤 Author

**[Tu Nombre]**
- LinkedIn: [Tu perfil]
- Portfolio: [Tu sitio web]
- Email: tu_email@example.com

---

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Spotify for providing personal data export functionality
- Google Cloud Platform for BigQuery and storage infrastructure
