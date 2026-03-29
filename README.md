# 🎵 SparkStream Music Analytics

> **An end-to-end big data pipeline for analyzing music streaming behavior at scale using Apache Spark Structured APIs.**

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.x-orange?logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-Structured%20APIs-red)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 🚀 Project Overview

**SparkStream Music Analytics** is a production-style data engineering project that processes large-scale music streaming logs to surface actionable insights. Built entirely with **Apache Spark Structured APIs (PySpark)**, this project demonstrates real-world skills in distributed data processing, window functions, join strategies, and behavioral analytics — the kind of work done at companies like Spotify, Apple Music, and Netflix.

The pipeline ingests raw listening activity and song metadata, then runs seven distinct analytical modules ranging from personalized recommendations to loyalty scoring and night-time engagement tracking.

---

## 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| **Python 3.8+** | Primary programming language |
| **Apache Spark (PySpark)** | Distributed data processing engine |
| **Spark Structured APIs** | DataFrame & SQL-style transformations |
| **Spark Window Functions** | Ranking, partitioning, and ordering |
| **Pandas** | Synthetic dataset generation |
| **CSV / JSON** | Input/output data formats |

---

## 📁 Project Structure

```
SparkStream-Music-Analytics/
├── datagen.py              # Synthetic data generator (100 users, 50 songs, 1000 logs)
├── main.py                 # Core Spark analytics pipeline (7 analysis modules)
├── listening_logs.csv      # Generated user listening activity dataset
├── songs_metadata.csv      # Generated song catalog with genres and moods
├── Requirements            # Python dependencies
└── output/                 # Analysis results (auto-generated)
    ├── user_favorite_genres/
    ├── avg_listen_time_per_song/
    ├── top_songs_this_week/
    ├── happy_recommendations/
    ├── genre_loyalty_scores/
    └── night_owl_users/
```

---

## 📊 Dataset Schema

### listening_logs.csv — User Streaming Activity
| Column | Type | Description |
|---|---|---|
| `user_id` | String | Unique user identifier (e.g., user_42) |
| `song_id` | String | Unique song identifier (e.g., song_17) |
| `timestamp` | Timestamp | Datetime of the streaming event |
| `duration_sec` | Integer | Number of seconds the song was played (30–300s) |

### songs_metadata.csv — Song Catalog
| Column | Type | Description |
|---|---|---|
| `song_id` | String | Unique song identifier |
| `title` | String | Song title |
| `artist` | String | Artist name |
| `genre` | String | Genre (Pop, Rock, Jazz, Classical, Hip-Hop) |
| `mood` | String | Mood tag (Happy, Sad, Energetic, Chill) |

---

## 🔍 Analytics Modules

### 1. 🎯 User Favorite Genre Detection
Joins listening logs with song metadata and uses **Spark Window Functions** (rank over partition) to identify each user's most-played genre. Outputs one row per user with their dominant genre.

### 2. ⏱️ Average Listen Time Per Song
Aggregates all streaming events per song to compute the **mean playback duration**, giving insight into which songs hold listener attention the longest.

### 3. 🔥 Top 10 Songs of the Week
Filters streaming events within a specific date window and ranks songs by **total play count**, identifying the week's trending tracks.

### 4. 💡 Mood-Based Song Recommendations
Implements a **content-based recommendation engine** using Spark anti-joins and cross-joins:
- Identifies users whose primary listening mood is "Sad"
- Finds "Happy" mood songs they have **never played before**
- Recommends up to **3 personalized Happy songs** per user using random sampling via a window row_number

### 5. 🏆 Genre Loyalty Score
Computes a **loyalty score** (0.0–1.0) for each user representing the proportion of their total listens in their favorite genre. Filters for users scoring **≥ 0.8**, identifying the platform's most genre-loyal listeners.

### 6. 🌙 Night Owl User Identification
Extracts users who actively stream music **between midnight and 5 AM**, applying a minimum play count threshold of 5 sessions to filter out noise. Useful for segmenting late-night listeners for targeted campaigns.

### 7. 📋 Enriched Listening Logs
Performs a **broadcast-friendly join** between listening logs and song metadata to produce a fully enriched dataset combining behavioral and descriptive attributes for downstream ML or BI use.

---

## ⚙️ Getting Started

### Prerequisites

- Python 3.8+
- Java 8 or 11 (required by Spark)
- Apache Spark 3.x

```bash
# Install Python dependencies
pip install pyspark pandas
```

### Run the Pipeline

```bash
# Step 1: Generate synthetic datasets
python datagen.py

# Step 2: Run the full Spark analytics pipeline
spark-submit main.py

# Step 3: Inspect the outputs
ls output/
```

---

## 💡 Key Engineering Concepts Demonstrated

- **Spark Structured API** — DataFrame transformations, filters, joins, aggregations
- **Window Functions** — `rank()`, `row_number()`, `partitionBy()`, `orderBy()`
- **Join Strategies** — inner joins, left anti-joins, cross-joins for recommendation logic
- **Aggregations** — `groupBy`, `agg`, `collect_list`, `avg`, `sum`, `max`
- **Time-Based Filtering** — timestamp parsing, `hour()` extraction, date range filters
- **Scalable Output** — partitioned CSV/JSON writes with overwrite semantics
- **Synthetic Data Generation** — reproducible data pipelines with `random.seed`

---

## 📈 Sample Insights Produced

| Analysis | Output |
|---|---|
| Favorite genres | 1 row per user with their top genre |
| Average listen time | Per-song average playback duration in seconds |
| Weekly trending | Top 10 most-streamed songs in the target week |
| Recommendations | Up to 3 Happy songs per Sad-music listener |
| Genre loyalty | Users with ≥ 80% listens in a single genre |
| Night owls | Users with 5+ late-night (12AM–5AM) sessions |

---

## 🤝 Contributing

Pull requests are welcome. For significant changes, please open an issue first to discuss the proposed modification.

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).
