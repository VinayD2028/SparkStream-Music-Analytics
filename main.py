"""
main.py — SparkStream Music Analytics Pipeline
================================================
Author: VinayD2028
Description:
    Core analytics pipeline built on Apache Spark Structured APIs.
    Processes music streaming logs and song metadata to extract
    behavioral insights, compute engagement metrics, and generate
    personalized mood-based song recommendations.

Modules:
    1. User Favorite Genre Detection
    2. Average Listen Time Per Song
    3. Top 10 Songs of the Week
    4. Happy Song Recommendations for Sad-music Listeners
    5. Genre Loyalty Score Computation
    6. Night Owl User Identification
    7. (Enriched logs join — see output/enriched_logs if extended)

Usage:
    spark-submit main.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, collect_list, desc, hour, max, rand,
    rank, row_number, sum
)
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────────────────
# Initialize SparkSession
# SparkSession is the entry point to all Spark functionality.
# "MusicAnalysis" is the application name visible in the Spark UI.
# ─────────────────────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# ─────────────────────────────────────────────────────────────────────────────
# Load Input Datasets
# Both CSVs are read with header=True (first row = column names)
# and inferSchema=True (Spark auto-detects column types).
# ─────────────────────────────────────────────────────────────────────────────
logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
# Schema: user_id (str), song_id (str), timestamp (timestamp), duration_sec (int)

metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)
# Schema: song_id (str), title (str), artist (str), genre (str), mood (str)


# ─────────────────────────────────────────────────────────────────────────────
# MODULE 1: User Favorite Genre Detection
# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Define a Window that partitions rows by user_id and orders within
#         each partition by play count descending.
#         This allows us to rank genres per user.
window = Window.partitionBy("user_id").orderBy(desc("count"))

(
    logs.join(metadata, "song_id")          # Inner join: enrich logs with genre info
        .groupBy("user_id", "genre")        # Count plays per (user, genre) pair
        .count()
        .withColumn("rank", rank().over(window))  # Assign rank 1 to most-played genre
        .filter(col("rank") == 1)           # Keep only the top genre per user
        .drop("rank")                       # Drop the helper rank column before writing
        .write.csv("output/user_favorite_genres", mode="overwrite")
)
# Output: user_id, genre, count  — one row per user showing their dominant genre


# ─────────────────────────────────────────────────────────────────────────────
# MODULE 2: Average Listen Time Per Song
# ─────────────────────────────────────────────────────────────────────────────
# Aggregates all streaming events for each song to compute how long
# listeners engage with it on average (in seconds).
# A higher average indicates stronger listener retention.
(
    logs.groupBy("song_id")
        .agg(avg("duration_sec").alias("avg_duration_sec"))
        .write.csv("output/avg_listen_time_per_song", mode="overwrite")
)
# Output: song_id, avg_duration_sec


# ─────────────────────────────────────────────────────────────────────────────
# MODULE 3: Top 10 Songs of the Week
# ─────────────────────────────────────────────────────────────────────────────
# Filters streaming activity to a specific week (March 24–28, 2025),
# counts plays per song, sorts descending, and takes the top 10.
# The date window is defined as [start, end) — inclusive start, exclusive end.
(
    logs.filter(
            (col("timestamp") >= "2025-03-24") &
            (col("timestamp") < "2025-03-29")
        )
        .groupBy("song_id")
        .count()
        .orderBy(desc("count"))             # Highest play count first
        .limit(10)                          # Take only the top 10 songs
        .write.csv("output/top_songs_this_week", mode="overwrite")
)
# Output: song_id, count  — top 10 most-played songs in the specified week


# ─────────────────────────────────────────────────────────────────────────────
# MODULE 4: Mood-Based Song Recommendations
# Strategy: Find users who primarily listen to "Sad" songs, then recommend
#           up to 3 "Happy" songs they have never played before.
# ─────────────────────────────────────────────────────────────────────────────

# Step A: Identify users whose dominant mood is "Sad"
# We aggregate play counts per (user, mood), then pick the max mood label.
# Note: Using max() here selects the alphabetically largest mood string —
# this works as a tie-breaker approximation; a rank() approach is more precise.
sad_users = (
    logs.join(metadata, "song_id")
        .groupBy("user_id", "mood")
        .count()
        .groupBy("user_id")
        .agg(max("mood").alias("primary_mood"))
        .filter(col("primary_mood") == "Sad")
        .select(col("user_id").alias("sad_user_id"))
)
# Result: sad_user_id — list of users whose primary listening mood is Sad

# Step B: Collect songs already played by sad users (to exclude from recommendations)
played_by_sad = (
    logs.join(sad_users, logs.user_id == sad_users.sad_user_id)
        .select("sad_user_id", "song_id")   # Only keep user and song identifiers
)
# Result: (sad_user_id, song_id) — songs already heard by sad users

# Step C: Get the pool of all "Happy" mood songs from the catalog
happy_songs = (
    metadata.filter(col("mood") == "Happy")
            .select("song_id", "title")
)
# Result: happy songs available for recommendation

# Step D: Cross-join every sad user with every happy song, then use a left anti-join
#         to remove songs they have already listened to.
#         Assign a random row number per user and keep only the top 3.
#         This produces diverse, non-redundant recommendations.
recommendations = (
    sad_users.crossJoin(happy_songs)                       # All (user, happy_song) pairs
             .join(played_by_sad,
                   ["sad_user_id", "song_id"],
                   "left_anti")                            # Exclude already-played songs
             .withColumn("rn", row_number().over(
                 Window.partitionBy("sad_user_id").orderBy(rand())  # Randomize order
             ))
             .filter(col("rn") <= 3)                       # Keep only top 3 per user
             .groupBy("sad_user_id")
             .agg(collect_list("title").alias("recommended_songs"))  # Bundle into list
)

# Write recommendations as JSON for structured, nested output
recommendations.write.json("output/happy_recommendations", mode="overwrite")
# Output: { "sad_user_id": "user_X", "recommended_songs": ["Title_A", "Title_B", ...] }


# ─────────────────────────────────────────────────────────────────────────────
# MODULE 5: Genre Loyalty Score
# A loyalty score = (plays in top genre) / (total plays for that user)
# Score of 1.0 means the user exclusively listens to one genre.
# We filter for users with loyalty >= 0.8 — highly genre-focused listeners.
# ─────────────────────────────────────────────────────────────────────────────

# Step A: Count plays per (user, genre) combination
user_genre_counts = (
    logs.join(metadata, "song_id")
        .groupBy("user_id", "genre")
        .count()
)
# Result: user_id, genre, count

# Step B: Compute the total number of plays per user across all genres
total_plays = (
    user_genre_counts.groupBy("user_id")
                     .agg(sum("count").alias("total"))
)
# Result: user_id, total

# Step C: Compute loyalty score = genre_count / total_plays
#         Filter to users where loyalty score >= 0.8 (genre-loyal listeners)
loyalty_scores = (
    user_genre_counts.join(total_plays, "user_id")
                     .withColumn("loyalty_score", col("count") / col("total"))
                     .filter(col("loyalty_score") >= 0.8)
                     .write.csv("output/genre_loyalty_scores", mode="overwrite")
)
# Output: user_id, genre, count, total, loyalty_score  — only highly loyal users


# ─────────────────────────────────────────────────────────────────────────────
# MODULE 6: Night Owl User Identification
# Finds users who frequently listen to music between 12 AM and 5 AM.
# The hour() function extracts the hour component from the timestamp column.
# A threshold of >5 sessions filters out occasional late-night listeners,
# retaining only habitual night-time streamers.
# ─────────────────────────────────────────────────────────────────────────────
(
    logs.filter(hour("timestamp").between(0, 5))  # Keep only late-night sessions
        .groupBy("user_id")
        .count()
        .filter(col("count") > 5)                 # Minimum 5 late-night sessions
        .write.csv("output/night_owl_users", mode="overwrite")
)
# Output: user_id, count  — users with more than 5 late-night listening sessions


# ─────────────────────────────────────────────────────────────────────────────
# Shut down the Spark session to release cluster resources
# ─────────────────────────────────────────────────────────────────────────────
spark.stop()
