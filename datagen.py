"""
datagen.py — Synthetic Dataset Generator
==========================================
Author: VinayD2028
Description:
    Generates two reproducible CSV datasets that simulate a music
    streaming platform's data warehouse:

      1. listening_logs.csv  — User streaming activity events
      2. songs_metadata.csv  — Song catalog with genre and mood tags

    A fixed random seed (42) ensures that re-running this script
    always produces the same datasets, making results deterministic
    and reproducible across environments.

Output Files:
    listening_logs.csv   — 1,000 streaming events across 100 users
    songs_metadata.csv   — 50 songs with genre and mood attributes

Usage:
    python datagen.py
"""

import pandas as pd
import random
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# Configuration Parameters
# Adjust these to scale the dataset up or down as needed.
# ─────────────────────────────────────────────────────────────────────────────
random.seed(42)       # Fix random seed for reproducibility

num_users = 100       # Total number of simulated users on the platform
num_songs = 50        # Total number of songs in the catalog
num_logs  = 1000      # Total number of streaming events to generate

# Generate sequential user and song identifiers
# e.g., ["user_1", "user_2", ..., "user_100"]
user_ids = [f'user_{i}' for i in range(1, num_users + 1)]

# e.g., ["song_1", "song_2", ..., "song_50"]
song_ids = [f'song_{i}' for i in range(1, num_songs + 1)]


# ─────────────────────────────────────────────────────────────────────────────
# Generate: listening_logs.csv
# Each row represents a single streaming event where a user played a song.
# ─────────────────────────────────────────────────────────────────────────────
logs = []

# Define the date range for all streaming events (full month of March 2025)
start_date = datetime(2025, 3, 1)
end_date   = datetime(2025, 3, 28)

for _ in range(num_logs):
    # Randomly pick a user and a song for this streaming event
    user = random.choice(user_ids)
    song = random.choice(song_ids)

    # Generate a random timestamp within [start_date, end_date]
    # by offsetting the start date by a random number of seconds
    total_seconds = int((end_date - start_date).total_seconds())
    random_offset = timedelta(seconds=random.randint(0, total_seconds))
    timestamp = (start_date + random_offset).strftime('%Y-%m-%d %H:%M:%S')

    # Simulate how long the user played the song (30 to 300 seconds)
    # 30s ≈ partial listen; 300s ≈ full song
    duration_sec = random.randint(30, 300)

    logs.append([user, song, timestamp, duration_sec])

# Write the listening logs to CSV
pd.DataFrame(logs, columns=['user_id', 'song_id', 'timestamp', 'duration_sec']) \
  .to_csv('listening_logs.csv', index=False)

print(f"Generated listening_logs.csv: {num_logs} rows")


# ─────────────────────────────────────────────────────────────────────────────
# Generate: songs_metadata.csv
# Each row describes a song in the catalog with genre and mood attributes.
# ─────────────────────────────────────────────────────────────────────────────

# Supported genres and mood tags for classification
genres = ['Pop', 'Rock', 'Jazz', 'Classical', 'Hip-Hop']
moods  = ['Happy', 'Sad', 'Energetic', 'Chill']

metadata = []

for song_id in song_ids:
    # Assign a human-readable title derived from the song_id
    title = f'Title_{song_id}'

    # Assign a random artist from a pool of 20 fictional artists
    artist = f'Artist_{random.randint(1, 20)}'

    # Randomly assign genre and mood to each song
    genre = random.choice(genres)
    mood  = random.choice(moods)

    metadata.append([song_id, title, artist, genre, mood])

# Write the song metadata to CSV
pd.DataFrame(metadata, columns=['song_id', 'title', 'artist', 'genre', 'mood']) \
  .to_csv('songs_metadata.csv', index=False)

print(f"Generated songs_metadata.csv: {num_songs} songs")
print("Dataset generation complete. Ready to run main.py.")
