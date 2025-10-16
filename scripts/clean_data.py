import os
import math
import sqlite3
from datetime import time
import pandas as pd

CSV_PATH = "data/raw/train_unzipped/train.csv"
DB_PATH = "data/db/nyc.db"
LOG_PATH = "logs/cleaning.log"
CHUNK_SIZE = 100_000  # adjust if memory is tight

# ------------------- Cleaning ------------------- #

def clean_and_validate(df: pd.DataFrame) -> pd.DataFrame:
    """I clean and validate the raw taxi data before I generate features."""
    # 1) I drop rows with missing essential fields
    essential_cols = [
        "pickup_datetime", "dropoff_datetime",
        "pickup_longitude", "pickup_latitude",
        "dropoff_longitude", "dropoff_latitude", "trip_duration"
    ]
    df = df.dropna(subset=essential_cols)

    # 2) I remove duplicate trip IDs (if any)
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])

    # 3) I remove invalid coordinates (rough NYC bbox)
    df = df[
        (df["pickup_latitude"].between(40, 41)) &
        (df["dropoff_latitude"].between(40, 41)) &
        (df["pickup_longitude"].between(-75, -72)) &
        (df["dropoff_longitude"].between(-75, -72))
    ]

    # 4) I remove non-positive durations
    df = df[df["trip_duration"] > 0]

    return df

# ------------------- Feature Engineering ------------------- #

def to_datetime_safe(series):
    """I convert strings to pandas datetimes; invalid rows become NaT (not-a-time)."""
    return pd.to_datetime(series, errors="coerce", infer_datetime_format=True, utc=False)

def haversine_km(lat1, lon1, lat2, lon2):
    """I compute great-circle distance in kilometers using the Haversine formula."""
    R = 6371.0088  # mean Earth radius in km
    # convert degrees to radians
    lat1 = math.radians(lat1); lon1 = math.radians(lon1)
    lat2 = math.radians(lat2); lon2 = math.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (math.sin(dlat / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def compute_distance_series(df):
    """I compute Haversine distance for each row."""
    return df.apply(
        lambda r: haversine_km(
            r["pickup_latitude"], r["pickup_longitude"],
            r["dropoff_latitude"], r["dropoff_longitude"]
        ),
        axis=1
    )

def categorize_duration(seconds):
    """I bucket the trip duration into short/medium/long for simple analysis."""
    if seconds <= 300:     # ≤ 5 minutes
        return "short"
    if seconds <= 1200:    # 5–20 minutes
        return "medium"
    return "long"          # > 20 minutes

def is_rush_hour(dt):
    """I flag NYC-like rush hours: 7–9 AM and 5–7 PM local time."""
    if pd.isna(dt):
        return 0
    t = dt.time()
    morning = time(7, 0) <= t <= time(9, 0)
    evening = time(17, 0) <= t <= time(19, 0)
    return int(morning or evening)

def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """I add distance, speed, duration_category, and rush_hour_flag; I also log anomalies."""
    # 1) parse timestamps
    df["pickup_dt"] = to_datetime_safe(df["pickup_datetime"])
    df["dropoff_dt"] = to_datetime_safe(df["dropoff_datetime"])

    # 2) drop rows where datetimes failed to parse or are inverted
    before = len(df)
    bad_time = df[df["pickup_dt"].isna() | df["dropoff_dt"].isna() | (df["dropoff_dt"] < df["pickup_dt"])]
    if not bad_time.empty:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[TIME] Excluding {len(bad_time)} rows with invalid/inverted timestamps\n")
        df = df.drop(bad_time.index)

    # 3) compute distance (km)
    df["trip_distance_km"] = compute_distance_series(df)

    # 4) speed (km/h); protect against division by zero
    df["trip_speed_kmh"] = (df["trip_distance_km"] / df["trip_duration"].replace(0, pd.NA)) * 3600

    # 5) duration category
    df["duration_category"] = df["trip_duration"].apply(categorize_duration)

    # 6) rush hour flag from pickup time
    df["rush_hour_flag"] = df["pickup_dt"].apply(is_rush_hour)

    # 7) log suspicious values (extreme speeds or distances)
    suspicious = df[(df["trip_speed_kmh"] > 120) | (df["trip_distance_km"] > 100)]
    if not suspicious.empty:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[ANOMALY] {len(suspicious)} rows with speed>120 km/h or distance>100 km\n")

    after = len(df)
    dropped = before - after
    if dropped > 0:
        print(f"⏱️  Parsed timestamps & added features. Dropped {dropped} rows due to invalid time order.")
    else:
        print("⏱️  Parsed timestamps & added features. No time-order drops in this chunk.")
    return df

# ------------------- DB Insert ------------------- #

COLUMNS = [
    "id","vendor_id","pickup_datetime","dropoff_datetime","passenger_count",
    "pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude",
    "store_and_fwd_flag","trip_duration","trip_distance_km","trip_speed_kmh",
    "duration_category","rush_hour_flag"
]

def insert_chunk_to_db(df: pd.DataFrame):
    """I insert a cleaned+enriched chunk into SQLite, skipping duplicates."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Ensure table exists (defensive)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS taxi_trips (
            id TEXT PRIMARY KEY,
            vendor_id TEXT,
            pickup_datetime TEXT,
            dropoff_datetime TEXT,
            passenger_count INTEGER,
            pickup_longitude REAL,
            pickup_latitude REAL,
            dropoff_longitude REAL,
            dropoff_latitude REAL,
            store_and_fwd_flag TEXT,
            trip_duration INTEGER,
            trip_distance_km REAL,
            trip_speed_kmh REAL,
            duration_category TEXT,
            rush_hour_flag INTEGER
        )
    """)

    # Convert NaN to None for SQLite
    records = []
    for _, r in df[COLUMNS].iterrows():
        rec = []
        for v in r.tolist():
            if isinstance(v, float) and (pd.isna(v)):  # NaN
                rec.append(None)
            else:
                rec.append(v)
        records.append(tuple(rec))

    cur.executemany(
        """
        INSERT OR IGNORE INTO taxi_trips (
            id, vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
            pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
            store_and_fwd_flag, trip_duration, trip_distance_km, trip_speed_kmh,
            duration_category, rush_hour_flag
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        records
    )
    conn.commit()
    inserted = cur.rowcount  # NOTE: sqlite returns -1 for executemany; we’ll just print length
    conn.close()
    print(f"📥 Inserted {len(records)} rows (duplicates ignored).")

# ------------------- Runner ------------------- #

def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found at {CSV_PATH}")
    print("✅ Found my dataset at:", CSV_PATH)

    # Stream the full CSV in chunks so I don't blow up memory
    total_rows = 0
    chunk_idx = 0

    for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
        chunk_idx += 1
        print(f"\n--- Processing chunk {chunk_idx} (rows: {len(chunk)}) ---")

        cleaned = clean_and_validate(chunk)
        print(f"✅ After validation: {len(cleaned)} rows")

        if len(cleaned) == 0:
            print("ℹ️  Nothing to insert from this chunk (all invalid).")
            continue

        enriched = add_features(cleaned)

        insert_chunk_to_db(enriched)

        total_rows += len(enriched)
        print(f"✅ Running total inserted: {total_rows}")

    print(f"\n🎉 Done. Total rows inserted into SQLite: {total_rows}")
    print(f"🧾 Logs (if any): {LOG_PATH}")

if __name__ == "__main__":
    main()