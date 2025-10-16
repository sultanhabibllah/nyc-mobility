import os
import sqlite3
from flask import Flask, jsonify, request, render_template
from dotenv import load_dotenv

# Load .env
load_dotenv()

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/db/nyc.db")
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "0")

def get_db_path():
    if DATABASE_URL.startswith("sqlite:///"):
        return DATABASE_URL.replace("sqlite:///", "", 1)
    return "data/db/nyc.db"

# ----------------- DB Helpers ----------------- #
def query_db(sql, params=()):
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows

def execute_db(sql, params=()):
    conn = sqlite3.connect(get_db_path())
    cur = conn.cursor()
    cur.execute(sql, params)
    conn.commit()
    conn.close()

# ----------------- Init (create table & indexes if missing) ----------------- #
def init_db():
    os.makedirs("data/db", exist_ok=True)
    conn = sqlite3.connect(get_db_path())
    cur = conn.cursor()
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
    # Helpful indexes for queries you run on the dashboard
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pickup_datetime ON taxi_trips (pickup_datetime)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_duration_category ON taxi_trips (duration_category)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_speed ON taxi_trips (trip_speed_kmh)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rush ON taxi_trips (rush_hour_flag)")
    conn.commit()
    conn.close()
    print("✅ DB ready with indexes.")

with app.app_context():
    init_db()

# ----------------- Routes ----------------- #
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/health")
def health():
    db_path = get_db_path()
    return jsonify({
        "status": "ok",
        "database_url": DATABASE_URL,
        "sqlite_file_path": db_path,
        "sqlite_file_exists": os.path.exists(db_path)
    })

# ---------- Summary KPIs ---------- #
@app.route("/api/summary")
def api_summary():
    start = request.args.get("start")
    end = request.args.get("end")
    where = []
    params = []
    if start:
        where.append("date(substr(pickup_datetime,1,19)) >= date(?)")
        params.append(start)
    if end:
        where.append("date(substr(pickup_datetime,1,19)) <= date(?)")
        params.append(end)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT
            COUNT(*) AS trips,
            AVG(trip_duration) AS avg_duration_s,
            AVG(trip_distance_km) AS avg_km,
            AVG(trip_speed_kmh) AS avg_kmh
        FROM taxi_trips
        {where_sql}
    """
    rows = query_db(sql, tuple(params))
    return jsonify(dict(rows[0]) if rows else {})

# ---------- Busiest hours (custom selection sort) ---------- #
def selection_sort_desc(pairs):
    # Manual algorithm to satisfy “implement an algorithm yourself”
    arr = pairs[:]
    n = len(arr)
    for i in range(n):
        max_idx = i
        for j in range(i + 1, n):
            if arr[j][1] > arr[max_idx][1]:
                max_idx = j
        if max_idx != i:
            arr[i], arr[max_idx] = arr[max_idx], arr[i]
    return arr

@app.route("/api/busiest_hours")
def api_busiest_hours():
    k = int(request.args.get("k", "5"))
    where, params = [], []
    start = request.args.get("start")
    end = request.args.get("end")
    if start:
        where.append("date(substr(pickup_datetime,1,19)) >= date(?)")
        params.append(start)
    if end:
        where.append("date(substr(pickup_datetime,1,19)) <= date(?)")
        params.append(end)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT CAST(strftime('%H', substr(pickup_datetime,1,19)) AS INTEGER) AS hour,
               COUNT(*) AS trips
        FROM taxi_trips
        {where_sql}
        GROUP BY hour
    """
    rows = query_db(sql, tuple(params))
    pairs = [(int(r["hour"]), int(r["trips"])) for r in rows if r["hour"] is not None]
    sorted_pairs = selection_sort_desc(pairs)
    return jsonify({"top": [{"hour": h, "trips": c} for h, c in sorted_pairs[:k]]})

# ---------- Distribution by duration category ---------- #
@app.route("/api/distribution")
def api_distribution():
    # Optional filters: start, end, rush (0/1), min_passengers, max_passengers
    start = request.args.get("start")
    end = request.args.get("end")
    rush = request.args.get("rush")  # "0" or "1"
    minp = request.args.get("min_passengers")
    maxp = request.args.get("max_passengers")

    where, params = [], []
    if start:
        where.append("date(substr(pickup_datetime,1,19)) >= date(?)")
        params.append(start)
    if end:
        where.append("date(substr(pickup_datetime,1,19)) <= date(?)")
        params.append(end)
    if rush in ("0","1"):
        where.append("rush_hour_flag = ?")
        params.append(int(rush))
    if minp:
        where.append("passenger_count >= ?")
        params.append(int(minp))
    if maxp:
        where.append("passenger_count <= ?")
        params.append(int(maxp))

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT duration_category, COUNT(*) AS trips
        FROM taxi_trips
        {where_sql}
        GROUP BY duration_category
    """
    rows = query_db(sql, tuple(params))
    return jsonify({r["duration_category"]: r["trips"] for r in rows})

# ---------- Speed histogram (manual bucketing) ---------- #
def manual_hist(values, bin_size):
    """
    Build a histogram without numpy/pandas helpers.
    Returns list of (bin_label, count), where label is 'low-high'.
    """
    buckets = {}
    for v in values:
        if v is None:
            continue
        try:
            x = float(v)
        except (TypeError, ValueError):
            continue
        if x < 0:
            continue
        low = int(x // bin_size) * bin_size
        high = low + bin_size
        label = f"{low}-{high}"
        buckets[label] = buckets.get(label, 0) + 1

    # Convert to sorted list by numeric low bound (manual insertion sort)
    items = list(buckets.items())
    for i in range(1, len(items)):
        key_item = items[i]
        j = i - 1
        while j >= 0 and int(items[j][0].split('-')[0]) > int(key_item[0].split('-')[0]):
            items[j+1] = items[j]
            j -= 1
        items[j+1] = key_item
    return items

@app.route("/api/speeds_hist")
def api_speeds_hist():
    # Optional: start, end; bin_size default 5 km/h
    start = request.args.get("start")
    end = request.args.get("end")
    bin_size = int(request.args.get("bin_size", "5"))

    where, params = [], []
    if start:
        where.append("date(substr(pickup_datetime,1,19)) >= date(?)")
        params.append(start)
    if end:
        where.append("date(substr(pickup_datetime,1,19)) <= date(?)")
        params.append(end)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"SELECT trip_speed_kmh FROM taxi_trips {where_sql}"
    rows = query_db(sql, tuple(params))
    values = [row["trip_speed_kmh"] for row in rows]

    hist = manual_hist(values, bin_size)
    return jsonify({"bins": [{"label": k, "count": v} for k, v in hist]})

if __name__ == "__main__":
    app.run(debug=(FLASK_DEBUG == "1"))
