import pandas as pd
import sqlite3
import json

# 1. Load parquet
df = pd.read_parquet("./dump_db/events_20201101.parquet")

# 2. Convert problematic columns to JSON strings
for col in df.columns:
    if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

# 3. Save to SQLite
conn = sqlite3.connect("events.db")
df.to_sql("events_20201101", conn, if_exists="replace", index=False)

# 4. Dump to .sql
with open("./dump_SQL/events_20201101.sql", "w") as f:
    for line in conn.iterdump():
        f.write(f"{line}\n")

conn.close()
