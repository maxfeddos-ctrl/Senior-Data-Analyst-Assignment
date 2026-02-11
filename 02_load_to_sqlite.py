"""
Load CSVs into SQLite database
Just a quick loader script to get the raw files into a db
"""

import sqlite3
import pandas as pd
import os

DB = '2_SQL_DATABASE/timedoctor_analytics.db'
RAW = 'data/raw'

os.makedirs('2_SQL_DATABASE', exist_ok=True)

print("\nLoading CSV files into SQLite...")
print(f"Database: {DB}\n")

conn = sqlite3.connect(DB)

# drop old tables if they exist
print("Cleaning up old tables...")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS raw_fact_activity_sessions")
cursor.execute("DROP TABLE IF EXISTS raw_dim_users")
cursor.execute("DROP TABLE IF EXISTS raw_dim_accounts")
cursor.execute("DROP TABLE IF EXISTS raw_dim_applications")
cursor.execute("DROP TABLE IF EXISTS raw_dim_projects")
cursor.execute("DROP TABLE IF EXISTS raw_dim_tasks")
conn.commit()

# load each csv
files_to_load = {
    'raw_dim_accounts': 'dim_accounts.csv',
    'raw_dim_users': 'dim_users.csv',
    'raw_dim_applications': 'dim_applications.csv',
    'raw_dim_projects': 'dim_projects.csv',
    'raw_dim_tasks': 'dim_tasks.csv',
    'raw_fact_activity_sessions': 'fact_activity_sessions.csv'
}

for table, csvfile in files_to_load.items():
    path = f'{RAW}/{csvfile}'
    
    if not os.path.exists(path):
        print(f"  WARNING: {csvfile} not found, skipping")
        continue
    
    df = pd.read_csv(path)
    df.to_sql(table, conn, if_exists='replace', index=False)
    print(f"  {table}: {len(df):,} rows")

conn.commit()

# check what we got
print("\nVerifying...")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()

for t in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {t[0]}")
    cnt = cursor.fetchone()[0]
    print(f"  {t[0]}: {cnt:,} rows")

conn.close()

print("\nDone! Database ready at:")
print(f"  {DB}")
print("\nNext: Run the SQL transformation scripts in DBeaver")
print("  - 04_transform_to_staging.sql")
print("  - 05_transform_to_analytics.sql\n")
