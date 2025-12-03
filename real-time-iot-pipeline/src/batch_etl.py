import pandas as pd
import os

# === File paths ===
RAW_FILE = "data/sensor_data.csv"
PROCESSED_FILE = "data/processed_data.csv"

# === Extract ===
def extract_data(file_path):"""
iot_dw_etl.py

Single-file Python ETL script that:
- Reads new records from an existing source table (default: WeatherStation_Pro)
- Creates Data Warehouse tables (DimDate, DimDevice, DimLocation, DimAlert, FactWeather)
- Loads new rows incrementally using ETL_Control.LastLoadedTimestamp

Requirements:
- Python 3.8+
- pyodbc
- Install: pip install pyodbc

Usage:
- Edit SQL_CONN_STR and SOURCE_TABLE below to match your environment.
- Run periodically (SQL Agent/cron) or manually.
"""

import pyodbc
from datetime import datetime, date
import sys

# ----------------------------
# CONFIG - update for your env
# ----------------------------
SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=team4iotproject.database.windows.net;"
    "Database=Depi_Project1;"
    "Uid=team4iotproject;"
    "Pwd=team4444#;"
)
SOURCE_TABLE = "WeatherStation_Pro"     # <- the existing table you said you have
ETL_CONTROL_TABLE = "ETL_Control"       # control table to track incremental loads

# DW table names
DIM_DATE = "DimDate"
DIM_DEVICE = "DimDevice"
DIM_LOCATION = "DimLocation"
DIM_ALERT = "DimAlert"
FACT_WEATHER = "FactWeather"

# ----------------------------
# Helper SQL templates
# ----------------------------
CREATE_CONTROL_SQL = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{ETL_CONTROL_TABLE}') AND type in (N'U'))
BEGIN
    CREATE TABLE {ETL_CONTROL_TABLE} (
        ControlID INT IDENTITY(1,1) PRIMARY KEY,
        SourceTable NVARCHAR(200) UNIQUE,
        LastLoadedTimestamp DATETIME NULL,
        LastRun DATETIME NULL
    );
    INSERT INTO {ETL_CONTROL_TABLE} (SourceTable, LastLoadedTimestamp, LastRun) VALUES ('{SOURCE_TABLE}', NULL, NULL);
END
"""

CREATE_DIM_DATE_SQL = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{DIM_DATE}') AND type in (N'U'))
BEGIN
    CREATE TABLE {DIM_DATE} (
        DateKey INT PRIMARY KEY,      -- YYYYMMDD
        FullDate DATE NOT NULL,
        YearInt INT,
        MonthInt INT,
        DayInt INT,
        WeekOfYear INT,
        QuarterInt INT
    );
END
"""

CREATE_DIM_DEVICE_SQL = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{DIM_DEVICE}') AND type in (N'U'))
BEGIN
    CREATE TABLE {DIM_DEVICE} (
        DeviceKey INT IDENTITY(1,1) PRIMARY KEY,
        DeviceID NVARCHAR(100),
        DeviceType NVARCHAR(100),
        CONSTRAINT UQ_{DIM_DEVICE}_Device UNIQUE (DeviceID, DeviceType)
    );
END
"""

CREATE_DIM_LOCATION_SQL = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{DIM_LOCATION}') AND type in (N'U'))
BEGIN
    CREATE TABLE {DIM_LOCATION} (
        LocationKey INT IDENTITY(1,1) PRIMARY KEY,
        Governorate NVARCHAR(100),
        City NVARCHAR(100),
        Zone NVARCHAR(50),
        CONSTRAINT UQ_{DIM_LOCATION}_Loc UNIQUE (Governorate, City, Zone)
    );
END
"""

CREATE_DIM_ALERT_SQL = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{DIM_ALERT}') AND type in (N'U'))
BEGIN
    CREATE TABLE {DIM_ALERT} (
        AlertKey INT IDENTITY(1,1) PRIMARY KEY,
        AlertType NVARCHAR(100),
        AlertLevel INT,
        CONSTRAINT UQ_{DIM_ALERT}_Alert UNIQUE (AlertType, AlertLevel)
    );
END
"""

CREATE_FACT_SQL = f"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{FACT_WEATHER}') AND type in (N'U'))
BEGIN
    CREATE TABLE {FACT_WEATHER} (
        FactKey INT IDENTITY(1,1) PRIMARY KEY,
        DateKey INT,
        DeviceKey INT,
        LocationKey INT,
        AlertKey INT,
        Timestamp DATETIME,
        Temperature_C FLOAT,
        Humidity_pct FLOAT,
        WindSpeed_kmh FLOAT,
        WindDirection NVARCHAR(10),
        Rainfall_mm FLOAT,
        CloudCoverage_pct FLOAT,
        UV_Index FLOAT,
        Pressure_hPa FLOAT,
        Battery_pct FLOAT
    );
END
"""

# ----------------------------
# Utility functions
# ----------------------------
def connect(conn_str):
    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
        return conn
    except Exception as e:
        print("ERROR: could not connect to database:", e)
        sys.exit(1)

def ensure_dw_schema(cursor):
    cursor.execute(CREATE_CONTROL_SQL)
    cursor.execute(CREATE_DIM_DATE_SQL)
    cursor.execute(CREATE_DIM_DEVICE_SQL)
    cursor.execute(CREATE_DIM_LOCATION_SQL)
    cursor.execute(CREATE_DIM_ALERT_SQL)
    cursor.execute(CREATE_FACT_SQL)

def get_last_loaded_timestamp(cursor):
    cursor.execute(f"SELECT LastLoadedTimestamp FROM {ETL_CONTROL_TABLE} WHERE SourceTable = ?", SOURCE_TABLE)
    row = cursor.fetchone()
    return row[0] if row else None

def update_last_loaded_timestamp(cursor, ts):
    now = datetime.utcnow()
    cursor.execute(f"""
        UPDATE {ETL_CONTROL_TABLE}
        SET LastLoadedTimestamp = ?, LastRun = ?
        WHERE SourceTable = ?
    """, ts, now, SOURCE_TABLE)

def parse_row_value(row, colname):
    # pyodbc returns row as tuple; we'll address by column names via cursor.description mapping later
    return row.get(colname)

def ensure_dim_date(cursor, dt: datetime):
    """
    Insert date row into DimDate if not exists. Return DateKey (YYYYMMDD int).
    """
    dk = int(dt.strftime("%Y%m%d"))
    cursor.execute(f"SELECT DateKey FROM {DIM_DATE} WHERE DateKey = ?", dk)
    r = cursor.fetchone()
    if r:
        return r[0]
    # compute attributes
    full = dt.date()
    year = dt.year
    month = dt.month
    day = dt.day
    week = dt.isocalendar()[1]
    quarter = (month - 1) // 3 + 1
    cursor.execute(f"INSERT INTO {DIM_DATE} (DateKey, FullDate, YearInt, MonthInt, DayInt, WeekOfYear, QuarterInt) VALUES (?,?,?,?,?,?,?)",
                   dk, full, year, month, day, week, quarter)
    return dk

def get_or_create_device(cursor, device_id, device_type):
    cursor.execute(f"SELECT DeviceKey FROM {DIM_DEVICE} WHERE DeviceID = ? AND DeviceType = ?", device_id, device_type)
    r = cursor.fetchone()
    if r:
        return r[0]
    cursor.execute(f"INSERT INTO {DIM_DEVICE} (DeviceID, DeviceType) VALUES (?,?)", device_id, device_type)
    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]

def get_or_create_location(cursor, gov, city, zone):
    cursor.execute(f"SELECT LocationKey FROM {DIM_LOCATION} WHERE Governorate = ? AND City = ? AND Zone = ?", gov, city, zone)
    r = cursor.fetchone()
    if r:
        return r[0]
    cursor.execute(f"INSERT INTO {DIM_LOCATION} (Governorate, City, Zone) VALUES (?,?,?)", gov, city, zone)
    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]

def get_or_create_alert(cursor, alert_type, alert_level):
    cursor.execute(f"SELECT AlertKey FROM {DIM_ALERT} WHERE AlertType = ? AND AlertLevel = ?", alert_type, alert_level)
    r = cursor.fetchone()
    if r:
        return r[0]
    cursor.execute(f"INSERT INTO {DIM_ALERT} (AlertType, AlertLevel) VALUES (?,?)", alert_type, alert_level)
    cursor.execute("SELECT @@IDENTITY")
    return cursor.fetchone()[0]

# ----------------------------
# Main ETL
# ----------------------------
def run_etl():
    conn = connect(SQL_CONN_STR)
    cursor = conn.cursor()

    try:
        print("Ensuring DW schema exists...")
        ensure_dw_schema(cursor)
        conn.commit()
        print("Schema ensured.")

        last_loaded = get_last_loaded_timestamp(cursor)
        print("Last loaded timestamp:", last_loaded)

        # Build SELECT query: get only new rows
        if last_loaded:
            select_sql = f"SELECT * FROM {SOURCE_TABLE} WHERE Timestamp > ? ORDER BY Timestamp"
            cursor.execute(select_sql, last_loaded)
        else:
            select_sql = f"SELECT * FROM {SOURCE_TABLE} ORDER BY Timestamp"
            cursor.execute(select_sql)

        rows = cursor.fetchall()
        cols = [c[0] for c in cursor.description] if cursor.description else []
        print(f"Found {len(rows)} rows to process.")

        if not rows:
            print("No new rows. Exiting.")
            conn.commit()
            return

        max_ts = last_loaded

        # Process each row
        for raw in rows:
            # convert row to dict for easy access
            row = {cols[i]: raw[i] for i in range(len(cols))}

            # parse timestamp - handle different types (datetime or string)
            raw_ts = row.get("Timestamp")
            if raw_ts is None:
                print("Skipping row without Timestamp:", row)
                continue
            if isinstance(raw_ts, datetime):
                ts = raw_ts
            else:
                # SQL datetime strings like '2025-12-03 20:32:18.000'
                try:
                    ts = datetime.strptime(str(raw_ts), "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    try:
                        ts = datetime.strptime(str(raw_ts), "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        print("Unrecognized Timestamp format, skipping row:", raw_ts)
                        continue

            if (max_ts is None) or (ts > max_ts):
                max_ts = ts

            # ensure dims exist and get keys
            date_key = ensure_dim_date(cursor, ts)
            device_key = get_or_create_device(cursor,
                                              str(row.get("DeviceID") or ""),
                                              str(row.get("DeviceType") or ""))
            location_key = get_or_create_location(cursor,
                                                  str(row.get("Governorate") or ""),
                                                  str(row.get("City") or ""),
                                                  str(row.get("Zone") or ""))
            # AlertLevel in table might be numeric or string
            alert_level_val = row.get("AlertLevel")
            try:
                alert_level = int(alert_level_val) if alert_level_val is not None else None
            except Exception:
                alert_level = None
            alert_key = get_or_create_alert(cursor,
                                            str(row.get("AlertType") or ""),
                                            alert_level if alert_level is not None else -1)

            # insert into fact table
            cursor.execute(f"""
                INSERT INTO {FACT_WEATHER} (
                    DateKey, DeviceKey, LocationKey, AlertKey, Timestamp,
                    Temperature_C, Humidity_pct, WindSpeed_kmh, WindDirection,
                    Rainfall_mm, CloudCoverage_pct, UV_Index, Pressure_hPa, Battery_pct
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            date_key, device_key, location_key, alert_key, ts,
            safe_float(row.get("Temperature_C")),
            safe_float(row.get("Humidity_pct")),
            safe_float(row.get("WindSpeed_kmh")),
            str(row.get("WindDirection") or ""),
            safe_float(row.get("Rainfall_mm")),
            safe_float(row.get("CloudCoverage_pct")),
            safe_float(row.get("UV_Index")),
            safe_float(row.get("Pressure_hPa")),
            safe_float(row.get("Battery_pct"))
            )

            # commit every N rows could be added; for simplicity commit after all
        # update control table
        update_last_loaded_timestamp(cursor, max_ts)
        conn.commit()
        print(f"ETL finished. New LastLoadedTimestamp set to {max_ts}")

    except Exception as e:
        conn.rollback()
        print("ETL failed:", e)
    finally:
        cursor.close()
        conn.close()

def safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None

# ----------------------------
# Run script
# ----------------------------
if __name__ == "__main__":
    run_etl()
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Raw sensor data file not found: {file_path}")
    df = pd.read_csv(file_path)
    print(f"✅ Extracted {len(df)} rows from {file_path}")
    return df

# === Transform ===f
def transform_data(df):
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Remove duplicates
    df = df.drop_duplicates()

    # Aggregate per minute
    df['date'] = df['timestamp'].dt.floor('min')
    grouped = (
        df.groupby('date')
        .agg(avg_temp=('temperature', 'mean'),
             avg_humidity=('humidity', 'mean'))
        .reset_index()
    )

    # Flag anomalies
    grouped['temp_alert'] = grouped['avg_temp'].apply(lambda x: 'Yes' if x > 35 else 'No')
    grouped['humidity_alert'] = grouped['avg_humidity'].apply(lambda x: 'Yes' if x > 50 else 'No')

    print(f"⚙️ Transformed data: {len(grouped)} aggregated rows.")
    return grouped

# === Load ===
def load_data(df, output_file):
    df.to_csv(output_file, index=False)
    print(f"📁 Loaded processed data to {output_file}")

# === Main ETL flow ===
def run_etl():
    print("🚀 Starting ETL process...")
    raw_df = extract_data(RAW_FILE)
    transformed_df = transform_data(raw_df)
    load_data(transformed_df, PROCESSED_FILE)
    print("🎯 ETL completed successfully!")

if __name__ == "__main__":
    run_etl()

