import pandas as pd
import time
import os

RAW_FILE = "data/sensor_data.csv"
ALERTS_FILE = "data/stream_alerts.csv"

def check_new_data(last_rows):
    df = pd.read_csv(RAW_FILE)
    if len(df) > last_rows:
        new_data = df.iloc[last_rows:]
        return new_data, len(df)
    return None, last_rows

def process_stream(df):
    alerts = []
    for _, row in df.iterrows():
        if row['temperature'] > 35:
            alerts.append({
                "timestamp": row["timestamp"],
                "metric": "temperature",
                "value": row["temperature"],
                "alert": "High Temp"
            })
        if row['humidity'] > 50:
            alerts.append({
                "timestamp": row["timestamp"],
                "metric": "humidity",
                "value": row["humidity"],
                "alert": "High Humidity"
            })
    return alerts

def save_alerts(alerts):
    if alerts:
        df = pd.DataFrame(alerts)
        if not os.path.exists(ALERTS_FILE):
            df.to_csv(ALERTS_FILE, index=False)
        else:
            df.to_csv(ALERTS_FILE, index=False, mode='a', header=False)
        print(f"⚠️ Alerts saved: {len(alerts)}")

if __name__ == "__main__":
    print("🔄 Starting Streaming Listener...")
    last_rows = 0
    while True:
        new_data, last_rows = check_new_data(last_rows)
        if new_data is not None:
            print(f"📥 New rows detected: {len(new_data)}")
            alerts = process_stream(new_data)
            save_alerts(alerts)
        time.sleep(5)
