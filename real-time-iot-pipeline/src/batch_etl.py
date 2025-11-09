import pandas as pd
import os

# === File paths ===
RAW_FILE = "data/sensor_data.csv"
PROCESSED_FILE = "data/processed_data.csv"

# === Extract ===
def extract_data(file_path):
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
