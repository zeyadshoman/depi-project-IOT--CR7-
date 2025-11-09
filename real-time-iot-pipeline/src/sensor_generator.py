import time
import random
import pandas as pd
from datetime import datetime
import os

# ensure data folder exists
os.makedirs("data", exist_ok=True)

def generate_sensor_data():
    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2)
    }

if __name__ == "__main__":
    csv_path = "data/sensor_data.csv"
    if not os.path.isfile(csv_path):
        pd.DataFrame(columns=["timestamp","temperature","humidity"]).to_csv(csv_path, index=False)
    print("Starting IoT Sensor Data Generator (press Ctrl+C to stop)...")
    while True:
        data = generate_sensor_data()
        df = pd.DataFrame([data])
        df.to_csv(csv_path, mode='a', header=False, index=False)
        print(data)
        time.sleep(5)