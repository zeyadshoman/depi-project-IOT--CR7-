import pyodbc
import random
from datetime import datetime
import time
import requests

# --- CONFIGURATION ---
POWER_BI_URL = "https://api.powerbi.com/beta/2082de46-1afa-4b64-a440-6558f80e9840/datasets/12ca1e37-95a7-4ce1-826f-c2141c2fc60c/rows?experience=power-bi&key=pDybOBTaxgs0lT9qRqZem4A%2FUzTzqr4NVshzAZA%2F7AKcQ1OIgwnR8mqe5RJccnliXpAQQAVgK%2B%2B59y2f9LqVcA%3D%3D"  # Replace this
SERVER = "DESKTOP-OQDQ9FN"
DATABASE = "DepiProject"

# --- Function to generate random data ---
def generate_data():
    temp = round(random.uniform(20.0, 30.0), 2)
    humidity = round(random.uniform(30.0, 70.0), 2)
    timestamp = datetime.now()
    return temp, humidity, timestamp

# --- Connect to SQL Server ---
try:
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"Trusted_Connection=yes;"
    )
    cursor = conn.cursor()
    print("✅ Connected to SQL Server successfully.")

    # Ensure table exists
    cursor.execute('''
        IF OBJECT_ID('SensorData', 'U') IS NULL
        CREATE TABLE SensorData (
            Id INT IDENTITY(1,1) PRIMARY KEY,
            Temperature FLOAT,
            Humidity FLOAT,
            Timestamp DATETIME
        )
    ''')
    conn.commit()
    print("✅ Table check/creation completed.")

    counter = 0
    print("🚀 Starting real-time data generation... Press Ctrl+C to stop.")

    while True:
        temp, humidity, timestamp = generate_data()

        # --- 1️⃣ Push to Power BI API ---
        payload = [{
            "Temperature": temp,
            "Humidity": humidity,
            "Timestamp": timestamp.isoformat()
        }]
        try:
            response = requests.post(POWER_BI_URL, json=payload)
            if response.status_code == 200:
                print(f"📡 Sent to Power BI: {payload}")
            else:
                print(f"⚠️ Power BI push failed: {response.status_code} {response.text}")
        except Exception as e:
            print(f"⚠️ Power BI push error: {e}")

        # --- 2️⃣ Save to SQL Server ---
        insert_query = '''
            INSERT INTO SensorData (Temperature, Humidity, Timestamp)
            VALUES (?, ?, ?)
        '''
        cursor.execute(insert_query, (temp, humidity, timestamp))
        counter += 1

        if counter % 10 == 0:
            conn.commit()

        # Wait 1 second before next record
        time.sleep(1)

except KeyboardInterrupt:
    print("\n🛑 Stopped by user.")

except Exception as e:
    print(f"⚠️ Error: {e}")

finally:
    try:
        cursor.close()
        conn.commit()
        conn.close()
        print("🔒 SQL connection closed safely.")
    except:
        pass
