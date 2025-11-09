import pyodbc
import random
from datetime import datetime
import time

# Function to generate random sensor data
def generate_data():
    temp = round(random.uniform(20.0, 30.0), 2)
    humidity = round(random.uniform(30.0, 70.0), 2)
    timestamp = datetime.now()
    return temp, humidity, timestamp

# Connect to SQL Server
try:
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'  # Use your installed driver
        'SERVER=LAPTOP-4RAII4JU;'
        'DATABASE=protr1;'
        'Trusted_Connection=yes;'
    )
    cursor = conn.cursor()
    print("✅ Connected to SQL Server successfully.")

    # Create table if it doesn't exist
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

    # Start inserting data
    counter = 0
    try:
        while True:
            temp, humidity, timestamp = generate_data()
            insert_query = '''
                INSERT INTO SensorData (Temperature, Humidity, Timestamp)
                VALUES (?, ?, ?)
            '''
            cursor.execute(insert_query, (temp, humidity, timestamp))
            counter += 1

            # Commit every 10 inserts for better performance
            if counter % 10 == 0:
                conn.commit()

            print(f"Inserted data: Temp={temp}, Humidity={humidity}, Timestamp={timestamp}")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Data insertion stopped by user.")

    except Exception as e:
        print(f"⚠️ Error during data insertion: {e}")

finally:
    # Always close connection safely
    try:
        cursor.close()
        conn.commit()
        conn.close()
        print("🔒 Connection closed safely.")
    except:
        pass
