# ----------------------------------------------------------
#  IMPORTS
# ----------------------------------------------------------
import requests
import random
import time
from datetime import datetime, timezone
import pyodbc
import csv
import os

# ----------------------------------------------------------
#  CONFIGURATION
# ----------------------------------------------------------
POWER_BI_URL = "https://api.powerbi.com/beta/2082de46-1afa-4b64-a440-6558f80e9840/datasets/12ca1e37-95a7-4ce1-826f-c2141c2fc60c/rows?experience=power-bi&key=pDybOBTaxgs0lT9qRqZem4A%2FUzTzqr4NVshzAZA%2F7AKcQ1OIgwnR8mqe5RJccnliXpAQQAVgK%2B%2B59y2f9LqVcA%3D%3D"

SQL_CONN_STR = (
   "Driver={ODBC Driver 17 for SQL Server};"
  "Server=team4iotproject.database.windows.net;"
    "Database=Depi_Project1;"
    "Uid=team4iotproject;"
    "Pwd=team4444#;"
)
# CSV Configuration
CSV_FILENAME = "weather_station_data.csv"
CSV_HEADERS = [
    "Timestamp", "DeviceID", "DeviceType", "Governorate", "City", "Zone",
    "Temperature_C", "Humidity_pct", "WindSpeed_kmh", "WindDirection",
    "Rainfall_mm", "CloudCoverage_pct", "UV_Index", "Pressure_hPa",
    "Battery_pct", "AlertLevel", "AlertType", "Advisory"
]

# ----------------------------------------------------------
#  EGYPT LOCATIONS FULL LIST
# ----------------------------------------------------------
EGYPT_LOCATIONS = [ {"governorate": "Cairo", "city": "Nasr City", "device_id": "WS_Cairo_01"},
    {"governorate": "Cairo", "city": "Maadi", "device_id": "WS_Cairo_02"},
    {"governorate": "Cairo", "city": "Heliopolis", "device_id": "WS_Cairo_03"},
    {"governorate": "Giza", "city": "Dokki", "device_id": "WS_Giza_01"},
    {"governorate": "Giza", "city": "6th of October", "device_id": "WS_Giza_02"},
    {"governorate": "Giza", "city": "Haram", "device_id": "WS_Giza_03"},
    {"governorate": "Alexandria", "city": "Smouha", "device_id": "WS_Alex_01"},
    {"governorate": "Alexandria", "city": "Stanley", "device_id": "WS_Alex_02"},
    {"governorate": "Alexandria", "city": "Gleem", "device_id": "WS_Alex_03"},
    {"governorate": "Qalyubia", "city": "Shubra El Kheima", "device_id": "WS_Qalyubia_01"},
    {"governorate": "Qalyubia", "city": "Benha", "device_id": "WS_Qalyubia_02"},
    {"governorate": "Sharqia", "city": "Zagazig", "device_id": "WS_Sharqia_01"},
    {"governorate": "Sharqia", "city": "Belbeis", "device_id": "WS_Sharqia_02"},
    {"governorate": "Dakahlia", "city": "Mansoura", "device_id": "WS_Dakahlia_01"},
    {"governorate": "Dakahlia", "city": "Mit Ghamr", "device_id": "WS_Dakahlia_02"},
    {"governorate": "Monufia", "city": "Shibin El Kom", "device_id": "WS_Monufia_01"},
    {"governorate": "Monufia", "city": "Sadat City", "device_id": "WS_Monufia_02"},
    {"governorate": "Beheira", "city": "Damanhour", "device_id": "WS_Beheira_01"},
    {"governorate": "Beheira", "city": "Kafr El Dawwar", "device_id": "WS_Beheira_02"},
    {"governorate": "Minya", "city": "Minya", "device_id": "WS_Minya_01"},
    {"governorate": "Minya", "city": "Maghagha", "device_id": "WS_Minya_02"},
    {"governorate": "Asyut", "city": "Asyut", "device_id": "WS_Asyut_01"},
    {"governorate": "Asyut", "city": "Abu Tig", "device_id": "WS_Asyut_02"},
    {"governorate": "Sohag", "city": "Sohag", "device_id": "WS_Sohag_01"},
    {"governorate": "Sohag", "city": "Tahta", "device_id": "WS_Sohag_02"},
    {"governorate": "Qena", "city": "Qena", "device_id": "WS_Qena_01"},
    {"governorate": "Qena", "city": "Nag Hammadi", "device_id": "WS_Qena_02"},
    {"governorate": "Luxor", "city": "Luxor", "device_id": "WS_Luxor_01"},
    {"governorate": "Luxor", "city": "Esna", "device_id": "WS_Luxor_02"},
    {"governorate": "Aswan", "city": "Aswan", "device_id": "WS_Aswan_01"},
    {"governorate": "Aswan", "city": "Kom Ombo", "device_id": "WS_Aswan_02"},
    {"governorate": "Red Sea", "city": "Hurghada", "device_id": "WS_RedSea_01"},
    {"governorate": "Red Sea", "city": "Marsa Alam", "device_id": "WS_RedSea_02"},
    {"governorate": "South Sinai", "city": "Sharm El Sheikh", "device_id": "WS_SouthSinai_01"},
    {"governorate": "South Sinai", "city": "Dahab", "device_id": "WS_SouthSinai_02"},
    {"governorate": "North Sinai", "city": "Arish", "device_id": "WS_NorthSinai_01"},
    {"governorate": "North Sinai", "city": "Rafah", "device_id": "WS_NorthSinai_02"},
    {"governorate": "Fayoum", "city": "Fayoum", "device_id": "WS_Fayoum_01"},
    {"governorate": "Fayoum", "city": "Sinnuris", "device_id": "WS_Fayoum_02"},
    {"governorate": "Beni Suef", "city": "Beni Suef", "device_id": "WS_BeniSuef_01"},
    {"governorate": "Beni Suef", "city": "Nasser", "device_id": "WS_BeniSuef_02"},
    {"governorate": "Matrouh", "city": "Marsa Matrouh", "device_id": "WS_Matrouh_01"},
    {"governorate": "Matrouh", "city": "Siwa", "device_id": "WS_Matrouh_02"}
]

# ----------------------------------------------------------
#  ZONES PER GOVERNORATE
# ----------------------------------------------------------
ZONES_MAP = {  "Cairo": ["Rooftop", "Balcony", "Indoor"],
    "Giza": ["Rooftop", "Garden", "Indoor"],
    "Alexandria": ["Rooftop", "Balcony", "Street"],
    "Qalyubia": ["Rooftop", "Street", "Indoor"],
    "Sharqia": ["Rooftop", "Garden", "Indoor"],
    "Dakahlia": ["Rooftop", "Balcony", "Indoor"],
    "Monufia": ["Rooftop", "Garden", "Indoor"],
    "Beheira": ["Rooftop", "Street", "Indoor"],
    "Minya": ["Rooftop", "Garden", "Indoor"],
    "Asyut": ["Rooftop", "Balcony", "Indoor"],
    "Sohag": ["Rooftop", "Street", "Indoor"],
    "Qena": ["Rooftop", "Balcony", "Indoor"],
    "Luxor": ["Rooftop", "Garden", "Indoor"],
    "Aswan": ["Rooftop", "Balcony", "Indoor"],
    "Red Sea": ["Rooftop", "Street", "Beach"],
    "South Sinai": ["Rooftop", "Street", "Beach"],
    "North Sinai": ["Rooftop", "Street", "Indoor"],
    "Fayoum": ["Rooftop", "Garden", "Indoor"],
    "Beni Suef": ["Rooftop", "Balcony", "Indoor"],
    "Matrouh": ["Rooftop", "Street", "Beach"]
}

# ----------------------------------------------------------
#  DEVICE TYPES
# ----------------------------------------------------------
DEVICE_TYPES = ["Weather Station Pro", "Weather Station Basic", "Outdoor Sensor", "Smart Sensor"]

# ----------------------------------------------------------
#  CSV FILE INITIALIZATION
# ----------------------------------------------------------
def initialize_csv():
    """Create CSV file with headers if it doesn't exist"""
    if not os.path.exists(CSV_FILENAME):
        with open(CSV_FILENAME, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
            writer.writeheader()
            print(f"📄 Created new CSV file: {CSV_FILENAME}")
    else:
        print(f"📄 Using existing CSV file: {CSV_FILENAME}")

def save_to_csv(record):
    """Append record to CSV file"""
    try:
        with open(CSV_FILENAME, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=CSV_HEADERS)
            writer.writerow(record)
        print(f"💾 Record saved to CSV: {record['Timestamp']} - {record['DeviceID']}")
    except Exception as e:
        print(f"❌ CSV Error: {e}")

# ----------------------------------------------------------
#  SENSOR DATA GENERATION
# ----------------------------------------------------------
def generate_weather_data():
    temperature = round(random.uniform(15, 42), 2)
    humidity = round(random.uniform(20, 95), 2)
    wind_speed = round(random.uniform(0, 60), 2)
    wind_direction = random.choice(["N","NE","E","SE","S","SW","W","NW"])
    rainfall = round(random.uniform(0, 50), 2)
    cloud = round(random.uniform(0, 100), 2)
    uv = round(random.uniform(0, 12), 2)
    pressure = round(random.uniform(980, 1050), 2)
    battery = round(random.uniform(30, 100), 2)
    return temperature, humidity, wind_speed, wind_direction, rainfall, cloud, uv, pressure, battery

# ----------------------------------------------------------
#  ALERT SYSTEM
# ----------------------------------------------------------
def analyze_alerts(temp, hum, wind, rain, pressure, uv):
    if uv > 8:
        return 2, "High UV"
    if wind > 40:
        return 2, "Wind Alert"
    if rain > 25:
        return 1, "Heavy Rain"
    if temp > 40:
        return 1, "Heat Wave"
    if pressure < 990:
        return 2, "Storm Warning"
    return 0, "Normal"

# ----------------------------------------------------------
#  ADVISORY MESSAGE
# ----------------------------------------------------------
def advisory_text(alert_type):
    messages = {
        "High UV": "☀️ Very high UV! Avoid direct sunlight.",
        "Wind Alert": "🌬️ Strong winds! Be cautious outside.",
        "Heavy Rain": "🌧️ Heavy rainfall expected.",
        "Heat Wave": "🔥 Extreme heat. Stay hydrated.",
        "Storm Warning": "⛈️ Possible storm! Stay indoors.",
        "Normal": "✅ Weather is stable."
    }
    return messages.get(alert_type, "No advisory")

# ----------------------------------------------------------
#  SQL CONNECTION
# ----------------------------------------------------------
connection = pyodbc.connect(SQL_CONN_STR)
cursor = connection.cursor()

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='WeatherStation_Pro' AND xtype='U')
BEGIN
    CREATE TABLE WeatherStation_Pro (
        Timestamp datetime,
        DeviceID nvarchar(100),
        DeviceType nvarchar(100),
        Governorate nvarchar(100),
        City nvarchar(100),
        Zone nvarchar(50),
        Temperature_C float,
        Humidity_pct float,
        WindSpeed_kmh float,
        WindDirection nvarchar(10),
        Rainfall_mm float,
        CloudCoverage_pct float,
        UV_Index float,
        Pressure_hPa float,
        Battery_pct float,
        AlertLevel int,
        AlertType nvarchar(100),
        Advisory nvarchar(300)
    );
END
""")
connection.commit()

# ----------------------------------------------------------
#  INITIALIZE CSV FILE
# ----------------------------------------------------------
initialize_csv()

# ----------------------------------------------------------
#  MAIN STREAMING LOOP
# ----------------------------------------------------------
print("🌦️ Streaming Professional Weather Data to SQL + Power BI + CSV ...")
print("=" * 60)

try:
    while True:
        # Generate weather data
        temp, hum, wind, wind_dir, rain, cloud, uv, pressure, battery = generate_weather_data()
        alert_level, alert_type = analyze_alerts(temp, hum, wind, rain, pressure, uv)
        advisory = advisory_text(alert_type)

        # Select random location
        selected_location = random.choice(EGYPT_LOCATIONS)

        # Select random zone based on governorate
        zones = ZONES_MAP.get(selected_location["governorate"], ["Rooftop"])
        selected_zone = random.choice(zones)

        # Select random device type
        device_type = random.choice(DEVICE_TYPES)

        # Create record
        record = {
            "Timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "DeviceID": selected_location["device_id"],
            "DeviceType": device_type,
            "Governorate": selected_location["governorate"],
            "City": selected_location["city"],
            "Zone": selected_zone,
            "Temperature_C": temp,
            "Humidity_pct": hum,
            "WindSpeed_kmh": wind,
            "WindDirection": wind_dir,
            "Rainfall_mm": rain,
            "CloudCoverage_pct": cloud,
            "UV_Index": uv,
            "Pressure_hPa": pressure,
            "Battery_pct": battery,
            "AlertLevel": alert_level,
            "AlertType": alert_type,
            "Advisory": advisory
        }

        # 1. SAVE TO CSV FILE
        save_to_csv(record)

        # 2. SEND TO POWER BI
        try:
            response = requests.post(POWER_BI_URL, json=[record])
            if response.status_code == 200:
                print(f"📊 Sent to Power BI: {record['Timestamp']}")
            else:
                print(f"⚠️ Power BI Error: {response.status_code}")
        except Exception as e:
            print(f"❌ Power BI Connection Error: {e}")

        # 3. INSERT INTO SQL DATABASE
        try:
            cursor.execute("""
                INSERT INTO WeatherStation_Pro VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
                record["Timestamp"], record["DeviceID"], record["DeviceType"],
                record["Governorate"], record["City"], record["Zone"],
                record["Temperature_C"], record["Humidity_pct"], record["WindSpeed_kmh"],
                record["WindDirection"], record["Rainfall_mm"], record["CloudCoverage_pct"],
                record["UV_Index"], record["Pressure_hPa"], record["Battery_pct"],
                record["AlertLevel"], record["AlertType"], record["Advisory"]
            )
            connection.commit()
            print(f"🗄️  Saved to SQL: {record['DeviceID']} - {record['City']}")
        except Exception as e:
            print(f"❌ SQL Error: {e}")

        # Display current record summary
        print(f"📍 {record['Governorate']} - {record['City']} | 🌡️ {temp}°C | 💧 {hum}% | ⚠️ {alert_type}")
        print("-" * 60)

        # Wait before next iteration
        time.sleep(2)

except KeyboardInterrupt:
    print("\n🛑 Streaming stopped by user")
finally:
    # Clean up SQL connection
    cursor.close()
    connection.close()
    print("🔌 Database connection closed")
    print(f"📁 All data saved to: {CSV_FILENAME}")
    
