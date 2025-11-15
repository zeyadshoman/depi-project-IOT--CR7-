import random
import time
from datetime import datetime

def generate_sensor_data():
    """Simulate temperature and humidity sensor readings"""
    temperature = round(random.uniform(25.0, 45.0), 2)  # °C
    humidity = round(random.uniform(40.0, 90.0), 2)     # %
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return temperature, humidity, timestamp

def check_conditions(temp, hum):
    """Check temperature and humidity thresholds and give advice"""
    if temp > 35 and hum > 70:
        return "⚠️ It's very hot and humid! Avoid going outside and stay hydrated."
    elif temp > 35:
        return "☀️ High temperature detected! Try to stay indoors."
    elif hum > 80:
        return "💧 High humidity levels. Stay in cool, ventilated areas."
    else:
        return "✅ Conditions are normal. Safe to go outside."

def main():
    print("🌍 Real-Time IoT Weather Advisory System\n")
    while True:
        temp, hum, timestamp = generate_sensor_data()
        advice = check_conditions(temp, hum)
        print(f"[{timestamp}] Temperature: {temp}°C | Humidity: {hum}% --> {advice}")
        
        # Optional: you could send this data to Azure or Kafka here
        # send_to_kafka({"temperature": temp, "humidity": hum, "timestamp": timestamp, "advice": advice})
        
        time.sleep(5)  # wait 5 seconds between readings

if __name__ == "__main__":
    main()
