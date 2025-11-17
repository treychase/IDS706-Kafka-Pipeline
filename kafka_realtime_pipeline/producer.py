import time
import json
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import numpy as np

fake = Faker()

class IoTSensorSimulator:
    """Simulates realistic IoT sensor readings with temporal patterns and anomalies."""
    
    def __init__(self):
        # Base values for different sensor types
        self.base_temp = 22.0  # Celsius
        self.base_humidity = 45.0  # Percentage
        self.base_co2 = 400.0  # ppm
        self.base_energy = 50.0  # kWh
        
        # Track state for temporal correlation
        self.time_offset = 0
        self.anomaly_probability = 0.05
        
    def generate_temperature(self, room_type: str, time_of_day: int) -> float:
        """Generate temperature with daily patterns and room-specific variations."""
        # Daily pattern: cooler at night, warmer during day
        daily_variation = 3 * np.sin(2 * np.pi * time_of_day / 24)
        
        # Room-specific variations
        room_offsets = {
            "Server Room": -5.0,
            "Conference Room": 1.0,
            "Office": 0.0,
            "Lobby": 0.5,
            "Cafeteria": 2.0,
            "Warehouse": -2.0
        }
        
        base = self.base_temp + room_offsets.get(room_type, 0)
        temp = base + daily_variation + np.random.normal(0, 0.5)
        
        # Occasional anomalies (overheating)
        if random.random() < self.anomaly_probability:
            temp += random.uniform(8, 15)
            
        return round(temp, 2)
    
    def generate_humidity(self, temperature: float) -> float:
        """Generate humidity inversely correlated with temperature."""
        # Inverse relationship with temperature
        humidity = self.base_humidity - (temperature - self.base_temp) * 1.5
        humidity += np.random.normal(0, 2)
        
        # Anomaly: sudden humidity spike
        if random.random() < self.anomaly_probability:
            humidity += random.uniform(20, 30)
            
        return round(max(20, min(80, humidity)), 2)
    
    def generate_co2(self, occupancy: int, room_type: str) -> float:
        """Generate CO2 levels based on occupancy."""
        co2 = self.base_co2 + (occupancy * 50) + np.random.normal(0, 20)
        
        # Server rooms have lower CO2, conference rooms higher
        room_factors = {
            "Server Room": 0.8,
            "Conference Room": 1.3,
            "Office": 1.0,
            "Lobby": 0.9,
            "Cafeteria": 1.2,
            "Warehouse": 0.7
        }
        
        co2 *= room_factors.get(room_type, 1.0)
        
        # Anomaly: ventilation failure
        if random.random() < self.anomaly_probability * 0.5:
            co2 += random.uniform(400, 800)
            
        return round(max(300, co2), 2)
    
    def generate_occupancy(self, room_type: str, time_of_day: int) -> int:
        """Generate occupancy based on room type and time."""
        # Working hours pattern
        if 9 <= time_of_day <= 17:
            activity_factor = 1.0
        elif 7 <= time_of_day <= 20:
            activity_factor = 0.4
        else:
            activity_factor = 0.1
            
        max_occupancy = {
            "Server Room": 2,
            "Conference Room": 20,
            "Office": 15,
            "Lobby": 30,
            "Cafeteria": 50,
            "Warehouse": 10
        }
        
        max_occ = max_occupancy.get(room_type, 10)
        expected = int(max_occ * activity_factor)
        occupancy = max(0, int(np.random.poisson(expected)))
        
        return min(occupancy, max_occ)
    
    def generate_energy(self, occupancy: int, temperature: float, room_type: str) -> float:
        """Generate energy consumption based on occupancy and HVAC load."""
        # Base consumption
        base_consumption = {
            "Server Room": 150.0,
            "Conference Room": 40.0,
            "Office": 30.0,
            "Lobby": 20.0,
            "Cafeteria": 80.0,
            "Warehouse": 100.0
        }
        
        base = base_consumption.get(room_type, 50.0)
        
        # Occupancy increases energy
        occupancy_load = occupancy * 0.5
        
        # HVAC load increases with temperature deviation
        hvac_load = abs(temperature - 22) * 2
        
        energy = base + occupancy_load + hvac_load + np.random.normal(0, 5)
        
        # Anomaly: equipment malfunction
        if random.random() < self.anomaly_probability:
            energy *= random.uniform(2.0, 3.5)
            
        return round(max(0, energy), 2)


def generate_synthetic_sensor_reading(simulator: IoTSensorSimulator):
    """Generates synthetic IoT sensor data for smart building monitoring."""
    
    buildings = ["Building A", "Building B", "Building C"]
    floors = ["Floor 1", "Floor 2", "Floor 3", "Floor 4"]
    room_types = ["Server Room", "Conference Room", "Office", "Lobby", "Cafeteria", "Warehouse"]
    sensor_statuses = ["Normal", "Normal", "Normal", "Normal", "Maintenance", "Warning"]
    
    building = random.choice(buildings)
    floor = random.choice(floors)
    room_type = random.choice(room_types)
    room_id = f"{building}-{floor}-{room_type.replace(' ', '')}-{random.randint(101, 150)}"
    
    # Get current time for temporal patterns
    current_hour = datetime.now().hour
    
    # Generate correlated sensor readings
    occupancy = simulator.generate_occupancy(room_type, current_hour)
    temperature = simulator.generate_temperature(room_type, current_hour)
    humidity = simulator.generate_humidity(temperature)
    co2 = simulator.generate_co2(occupancy, room_type)
    energy = simulator.generate_energy(occupancy, temperature, room_type)
    
    # Determine sensor status
    status = random.choice(sensor_statuses)
    if temperature > 30 or co2 > 1000 or energy > 200:
        status = "Warning"
    if temperature > 35 or co2 > 1500:
        status = "Critical"
    
    return {
        "sensor_id": str(uuid.uuid4())[:12],
        "room_id": room_id,
        "building": building,
        "floor": floor,
        "room_type": room_type,
        "temperature": temperature,
        "humidity": humidity,
        "co2": co2,
        "occupancy": occupancy,
        "energy_consumption": energy,
        "status": status,
        "timestamp": datetime.now().isoformat(),
    }


def run_producer():
    """Kafka producer that sends synthetic IoT sensor readings to the 'sensors' topic."""
    try:
        print("[Producer] 🚀 Starting IoT Sensor Data Producer")
        print("[Producer] Connecting to Kafka at localhost:9092...")
        
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=30000,
            max_block_ms=60000,
            retries=5,
        )
        print("[Producer] ✓ Connected to Kafka successfully!")
        print("[Producer] 📊 Generating realistic IoT sensor data with temporal patterns...")
        print()
        
        simulator = IoTSensorSimulator()
        count = 0
        
        while True:
            reading = generate_synthetic_sensor_reading(simulator)
            
            # Format output
            status_emoji = "🟢" if reading["status"] == "Normal" else "🟡" if reading["status"] == "Maintenance" else "🔴"
            print(f"[Producer] {status_emoji} Reading #{count}: {reading['room_id']}")
            print(f"           🌡️  Temp: {reading['temperature']}°C | 💧 Humidity: {reading['humidity']}%")
            print(f"           🫁 CO2: {reading['co2']}ppm | 👥 Occupancy: {reading['occupancy']}")
            print(f"           ⚡ Energy: {reading['energy_consumption']}kWh | Status: {reading['status']}")
            
            future = producer.send("sensors", value=reading)
            record_metadata = future.get(timeout=10)
            print(f"[Producer] ✓ Sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
            print()
            
            producer.flush()
            count += 1
            
            # Variable sleep time for realistic patterns
            sleep_time = random.uniform(0.5, 1.5)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\n[Producer] 🛑 Shutting down gracefully...")
    except Exception as e:
        print(f"[Producer ERROR] ❌ {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'producer' in locals():
            producer.close()


if __name__ == "__main__":
    run_producer()