import json
import psycopg2
from kafka import KafkaConsumer

def run_consumer():
    """Consumes sensor readings from Kafka and inserts them into PostgreSQL."""
    try:
        print("[Consumer] 🎧 Starting IoT Sensor Data Consumer")
        print("[Consumer] Connecting to Kafka at localhost:9092...")
        
        consumer = KafkaConsumer(
            "sensors",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id="sensors-consumer-group",
        )
        print("[Consumer] ✓ Connected to Kafka successfully!")
        
        print("[Consumer] Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="kafka_db",
            user="kafka_user",
            password="kafka_password",
            host="localhost",
            port="5432",
        )
        conn.autocommit = True
        cur = conn.cursor()
        print("[Consumer] ✓ Connected to PostgreSQL successfully!")

        # Create main sensor readings table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_readings (
                sensor_id VARCHAR(50) PRIMARY KEY,
                room_id VARCHAR(100),
                building VARCHAR(50),
                floor VARCHAR(50),
                room_type VARCHAR(50),
                temperature NUMERIC(5, 2),
                humidity NUMERIC(5, 2),
                co2 NUMERIC(7, 2),
                occupancy INTEGER,
                energy_consumption NUMERIC(10, 2),
                status VARCHAR(50),
                timestamp TIMESTAMP
            );
            """
        )
        print("[Consumer] ✓ Table 'sensor_readings' ready.")
        
        # Create aggregated metrics table for Flink output
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_aggregates (
                id SERIAL PRIMARY KEY,
                window_start TIMESTAMP,
                window_end TIMESTAMP,
                building VARCHAR(50),
                floor VARCHAR(50),
                avg_temperature NUMERIC(5, 2),
                avg_humidity NUMERIC(5, 2),
                avg_co2 NUMERIC(7, 2),
                total_occupancy INTEGER,
                total_energy NUMERIC(10, 2),
                reading_count INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        print("[Consumer] ✓ Table 'sensor_aggregates' ready.")
        
        # Create anomalies table for ML model output
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_anomalies (
                id SERIAL PRIMARY KEY,
                sensor_id VARCHAR(50),
                room_id VARCHAR(100),
                anomaly_type VARCHAR(100),
                anomaly_score NUMERIC(5, 4),
                temperature NUMERIC(5, 2),
                humidity NUMERIC(5, 2),
                co2 NUMERIC(7, 2),
                energy_consumption NUMERIC(10, 2),
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        print("[Consumer] ✓ Table 'sensor_anomalies' ready.")
        
        print("[Consumer] 🎧 Listening for sensor readings...\n")

        message_count = 0
        for message in consumer:
            try:
                reading = message.value
                
                insert_query = """
                    INSERT INTO sensor_readings (
                        sensor_id, room_id, building, floor, room_type,
                        temperature, humidity, co2, occupancy, 
                        energy_consumption, status, timestamp
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sensor_id) DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        reading["sensor_id"],
                        reading["room_id"],
                        reading["building"],
                        reading["floor"],
                        reading["room_type"],
                        reading["temperature"],
                        reading["humidity"],
                        reading["co2"],
                        reading["occupancy"],
                        reading["energy_consumption"],
                        reading["status"],
                        reading["timestamp"],
                    ),
                )
                
                message_count += 1
                status_emoji = "🟢" if reading["status"] == "Normal" else "🟡" if reading["status"] == "Maintenance" else "🔴"
                
                print(f"[Consumer] {status_emoji} #{message_count} Stored reading from {reading['room_id']}")
                print(f"           🌡️  {reading['temperature']}°C | 💧 {reading['humidity']}% | " +
                      f"🫁 {reading['co2']}ppm | ⚡ {reading['energy_consumption']}kWh")
                
            except Exception as e:
                print(f"[Consumer ERROR] ❌ Failed to process message: {e}")
                continue
                
    except KeyboardInterrupt:
        print("\n[Consumer] 🛑 Shutting down gracefully...")
    except Exception as e:
        print(f"[Consumer ERROR] ❌ {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    run_consumer()