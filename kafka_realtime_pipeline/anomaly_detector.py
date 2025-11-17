"""
Sequential Modeling: Real-Time Anomaly Detection for IoT Sensors
Uses Isolation Forest and Statistical Methods for anomaly detection
"""

import time
import numpy as np
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')


class IoTAnomalyDetector:
    """
    Real-time anomaly detector for IoT sensor data.
    Uses multiple detection methods:
    1. Isolation Forest for multivariate anomalies
    2. Statistical thresholds for individual sensors
    3. Temporal pattern detection
    """
    
    def __init__(self):
        self.model = IsolationForest(
            contamination=0.1,  # Expected proportion of anomalies
            random_state=42,
            n_estimators=100
        )
        self.scaler = StandardScaler()
        self.is_trained = False
        
        # Statistical thresholds (will be updated based on data)
        self.thresholds = {
            'temperature': {'min': 15, 'max': 35},
            'humidity': {'min': 20, 'max': 80},
            'co2': {'min': 300, 'max': 1200},
            'energy_consumption': {'min': 0, 'max': 300}
        }
        
    def train_model(self, df: pd.DataFrame):
        """Train the Isolation Forest model on historical data."""
        if len(df) < 50:
            print("[Anomaly Detector] ⚠️  Insufficient data for training (need >= 50 samples)")
            return False
            
        # Select features for anomaly detection
        features = ['temperature', 'humidity', 'co2', 'occupancy', 'energy_consumption']
        X = df[features].values
        
        # Remove any NaN values
        X = X[~np.isnan(X).any(axis=1)]
        
        if len(X) < 50:
            print("[Anomaly Detector] ⚠️  Insufficient clean data for training")
            return False
        
        # Fit scaler and model
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        
        # Update statistical thresholds based on percentiles
        self.thresholds['temperature'] = {
            'min': df['temperature'].quantile(0.01),
            'max': df['temperature'].quantile(0.99)
        }
        self.thresholds['humidity'] = {
            'min': df['humidity'].quantile(0.01),
            'max': df['humidity'].quantile(0.99)
        }
        self.thresholds['co2'] = {
            'min': df['co2'].quantile(0.01),
            'max': df['co2'].quantile(0.99)
        }
        self.thresholds['energy_consumption'] = {
            'min': df['energy_consumption'].quantile(0.01),
            'max': df['energy_consumption'].quantile(0.99)
        }
        
        self.is_trained = True
        print(f"[Anomaly Detector] ✓ Model trained on {len(X)} samples")
        return True
    
    def detect_statistical_anomalies(self, reading: dict) -> list:
        """Detect anomalies using statistical thresholds."""
        anomalies = []
        
        # Temperature anomalies
        if reading['temperature'] < self.thresholds['temperature']['min']:
            anomalies.append({
                'type': 'Temperature - Too Low',
                'score': abs(reading['temperature'] - self.thresholds['temperature']['min']) / 10
            })
        elif reading['temperature'] > self.thresholds['temperature']['max']:
            anomalies.append({
                'type': 'Temperature - Too High',
                'score': abs(reading['temperature'] - self.thresholds['temperature']['max']) / 10
            })
        
        # Humidity anomalies
        if reading['humidity'] < self.thresholds['humidity']['min']:
            anomalies.append({
                'type': 'Humidity - Too Low',
                'score': abs(reading['humidity'] - self.thresholds['humidity']['min']) / 20
            })
        elif reading['humidity'] > self.thresholds['humidity']['max']:
            anomalies.append({
                'type': 'Humidity - Too High',
                'score': abs(reading['humidity'] - self.thresholds['humidity']['max']) / 20
            })
        
        # CO2 anomalies
        if reading['co2'] > self.thresholds['co2']['max']:
            anomalies.append({
                'type': 'CO2 - Dangerous Levels',
                'score': min(1.0, abs(reading['co2'] - self.thresholds['co2']['max']) / 500)
            })
        
        # Energy anomalies
        if reading['energy_consumption'] > self.thresholds['energy_consumption']['max']:
            anomalies.append({
                'type': 'Energy - Excessive Consumption',
                'score': min(1.0, abs(reading['energy_consumption'] - self.thresholds['energy_consumption']['max']) / 100)
            })
        
        return anomalies
    
    def detect_ml_anomalies(self, reading: dict) -> list:
        """Detect anomalies using Isolation Forest."""
        if not self.is_trained:
            return []
        
        features = ['temperature', 'humidity', 'co2', 'occupancy', 'energy_consumption']
        X = np.array([[reading[f] for f in features]])
        
        # Scale features
        X_scaled = self.scaler.transform(X)
        
        # Predict (-1 for anomaly, 1 for normal)
        prediction = self.model.predict(X_scaled)[0]
        
        if prediction == -1:
            # Get anomaly score (more negative = more anomalous)
            score = -self.model.score_samples(X_scaled)[0]
            return [{
                'type': 'Multivariate Anomaly',
                'score': min(1.0, score)
            }]
        
        return []
    
    def detect_temporal_anomalies(self, current: dict, history: pd.DataFrame) -> list:
        """Detect anomalies based on temporal patterns."""
        anomalies = []
        
        if len(history) < 5:
            return anomalies
        
        # Check for sudden spikes
        recent = history.tail(5)
        
        # Temperature spike detection
        temp_change = current['temperature'] - recent['temperature'].mean()
        if abs(temp_change) > 5:
            anomalies.append({
                'type': 'Temperature - Sudden Change',
                'score': min(1.0, abs(temp_change) / 10)
            })
        
        # Energy spike detection
        energy_change = current['energy_consumption'] - recent['energy_consumption'].mean()
        if abs(energy_change) > 50:
            anomalies.append({
                'type': 'Energy - Sudden Spike',
                'score': min(1.0, abs(energy_change) / 100)
            })
        
        return anomalies


def store_anomaly(cursor, reading: dict, anomaly: dict):
    """Store detected anomaly in database."""
    insert_query = """
        INSERT INTO sensor_anomalies (
            sensor_id, room_id, anomaly_type, anomaly_score,
            temperature, humidity, co2, energy_consumption
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.execute(insert_query, (
        reading['sensor_id'],
        reading['room_id'],
        anomaly['type'],
        round(anomaly['score'], 4),
        reading['temperature'],
        reading['humidity'],
        reading['co2'],
        reading['energy_consumption']
    ))


def run_anomaly_detector():
    """
    Main anomaly detection loop.
    Continuously monitors sensor readings and detects anomalies.
    """
    
    print("[Anomaly Detector] 🤖 Starting Sequential Anomaly Detection System")
    print("[Anomaly Detector] Connecting to PostgreSQL...")
    
    conn = psycopg2.connect(
        dbname="kafka_db",
        user="kafka_user",
        password="kafka_password",
        host="localhost",
        port="5432"
    )
    conn.autocommit = True
    
    print("[Anomaly Detector] ✓ Connected to PostgreSQL")
    
    detector = IoTAnomalyDetector()
    last_training_time = None
    training_interval = timedelta(minutes=5)  # Retrain every 5 minutes
    
    print("[Anomaly Detector] 🔍 Monitoring for anomalies...")
    print()
    
    iteration = 0
    
    while True:
        try:
            cursor = conn.cursor()
            
            # Fetch historical data for training/context
            historical_query = """
                SELECT temperature, humidity, co2, occupancy, energy_consumption
                FROM sensor_readings
                ORDER BY timestamp DESC
                LIMIT 1000
            """
            
            df_history = pd.read_sql_query(historical_query, conn)
            
            # Train or retrain model periodically
            current_time = datetime.now()
            if last_training_time is None or (current_time - last_training_time) > training_interval:
                if len(df_history) >= 50:
                    print("[Anomaly Detector] 🎓 Training model on recent data...")
                    detector.train_model(df_history)
                    last_training_time = current_time
                    print()
            
            # Fetch recent unprocessed readings
            recent_query = """
                SELECT sensor_id, room_id, building, floor, room_type,
                       temperature, humidity, co2, occupancy, energy_consumption,
                       timestamp
                FROM sensor_readings
                ORDER BY timestamp DESC
                LIMIT 20
            """
            
            df_recent = pd.read_sql_query(recent_query, conn)
            
            if len(df_recent) == 0:
                print("[Anomaly Detector] ⏳ Waiting for data...")
                time.sleep(5)
                continue
            
            # Process each reading
            anomalies_detected = 0
            
            for idx, row in df_recent.iterrows():
                reading = row.to_dict()
                
                all_anomalies = []
                
                # Statistical anomaly detection
                stat_anomalies = detector.detect_statistical_anomalies(reading)
                all_anomalies.extend(stat_anomalies)
                
                # ML-based anomaly detection
                ml_anomalies = detector.detect_ml_anomalies(reading)
                all_anomalies.extend(ml_anomalies)
                
                # Temporal anomaly detection
                temporal_anomalies = detector.detect_temporal_anomalies(reading, df_history)
                all_anomalies.extend(temporal_anomalies)
                
                # Store anomalies
                for anomaly in all_anomalies:
                    store_anomaly(cursor, reading, anomaly)
                    anomalies_detected += 1
                    
                    print(f"[Anomaly Detector] 🚨 ANOMALY DETECTED!")
                    print(f"                   Room: {reading['room_id']}")
                    print(f"                   Type: {anomaly['type']}")
                    print(f"                   Score: {anomaly['score']:.4f}")
                    print(f"                   Readings: 🌡️ {reading['temperature']}°C | " +
                          f"💧 {reading['humidity']}% | 🫁 {reading['co2']}ppm | " +
                          f"⚡ {reading['energy_consumption']}kWh")
                    print()
            
            iteration += 1
            if iteration % 10 == 0:
                print(f"[Anomaly Detector] ✓ Processed {iteration} batches, " +
                      f"detected {anomalies_detected} anomalies in this batch")
            
            cursor.close()
            
            # Wait before next check
            time.sleep(10)
            
        except KeyboardInterrupt:
            print("\n[Anomaly Detector] 🛑 Shutting down gracefully...")
            break
            
        except Exception as e:
            print(f"[Anomaly Detector ERROR] ❌ {e}")
            import traceback
            traceback.print_exc()
            time.sleep(10)
    
    conn.close()


if __name__ == "__main__":
    run_anomaly_detector()
