import threading
import json
import time
from kafka import KafkaProducer
import os
def check_arrival(train_id, arrival_time, trip_id):
    # Kafka configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'railway'

    # Create Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    while True:
        # Check if the sensor response file exists
        if os.path.exists('Job/sensor_response.json'):
            # Simulated sensor response from a JSON file
            with open('Job/sensor_response.json') as f:
                sensor_response = json.load(f)

            # Check if the train has arrived
            if sensor_response.get('train_id') == train_id:
                actual_arrival_time = time.time()
                delay = int((actual_arrival_time - arrival_time) / 60)
                stationId = sensor_response.get('stationId')

                # Create a dictionary with the data
                data = {
                    "tripId": trip_id,
                    "duration": delay,
                    "stationId": stationId
                }

                # Convert the dictionary to JSON
                json_data = json.dumps(data)

                # Send the JSON data to the Kafka topic
                producer.send(topic, value=json_data.encode())

                # Terminate the thread
                return

        # Wait for 10 seconds before checking again
        time.sleep(10)

# Example usage
train_id = 'T123'
arrival_time = time.time() + 1  # Set arrival time 5 seconds from now
trip_id = 1

# Start the thread
thread = threading.Thread(target=check_arrival, args=(train_id, arrival_time, trip_id))
thread.start()

# Wait for the thread to finish
thread.join()

print("Thread finished!")