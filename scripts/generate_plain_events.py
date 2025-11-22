import random
import subprocess
import time
import sys
import json
import names

# Kafka settings
TOPIC = "plain-topic"
BOOTSTRAP_SERVERS = "kafka:9092"
CONTAINER_NAME = "docker_glue_pyspark_demo-kafka-1"  # Replace with your actual container name

# Function to generate a random JSON event
def generate_event():
    id = random.randint(1, 100)
    name = names.get_full_name()
    amount = random.randint(0, 200)
    event = {"id": id, "name": name, "amount": amount}
    return event

# Get the number of iterations from the command line argument
if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} <number_of_iterations>")
    sys.exit(1)

iterations = int(sys.argv[1])

# Generate and send events for the specified number of iterations
for i in range(1, iterations + 1):
    event_data = generate_event()
    event_data_str = json.dumps(event_data)
    print(f"Sending event: {event_data_str}")

    # Execute the kafka-console-producer command inside the Docker container
    command = [
        "docker", "exec", "-i", CONTAINER_NAME,
        "kafka-console-producer", "--topic", TOPIC, "--bootstrap-server", BOOTSTRAP_SERVERS
    ]
    result = subprocess.run(command, input=event_data_str, text=True)
    print(f"Event sent (exit code: {result.returncode})")
    time.sleep(1)  # Adjust the sleep interval as needed
