#!/bin/bash
set -ex  # Exit on error, print commands

# Kafka settings
TOPIC="plain-topic"
BOOTSTRAP_SERVERS="kafka:9092"
CONTAINER_NAME="docker_glue_pyspark_multi_orchestrator-kafka-1"  # Replace with your actual container name

# Function to generate a random JSON event
generate_event() {
  id=$((RANDOM % 100 + 1))
  name=$(cat /usr/share/dict/words | shuf -n 1 | awk '{print toupper(substr($0,1,1)) tolower(substr($0,2))}')
  amount=$((RANDOM % 201))sgyf
  echo "{\"id\": $id, \"name\": \"$name\", \"amount\": $amount}"
}

# Get the number of iterations from the command line argument
iterations=$1

# Check if the argument is provided
if [ -z "$iterations" ]; then
  echo "Usage: $0 <number_of_iterations>"
  exit 1
fi

# Generate and send events for the specified number of iterations
for i in $(seq 1 $iterations); do
  event_data=$(generate_event)
  echo "Sending event: $event_data"

  # Execute the kafka-console-producer command inside the Docker container
  docker exec -i "$CONTAINER_NAME" \
    kafka-console-producer --topic "$TOPIC" --bootstrap-server "$BOOTSTRAP_SERVERS" \
    <<< "$event_data"
  echo "Event sent (exit code: $?)" 
  sleep 1  # Adjust the sleep interval as needed
done