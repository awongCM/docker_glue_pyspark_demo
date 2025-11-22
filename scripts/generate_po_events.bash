#!/bin/bash
set -ex  # Exit on error, print commands

# Kafka settings
TOPIC="purchase-order"
BOOTSTRAP_SERVERS="kafka:9092"
CONTAINER_NAME="docker_glue_pyspark_demo-kafka-1"
NUM_EVENTS=$1  # Number of events to generate, passed as a parameter

if [ -z "$NUM_EVENTS" ]; then
  echo "Usage: $0 <number_of_events>"
  exit 1
fi

# Function to generate a random purchase order event
generate_purchase_order() {
  contact_name=$(shuf -n 1 /usr/share/dict/words)
  contact_email="${contact_name}@example.com"
  contact_phone="123-456-7890"
  po_number=$((RANDOM % 10000 + 1))
  items_count=$((RANDOM % 5 + 1))  # Random number of items between 1 and 5
  items="["
  meat_types=("Beef" "Chicken" "Fish")
  meat_grades=("Prime", "Choice", "Standard")

  for ((i=0; i<items_count; i++)); do
    meat_type=${meat_types[$RANDOM % ${#meat_types[@]}]}
    meat_grade=${meat_grades[$RANDOM  % ${#meat_grades[@]}]}
    sku="SKU$(shuf -i 1000-9999 -n 1)"
    description="${meat_type} ${meat_grade} $(shuf -i 1-100 -n 1)"
    weight_quantity=$((RANDOM % 40001))  # Random weight quantity between 0 and 40000
    price=$(printf "%.2f" "$(echo "scale=2; $RANDOM % 76" | bc)")  # Random price between 0 and 75
    items+="{\"sku\": \"$sku\", \"description\": \"$description\", \"weight_quantity\": $weight_quantity, \"price\": $price}"
    if [ $i -lt $((items_count - 1)) ]; then
      items+=","
    fi
  done

  items+="]"
  subtotal=$(printf "%.2f" "$(echo "scale=2; $(echo $items | jq -r '.[] | .weight_quantity * .price' | awk '{s+=$1} END {print s}')" | bc)")
  taxes=$(printf "%.2f" "$(echo "scale=2; $subtotal * 0.1" | bc)")  # Assume 10% tax
  total=$(printf "%.2f" "$(echo "scale=2; $subtotal + $taxes" | bc)")
  payment_due_date=$(date -d "$((RANDOM % 30 + 1)) days" +"%Y-%m-%d")

  echo "{\"contact_info\": {\"name\": \"$contact_name\", \"email\": \"$contact_email\", \"phone\": \"$contact_phone\"}, \"po_number\": \"$po_number\", \"items\": $items, \"subtotal\": $subtotal, \"taxes\": $taxes, \"total\": $total, \"payment_due_date\": \"$payment_due_date\"}"
}

# Generate and send the specified number of purchase order events to Kafka
for ((i=0; i<NUM_EVENTS; i++)); do
  event_data=$(generate_purchase_order)
  echo "Sending event: $event_data"

  # Execute the kafka-console-producer command inside the Docker container
  docker exec -i "$CONTAINER_NAME" \
    kafka-console-producer --topic "$TOPIC" --bootstrap-server "$BOOTSTRAP_SERVERS" \
    <<< "$event_data"
  echo "Event sent (exit code: $?)" 
  sleep 1  # Adjust the sleep interval as needed
done