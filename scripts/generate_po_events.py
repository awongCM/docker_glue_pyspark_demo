import random
import subprocess
import time
import sys
import json
from datetime import datetime, timedelta
import names

# Kafka settings
TOPIC = "purchase-order"
BOOTSTRAP_SERVERS = "kafka:9092"
CONTAINER_NAME = "docker_glue_pyspark_demo-kafka-1"

def generate_purchase_order():
    name = names.get_full_name()
    contact_name = name
    contact_email = f"{'.'.join(name.split(" ")).lower()}@example.com"
    contact_phone = "123-456-7890"
    po_number = random.randint(1, 10000)
    items_count = random.randint(1, 5)

    meat_types = ["Beef", "Chicken", "Fish"]
    meat_grades = ["Prime", "Choice", "Standard"]
    items = []

    for _ in range(items_count):
        meat_type = random.choice(meat_types)
        meat_grade = random.choice(meat_grades)
        sku = f"SKU{random.randint(1000, 9999)}"
        description = f"{meat_type} {meat_grade} {random.randint(1, 100)}"
        weight_quantity = random.randint(0, 40000)
        price = round(random.uniform(0, 75), 2)
        items.append({
            "sku": sku,
            "description": description,
            "weight_quantity": weight_quantity,
            "price": price
        })

    subtotal = round(sum(item["weight_quantity"] * item["price"] for item in items), 2)
    taxes = round(subtotal * 0.1, 2)
    total = round(subtotal + taxes, 2)
    payment_due_date = (datetime.now() + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")

    return {
        "contact_info": {
            "name": contact_name,
            "email": contact_email,
            "phone": contact_phone
        },
        "po_number": po_number,
        "items": items,
        "subtotal": subtotal,
        "taxes": taxes,
        "total": total,
        "payment_due_date": payment_due_date
    }

if len(sys.argv) != 2:
    print(f"Usage: {sys.argv[0]} <number_of_events>")
    sys.exit(1)

num_events = int(sys.argv[1])

for _ in range(num_events):
    event_data = generate_purchase_order()
    event_data_str = json.dumps(event_data)
    print(f"Sending event: {event_data_str}")

    command = [
        "docker", "exec", "-i", CONTAINER_NAME,
        "kafka-console-producer", "--topic", TOPIC, "--bootstrap-server", BOOTSTRAP_SERVERS
    ]
    result = subprocess.run(command, input=event_data_str, text=True)
    print(f"Event sent (exit code: {result.returncode})")
    time.sleep(1)  # Adjust the sleep interval as needed
