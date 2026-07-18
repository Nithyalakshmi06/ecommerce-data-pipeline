import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer
from faker import Faker

fake = Faker()

PRODUCT_CATALOG = [
    {"product_id": "P1001", "name": "Wireless Mouse", "category": "Electronics", "price": 799.00},
    {"product_id": "P1002", "name": "Bluetooth Headphones", "category": "Electronics", "price": 1999.00},
    {"product_id": "P1003", "name": "Cotton T-Shirt", "category": "Apparel", "price": 499.00},
    {"product_id": "P1004", "name": "Running Shoes", "category": "Footwear", "price": 2499.00},
    {"product_id": "P1005", "name": "Yoga Mat", "category": "Fitness", "price": 899.00},
]

CITIES = ["Coimbatore", "Chennai", "Bengaluru", "Hyderabad", "Mumbai"]


def build_order_event():
    product = random.choice(PRODUCT_CATALOG)
    quantity = random.randint(1, 4)
    return {
        "order_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "product_id": product["product_id"],
        "product_name": product["name"],
        "category": product["category"],
        "unit_price": product["price"],
        "quantity": quantity,
        "total_amount": round(product["price"] * quantity, 2),
        "city": random.choice(CITIES),
        "payment_method": random.choice(["UPI", "Credit Card", "COD"]),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Producer started. Sending events to 'orders' topic. Ctrl+C to stop.\n")
    count = 0
    try:
        while True:
            event = build_order_event()
            producer.send("orders", value=event)
            count += 1
            print(f"[{count}] Sent: {event['product_name']} x{event['quantity']} "
                  f"-> Rs.{event['total_amount']} ({event['city']})")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print(f"\nStopped. Total sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
