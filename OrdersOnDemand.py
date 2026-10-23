import json
import uuid
import os
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": os.getenv("BOOTSTRAP"),
    "client.id": os.getenv("CLIENT_ID"),
})


def create_order():
    print("\n--- Create New Order ---")

    product_id = input("Product ID: ").strip()
    destination = input("Destination governorate: ").strip()

    try:
        quantity = float(input("Quantity in kg: "))
    except ValueError:
        print("Quantity must be a number.")
        return None

    priority = input("Priority (NORMAL/URGENT): ").strip().upper()

    if not product_id or not destination:
        print("Product ID and destination are required.")
        return None

    if quantity <= 0:
        print("Quantity must be greater than zero.")
        return None

    if priority not in ("NORMAL", "URGENT"):
        print("Priority must be NORMAL or URGENT.")
        return None

    now = datetime.now(timezone.utc)

    # Demo deadlines; adjust to your business rules
    deadline_hours = 2 if priority == "URGENT" else 168

    order = {
        "order_id": str(uuid.uuid4()),
        "product_id": product_id,
        "destination_governorate": destination,
        "quantity_kg": quantity,
        "priority": priority,
        "created_at": now.isoformat(),
        "delivery_deadline": (
            now + timedelta(hours=deadline_hours)
        ).isoformat(),
        "status": "NEW",
    }

    return order


def main():
    print("Order Producer started.")

    try:
        while True:
            order = create_order()

            if order is not None:
                producer.produce(
                    topic="new-orders",
                    key=order["order_id"],
                    value=json.dumps(order),
                )

                producer.flush()

                print("\nOrder sent successfully!")
                print(json.dumps(order, indent=2))

            again = input(
                "\nCreate another order? (yes/no): "
            ).strip().lower()

            if again not in ("yes", "y"):
                break

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        print("Order Producer stopped.")


if __name__ == "__main__":
    main()