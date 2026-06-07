import os
import time
import random
import pandas as pd
from datetime import datetime

input_folder = "data/input_stream"

categories = [
    "books",
    "electronics",
    "food",
    "clothes"
]

statuses = [
    "paid",
    "pending",
    "cancelled"
]

counter = 100

while True:
    data = []

    for i in range(5):
        row = {
            "event_time": datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "user_id": f"u{counter}",
            "category": random.choice(categories),
            "amount": round(
                random.uniform(10, 300),
                2
            ),
            "status": random.choice(statuses)
        }

        data.append(row)
        counter += 1

    df = pd.DataFrame(data)

    filename = (
        f"{input_folder}/events_"
        f"{int(time.time())}.csv"
    )

    df.to_csv(filename, index=False)

    print(f"dodano plik: {filename}")

    time.sleep(5)