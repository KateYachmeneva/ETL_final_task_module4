import csv
import random
from datetime import datetime, timedelta

rows_count = 300_000

regions = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW"]
campaign_types = ["credit_card_offer", "cash_loan", "mortgage", "car_loan"]
call_statuses = ["answered", "missed", "failed"]
client_responses = ["interested", "not_interested", "callback", "no_answer"]

start_date = datetime(2026, 5, 1, 8, 0, 0)

with open("transactions_v2.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "call_id",
        "call_time",
        "client_id",
        "region_code",
        "campaign_type",
        "call_status",
        "client_response",
        "duration_sec",
        "follow_up_required",
    ])

    for i in range(rows_count):
        call_time = start_date + timedelta(seconds=random.randint(0, 2_500_000))

        writer.writerow([
            f"call_20260501_{i:06d}",
            call_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            f"client_{random.randint(1000, 9999)}",
            random.choice(regions),
            random.choice(campaign_types),
            random.choice(call_statuses),
            random.choice(client_responses),
            random.randint(10, 600),
            random.choice(["true", "false"]),
        ])

print("transactions_v2.csv generated")
