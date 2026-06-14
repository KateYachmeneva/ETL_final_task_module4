import csv
import random
from datetime import datetime, timedelta

rows_count = 600_000

regions = ["DE-HE", "DE-BE", "DE-BY", "DE-HH", "DE-NW"]
product_types = ["cash_loan", "credit_card", "mortgage", "car_loan"]
risk_levels = ["low", "medium", "high"]
decision_statuses = ["approved", "rejected", "manual_review"]
channels = ["mobile", "web", "office", "partner"]

start_date = datetime(2026, 5, 1, 8, 0, 0)

with open("credit_applications.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)

    writer.writerow([
        "application_id",
        "event_time",
        "customer_id",
        "region_code",
        "product_type",
        "requested_amount",
        "term_months",
        "credit_score",
        "risk_level",
        "decision_status",
        "approved_amount",
        "channel",
        "employee_review_flag",
        "processing_time_sec",
    ])

    for i in range(rows_count):
        decision_status = random.choice(decision_statuses)
        requested_amount = random.randint(1000, 50000)

        if decision_status == "approved":
            approved_amount = requested_amount
        elif decision_status == "manual_review":
            approved_amount = random.randint(0, requested_amount)
        else:
            approved_amount = 0

        event_time = start_date + timedelta(seconds=random.randint(0, 2_500_000))

        writer.writerow([
            f"app_20260501_{i:06d}",
            event_time.strftime("%Y-%m-%d %H:%M:%S"),
            f"cust_{random.randint(10000, 99999)}",
            random.choice(regions),
            random.choice(product_types),
            requested_amount,
            random.choice([6, 12, 24, 36, 48, 60]),
            random.randint(300, 900),
            random.choice(risk_levels),
            decision_status,
            approved_amount,
            random.choice(channels),
            random.choice(["true", "false"]),
            random.randint(5, 300),
        ])

print("credit_applications.csv generated")