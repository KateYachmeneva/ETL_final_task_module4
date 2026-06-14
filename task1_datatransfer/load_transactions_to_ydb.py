import csv
import subprocess
import ydb
import time

YDB_ENDPOINT = "grpcs://ydb.serverless.yandexcloud.net:2135"

YDB_DATABASE = "/ru-central1/b1gptv7g1vlqggpah3i7/etnljhqujk3hopdo4fqs"

CSV_FILE = "transactions_v2.csv"
BATCH_SIZE = 50


def get_yc_token() -> str:
    result = subprocess.run(
        ["yc", "iam", "create-token"],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def get_driver():
    token = get_yc_token()

    driver = ydb.Driver(
        endpoint=YDB_ENDPOINT,
        database=YDB_DATABASE,
        credentials=ydb.AccessTokenCredentials(token),
    )

    driver.wait(fail_fast=True, timeout=20)
    return driver


def escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def upload_batch(session, rows):
    values = []

    for row in rows:
        follow_up_required = (
            "true"
            if row["follow_up_required"].strip().lower() == "true"
            else "false"
        )

        call_time = row["call_time"]

        values.append(f"""(
            "{escape(row["call_id"])}",
            Datetime("{call_time}"),
            "{escape(row["client_id"])}",
            "{escape(row["region_code"])}",
            "{escape(row["campaign_type"])}",
            "{escape(row["call_status"])}",
            "{escape(row["client_response"])}",
            {int(row["duration_sec"])},
            {follow_up_required}
        )""")

    query = f"""
    UPSERT INTO transactions_v2
    (
        call_id,
        call_time,
        client_id,
        region_code,
        campaign_type,
        call_status,
        client_response,
        duration_sec,
        follow_up_required
    )
    VALUES
    {",".join(values)};
    """

    session.transaction().execute(query, commit_tx=True)


def main():
    driver = get_driver()

    with ydb.SessionPool(driver) as pool:
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            batch = []
            total = 0

            for row in reader:
                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    pool.retry_operation_sync(
                        lambda session: upload_batch(session, batch)
                    )
                    total += len(batch)
                    print(f"Uploaded rows: {total}")
                    batch = []
                    time.sleep(0.2)

            if batch:
                pool.retry_operation_sync(
                    lambda session: upload_batch(session, batch)
                )
                total += len(batch)
                print(f"Uploaded rows: {total}")
                time.sleep(0.2)

    driver.stop()
    print("Done")


if __name__ == "__main__":
    main()