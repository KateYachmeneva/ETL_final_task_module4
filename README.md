## Задание 1. Работа с Yandex Data Transfer

### Цель

Цель задания — перенести данные из Managed Service for YDB в Yandex Object Storage с использованием сервиса Yandex Data Transfer.

### Что было сделано

В рамках задания была создана serverless-база данных YDB:

```text
etl-ydb-transactions
```

В базе была создана строковая таблица:

```text
transactions_v2
```

Структура таблицы:

```text
call_id
call_time
client_id
region_code
campaign_type
call_status
client_response
duration_sec
follow_up_required
```

Первичным ключом таблицы является поле:

```text
call_id
```

Для загрузки данных был подготовлен CSV-файл:

```text
transactions_v2.csv
```

Размер файла составил более 30 МБ, что соответствует требованию задания.

Данные были загружены в таблицу `transactions_v2`. Проверка загрузки была выполнена с помощью SQL-запроса:

```sql
SELECT COUNT(*) AS cnt
FROM transactions_v2;
```

После этого был создан трансфер:

```text
ydb-to-s3-transactions
```

Источник трансфера:

```text
YDB / etl-ydb-transactions / transactions_v2
```

Приёмник трансфера:

```text
Yandex Object Storage / dataproc-bucket-kate-2026
```

Трансфер был запущен для копирования данных из YDB в Object Storage.

### Скриншоты по заданию 1

Скриншоты находятся в папке:

```text
task1_datatransfer/screenshots/
```

Список скриншотов:

```text
01_ydb_database.png
```

База YDB `etl-ydb-transactions` создана и находится в статусе `Running`.

```text
02_ydb_table_empty.png
```

Таблица `transactions_v2` создана, видна структура колонок.

```text
03_csv_transactions_v2_preview.png
```

Подготовленный CSV-файл `transactions_v2.csv` размером более 30 МБ.

```text
04_ydb_table_transactions_v2.png
```

Данные загружены в таблицу `transactions_v2`, видны первые записи.

```text
05_ydb_select_count.png
```

Результат выполнения запроса `SELECT COUNT(*) FROM transactions_v2`, подтверждающий, что данные загружены в таблицу.

```text
06_datatransfer_source_endpoint.png
```

Создан endpoint-источник `source-ydb-transactions` для YDB.

```text
07_datatransfer_target_endpoint.png
```

Создан endpoint-приёмник `target-s3-transactions` для Object Storage.

```text
08_datatransfer_created.png
```

Трансфер `ydb-to-s3-transactions` создан.

```text
09_datatransfer_copying.png
```

Трансфер `ydb-to-s3-transactions` выполняется, статус «Копируется».

```text
10_datatransfer_completed.png
```

Трансфер `ydb-to-s3-transactions` успешно выполнен.

```text
11_object_storage_transactions_folder.png
```

В Object Storage появилась папка `transactions_v2` с результатами трансфера.

```text
12_object_storage_transactions_output.png
```

В Object Storage появились CSV-файлы, выгруженные из YDB через Data Transfer.

```text
13_downloads_csv_files.png
```

CSV-файлы из Object Storage скачаны для проверки.

```text
14_csv_numbers_preview.png
```

Содержимое выгруженного CSV-файла просмотрено в Apple Numbers.

### Результат

В результате выполнения задания данные из таблицы `transactions_v2` в Managed Service for YDB были успешно перенесены в Yandex Object Storage с помощью Yandex Data Transfer.

---

## Задание 2. Автоматизация работы с Yandex Data Processing при помощи Apache Airflow

### Цель

Цель задания — автоматизировать обработку CSV-файла с помощью Apache Airflow и Yandex Data Processing.

DAG в Apache Airflow должен автоматически выполнить следующие действия:

```text
1. Создать временный кластер Yandex Data Processing.
2. Запустить PySpark-задание.
3. Удалить временный кластер Yandex Data Processing.
```

### Что было сделано

Для задания был подготовлен CSV-файл:

```text
credit_applications.csv
```

Размер файла составил более 50 МБ, что соответствует требованию задания.

Структура входного файла:

```text
application_id
event_time
customer_id
region_code
product_type
requested_amount
term_months
credit_score
risk_level
decision_status
approved_amount
channel
employee_review_flag
processing_time_sec
```

Файл был загружен в Object Storage:

```text
s3a://dataproc-bucket-kate-2026/task2/input/credit_applications.csv
```

Также был подготовлен PySpark-скрипт:

```text
process_credit_applications.py
```

Скрипт выполняет обработку входного CSV-файла и сохраняет результат в Object Storage.

PySpark-скрипт был загружен в бакет:

```text
s3a://dataproc-bucket-kate-2026/task2/scripts/process_credit_applications.py
```

Для автоматизации был создан DAG:

```text
DATA_PROCESSING_CREDIT_APPLICATIONS
```

DAG-файл расположен в Object Storage:

```text
s3a://dataproc-bucket-kate-2026/task2/dags/data_processing_dag.py
```

DAG содержит три задачи:

```text
create_dataproc_cluster
run_pyspark_job
delete_dataproc_cluster
```

Для удаления временного кластера используется `TriggerRule.ALL_DONE`, чтобы кластер удалялся даже в случае ошибки на этапе обработки.

### Схема работы DAG

```text
Object Storage CSV
        ↓
Apache Airflow DAG
        ↓
create_dataproc_cluster
        ↓
run_pyspark_job
        ↓
delete_dataproc_cluster
        ↓
Object Storage output
```

### Результат выполнения

DAG был успешно запущен в Apache Airflow.

Все три задачи завершились успешно:

```text
create_dataproc_cluster — success
run_pyspark_job — success
delete_dataproc_cluster — success
```

Таким образом, был автоматически создан временный кластер Yandex Data Processing, выполнено PySpark-задание и затем временный кластер был удалён.

### Скриншоты по заданию 2

Скриншоты находятся в папке:

```text
task2_airflow_dataproc/screenshots/
```

Список скриншотов:

```text
01_input_file_in_object_storage.png
```

Входной файл `credit_applications.csv` загружен в `task2/input/` в Object Storage.

```text
02_pyspark_script_in_object_storage.png
```

PySpark-скрипт `process_credit_applications.py` загружен в `task2/scripts/`.

```text
03_dag_file_in_object_storage.png
```

DAG-файл `data_processing_dag.py` загружен в `task2/dags/`.

```text
04_airflow_cluster.png
```

Кластер Managed Service for Apache Airflow `airflow348` создан и находится в статусе `Alive`.

```text
05_airflow_dag_success.png
```

DAG `DATA_PROCESSING_CREDIT_APPLICATIONS` успешно выполнен. В интерфейсе отображаются три задачи (`create_dataproc_cluster`, `run_pyspark_job`, `delete_dataproc_cluster`), все завершились со статусом `success`.

```text
06_output_folders_in_object_storage.png
```

В Object Storage появились результаты обработки в директории `task2/output/`.

```text
07_output_applications_by_channel.png
```

Результат агрегации `applications_by_channel` в `task2/output/`.

```text
08_output_applications_by_region.png
```

Результат агрегации `applications_by_region` в `task2/output/`.

```text
09_output_applications_by_risk.png
```

Результат агрегации `applications_by_risk` в `task2/output/`.

```text
10_output_decisions_by_product.png
```

Результат агрегации `decisions_by_product` в `task2/output/`.

### Результат

В результате выполнения задания был реализован автоматизированный ETL-процесс: Apache Airflow создал временный кластер Yandex Data Processing, запустил PySpark-задание для обработки CSV-файла размером более 50 МБ и удалил кластер после завершения обработки.
---

## Задание 3. Работа с топиками Apache Kafka с помощью PySpark-заданий в Yandex Data Processing

### Цель

Цель задания — настроить потоковую обработку JSON-сообщений через Apache Kafka и PySpark-задания в Yandex Data Processing.

В рамках задания необходимо:

```text
1. Подготовить Kafka-архитектуру.
2. Создать PySpark-задание для записи JSON-сообщений в Kafka topic.
3. Создать PySpark-задание для чтения Kafka topic в streaming-режиме.
4. Разложить вложенный JSON в плоский вид.
5. Сохранить результат обработки в Yandex Object Storage.
```

### Что было сделано

Для выполнения задания использовался Kafka-кластер:

```text
dataproc-kafka
```

Кластер находится в статусе `Alive`.

В Kafka был создан topic:

```text
loan-events-topic
```

Для работы с topic использовался пользователь Kafka:

```text
user1
```

Пользователю `user1` были выданы права на topic `loan-events-topic`:

```text
ACCESS_ROLE_CONSUMER
ACCESS_ROLE_PRODUCER
ACCESS_ROLE_TOPIC_ADMIN
```

Для подключения к Kafka использовался bootstrap server:

```text
rc1b-pp8eafcpuf057hcb.mdb.yandexcloud.net:9091
```

### Структура JSON-сообщения

В Kafka topic записывались JSON-сообщения о кредитных заявках следующей структуры:

```json
{
  "application_id": "loan_784512",
  "customer": {
    "customer_id": "cust_441",
    "region": "DE-HE"
  },
  "loan": {
    "amount": 15000,
    "term_months": 36
  },
  "scoring": {
    "score": 712,
    "risk_level": "medium"
  },
  "documents": [
    {
      "type": "passport",
      "status": "verified"
    }
  ],
  "decision_status": "manual_review",
  "submitted_at": "2026-05-01T10:15:11Z"
}
```

### Подготовленные PySpark-скрипты

Для задания были подготовлены три PySpark-скрипта:

```text
kafka_write_json.py
kafka_read_stream_flatten.py
kafka_read_batch_flatten.py
```

Скрипты были загружены в Object Storage:

```text
s3a://dataproc-bucket-kate-2026/task3/scripts/kafka_write_json.py
s3a://dataproc-bucket-kate-2026/task3/scripts/kafka_read_stream_flatten.py
s3a://dataproc-bucket-kate-2026/task3/scripts/kafka_read_batch_flatten.py
```

### Скрипт `kafka_write_json.py`

Скрипт `kafka_write_json.py` выполняет генерацию тестового потока JSON-событий о кредитных заявках и записывает их в Kafka topic:

```text
loan-events-topic
```

Скрипт генерирует 100 000 JSON-сообщений. Общий объём переданных данных превышает 20 МБ, что соответствует требованию задания.

Задание было запущено в Yandex Data Processing как PySpark job.

Параметры запуска:

```text
Main python файл:
s3a://dataproc-bucket-kate-2026/task3/scripts/kafka_write_json.py

Пакеты:
org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2
```

Задание успешно завершилось со статусом `Done`.

### Скрипт `kafka_read_stream_flatten.py`

Скрипт `kafka_read_stream_flatten.py` читает сообщения из Kafka topic в streaming-режиме, разбирает вложенный JSON и преобразует его в плоскую структуру.

На выходе формируется таблица со следующими полями:

```text
application_id
customer_id
region
amount
term_months
score
risk_level
document_type
document_status
decision_status
submitted_at
```

Результат сохраняется в Object Storage:

```text
s3a://dataproc-bucket-kate-2026/task3/output/loan_events_flatten_stream/
```

Для streaming-задания также используется checkpoint-директория:

```text
s3a://dataproc-bucket-kate-2026/task3/checkpoints/loan_events_flatten_stream/
```

Задание успешно завершилось со статусом `Done`.

### Скрипт `kafka_read_batch_flatten.py`

Дополнительно был подготовлен скрипт `kafka_read_batch_flatten.py`.

Он читает сообщения из Kafka topic в batch-режиме, также разбирает вложенный JSON и сохраняет результат в плоском виде.

Результат batch-обработки сохраняется в Object Storage:

```text
s3a://dataproc-bucket-kate-2026/task3/output/loan_events_flatten_batch/
```

Также был сформирован preview-файл с первыми строками результата:

```text
s3a://dataproc-bucket-kate-2026/task3/output/loan_events_flatten_preview/
```

Preview используется для проверки того, что вложенный JSON был успешно преобразован в плоскую таблицу.

### Схема работы

```text
PySpark kafka_write_json.py
        ↓
Kafka topic loan-events-topic
        ↓
PySpark kafka_read_stream_flatten.py
        ↓
Object Storage loan_events_flatten_stream

Дополнительно:

Kafka topic loan-events-topic
        ↓
PySpark kafka_read_batch_flatten.py
        ↓
Object Storage loan_events_flatten_batch
        ↓
Object Storage loan_events_flatten_preview
```

### Результаты выполнения

В рамках задания была выполнена полная цепочка потоковой обработки:

```text
1. Создан Kafka topic loan-events-topic.
2. Пользователю user1 выданы права на чтение, запись и администрирование topic.
3. PySpark-скрипт kafka_write_json.py записал JSON-сообщения в Kafka.
4. PySpark-скрипт kafka_read_stream_flatten.py прочитал сообщения из Kafka в streaming-режиме.
5. Вложенный JSON был разобран и преобразован в плоскую таблицу.
6. Результат сохранён в Object Storage.
7. Дополнительно выполнена batch-проверка через kafka_read_batch_flatten.py.
```

Все PySpark-задания завершились успешно со статусом `Done`.

### Скриншоты по заданию 3

Скриншоты находятся в папке:

```text
task3_kafka_pyspark/screenshots/
```

Список скриншотов:

```text
01_kafka_cluster.png
```

Kafka-кластер `dataproc-kafka` создан и находится в статусе `Alive`.

```text
02_kafka_topic_loan_events.png
```

Создан Kafka topic `loan-events-topic`.

```text
03_kafka_user_permissions.png
```

Пользователю `user1` выданы права на topic `loan-events-topic`: consumer, producer и topic admin.

```text
04_scripts_in_bucket.png
```

PySpark-скрипты задания 3 загружены в Object Storage в директорию `task3/scripts/`.

```text
05_write_json_done.png
```

PySpark-задание `kafka_write_json.py` успешно выполнено. JSON-сообщения записаны в Kafka topic `loan-events-topic`.

```text
06_kafka_read_stream_flatten_done.png
```

PySpark-задание `kafka_read_stream_flatten.py` успешно выполнено. Сообщения прочитаны из Kafka в streaming-режиме и преобразованы в плоский вид.

```text
07_object_storage_stream_output.png
```

В Object Storage появились результаты streaming-обработки в директории `task3/output/loan_events_flatten_stream/`.

```text
08_kafka_read_batch_flatten_done.png
```

PySpark-задание `kafka_read_batch_flatten.py` успешно выполнено. Kafka topic прочитан в batch-режиме.

```text
09_flatten_preview_output.png
```

В Object Storage сформирован preview результата с плоской структурой данных.

```text
10_object_storage_batch_output.png
```

В Object Storage появились результаты batch-обработки в директории `task3/output/loan_events_flatten_batch/`.

```text
11_object_storage_stream_output.png
```

Дополнительный скриншот результата streaming-обработки в Object Storage.

### Результат

В результате выполнения задания была реализована потоковая аналитика на базе Apache Kafka и PySpark в Yandex Data Processing.

JSON-события о кредитных заявках объёмом более 20 МБ были записаны в Kafka topic `loan-events-topic`, затем прочитаны PySpark-заданиями, разобраны из вложенного JSON и сохранены в Object Storage в плоском табличном виде.
