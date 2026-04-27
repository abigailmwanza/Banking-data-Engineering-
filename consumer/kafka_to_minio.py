import boto3
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# Read the .env file so we can use the passwords and addresses stored in it.
# Without this line, os.getenv() below would return nothing.
load_dotenv()

# -------------------------------------------------------------
# STEP 1: CONNECT TO KAFKA AND LISTEN TO 3 TOPICS
#
# Kafka is like a message board. Debezium posts a message every
# time a row is inserted or updated in PostgreSQL. We subscribe
# to three "channels" (topics) — one for each database table.
# This script will receive every new banking row change in real time.
# -------------------------------------------------------------
consumer = KafkaConsumer(
    # These are the three channels we want to listen to.
    # Each one matches a table in the PostgreSQL banking database.
    # Topic name = {topic.prefix}.{schema}.{table}
    # topic.prefix is set to "banking" in generate_and_post_connector.py
    'banking.public.customers',
    'banking.public.accounts',
    'banking.public.transactions',

    # The address of the Kafka server.
    # localhost:29092 is used when running this script on your machine.
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP"),

    # When this script starts fresh (no history), should it read
    # old messages or only new ones?
    # 'earliest' = start from the very beginning, read everything.
    # 'latest'   = only read messages that arrive after now.
    auto_offset_reset='earliest',

    # Kafka remembers which messages we have already read.
    # Setting this to True means Kafka automatically saves our
    # progress, so if the script restarts, it picks up where it
    # left off instead of reading everything again.
    enable_auto_commit=True,

    # A group name for this consumer. Kafka uses this to track
    # our reading progress separately from other consumers. and we get data that is not duplicated if we run multiple instances of this script. You can use any name here, but it should be unique to this application. 
    group_id=os.getenv("KAFKA_GROUP"),

    # Kafka messages arrive as raw bytes. This line converts
    # those bytes into a Python dictionary we can actually work with.
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# -------------------------------------------------------------
# STEP 2: CONNECT TO MINIO (OUR FILE STORAGE)
#
# MinIO is like a private version of Amazon S3 running on your
# own machine. We will save each banking record as a file here.
# boto3 is the library that lets Python talk to it.
# -------------------------------------------------------------
s3 = boto3.client(
    's3',
    endpoint_url=os.getenv("MINIO_ENDPOINT"),        # MinIO server address
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),  # MinIO username
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),  # MinIO password
    # boto3 always asks for a region. MinIO doesn't care what
    # value we put here, but we must provide something.
    region_name='us-east-1'
)

# The name of the storage bucket (like a folder) where files go.
bucket = os.getenv("MINIO_BUCKET")

# Check if the bucket already exists. If not, create it.
# This only creates the bucket on the very first run.
if bucket not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket=bucket)


# -------------------------------------------------------------
# STEP 3: SAVE ONE RECORD TO MINIO
#
# This function takes a single database row and saves it as a
# Parquet file in MinIO. It is called once per Kafka message.
#
# Why Parquet? It is a file format that Snowflake reads very
# efficiently — much faster than CSV or JSON for large data.
#
# Where is the file saved in MinIO?
#   transactions/date=2025-01-15/transactions_143022125642.parquet
#   └── table name  └── today's date  └── unique filename (time-based)
#
# Organising by date makes it easy to load only a specific day
# into Snowflake later.
# -------------------------------------------------------------
def write_single_to_minio(table_name, record):
    # Put the single row into a table structure (DataFrame)
    # so it can be saved as a Parquet file.
    df = pd.DataFrame([record])

    date_str  = datetime.now().strftime('%Y-%m-%d')
    # Add microseconds to the filename so two files saved at the
    # same second don't overwrite each other.
    file_path = f'{table_name}_{datetime.now().strftime("%H%M%S%f")}.parquet'

    # Save the file temporarily on this machine. we save in parquet format using the fastparquet engine, and we don't include the DataFrame index in the file.  
    df.to_parquet(file_path, engine='fastparquet', index=False)

    # Build the full path inside MinIO, e.g.:
    # transactions/date=2025-01-15/transactions_143022125642.parquet
    s3_key = f'{table_name}/date={date_str}/{file_path}'

    # Upload the file from this machine to MinIO.
    s3.upload_file(file_path, bucket, s3_key)

    # Delete the local copy — we no longer need it now that
    # it is safely stored in MinIO.
    os.remove(file_path)

    print(f'✅ Uploaded 1 record to s3://{bucket}/{s3_key}')


# -------------------------------------------------------------
# STEP 4: MAIN LOOP — KEEP READING MESSAGES FOREVER
#
# This loop waits for new Kafka messages and processes them
# one by one. It runs until you press Ctrl+C to stop it.
#
# Each message from Debezium looks like this:
#   {
#     "payload": {
#       "before": { old row data },   ← what the row looked like before
#       "after":  { new row data },   ← what the row looks like now
#       "op": "c"                     ← c=insert, u=update, d=delete
#     }
#   }
#
# We only care about "after" — the current state of the row.
# If "after" is empty (e.g. a delete event), we skip the message.
# -------------------------------------------------------------
print("✅ Connected to Kafka. Listening for messages...")

try:
    for message in consumer:
        # The full topic name, e.g. "banking_server.public.transactions"
        topic = message.topic

        # The message content, already converted to a Python dict.
        event = message.value

        # Dig into the message to find the actual row data.
        payload = event.get("payload", {})

        # "after" is the row as it looks now after the change.
        # It is None for delete events, which we skip.
        record = payload.get("after")

        if record:
            # topic.split('.')[-1] takes just the last part of the
            # topic name: "banking_server.public.transactions" → "transactions"
            # This becomes the folder name in MinIO.
            print(f"[{topic}] -> {record}")
            write_single_to_minio(topic.split('.')[-1], record)

except KeyboardInterrupt:
    # The user pressed Ctrl+C. Exit cleanly.
    # Kafka has already saved our progress, so nothing is lost.
    print("\nInterrupted by user. Exiting gracefully...")
