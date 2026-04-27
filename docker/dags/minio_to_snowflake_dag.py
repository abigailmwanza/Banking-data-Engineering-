import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load DAG-local .env — override container env so minio→raw schema wins
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(__file__), ".env"),
    override=True,
)

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
# Schema is hardcoded to "raw" — this DAG's sole job is to land CDC data
# into the raw layer. dbt reads from raw and writes to analytics.
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB") or os.getenv("SNOWFLAKE_DATABASE", "banking")
SNOWFLAKE_SCHEMA = "raw"

TABLES = ["customers", "accounts", "transactions"]

ALERT_RECIPIENTS = ["abigailmwnz@gmail.com"]

# -------- Failure Callback --------
def failure_email_callback(context):
    ti = context.get("task_instance")
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context.get("execution_date")
    try_number = ti.try_number
    max_tries = ti.max_tries + 1
    exception = context.get("exception")
    log_url = ti.log_url

    log_tail = "(log file not found)"
    try:
        log_path = ti.log_filepath if hasattr(ti, "log_filepath") else None
        if log_path and os.path.exists(log_path):
            with open(log_path, "r") as f:
                lines = f.readlines()
                log_tail = "".join(lines[-25:])
    except Exception as e:
        log_tail = f"(could not read log file: {e})"

    subject = f"🚨 Banking Pipeline Failed — {dag_id}.{task_id}"

    html_content = f"""
    <h3>🚨 Airflow Task Failure</h3>
    <table cellpadding="6" style="border-collapse: collapse; font-family: monospace;">
      <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
      <tr><td><b>Task</b></td><td>{task_id}</td></tr>
      <tr><td><b>Execution</b></td><td>{execution_date}</td></tr>
      <tr><td><b>Attempt</b></td><td>{try_number} of {max_tries}</td></tr>
    </table>

    <h4>Error</h4>
    <pre style="background:#f8d7da; padding:10px; border-left:4px solid #c00;">
{exception}
    </pre>

    <h4>Last 25 log lines</h4>
    <pre style="background:#f4f4f4; padding:10px; border-left:4px solid #666; font-size:12px;">
{log_tail}
    </pre>

    <p><a href="{log_url}">View full log in Airflow</a></p>
    """

    send_email(to=ALERT_RECIPIENTS, subject=subject, html_content=html_content)

# -------- Python Callables --------
def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    local_files = {}
    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []
        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files found in MinIO.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    cur.execute(f"USE DATABASE {SNOWFLAKE_DB}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        fq_table = f"{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{table}"

        for f in files:
            cur.execute(f"PUT file://{f} @%{table}")
            print(f"Uploaded {f} -> @%{table} stage (schema={SNOWFLAKE_SCHEMA})")

        copy_sql = f"""
        COPY INTO {fq_table}
        FROM @%{table}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """
        cur.execute(copy_sql)
        print(f"Data loaded into {fq_table}")

    cur.close()
    conn.close()

# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email": ALERT_RECIPIENTS,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": failure_email_callback,
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    task1 >> task2