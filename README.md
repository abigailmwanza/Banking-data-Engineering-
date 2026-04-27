# Banking Data Engineering Pipeline

> An end-to-end, production-style streaming data pipeline that captures banking transactions in real time, lands them in a data lake, and loads them into a cloud data warehouse for analytics.

**Pipeline:** `PostgreSQL → Debezium → Kafka → MinIO → Snowflake → dbt → Metabase`
**Orchestration:** Apache Airflow
**Visualization:** Metabase

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Pipeline in Action](#3-pipeline-in-action)
4. [Technology Choices](#4-technology-choices)
5. [Tech Stack](#5-tech-stack)
6. [Project Structure](#6-project-structure)
7. [Getting Started](#7-getting-started)
8. [Business Applications — Zambian Banking Sector](#8-business-applications--zambian-banking-sector)

---

## 1. Overview

This project demonstrates a modern, event-driven data pipeline for a retail banking domain. Transactions generated in a PostgreSQL OLTP database are captured via Change Data Capture (CDC), streamed through Kafka, persisted as Parquet files in object storage, and loaded into Snowflake for analytical processing and transformation with dbt.

The design mirrors the architecture used by banks and fintechs to deliver sub-minute analytics, regulatory reporting, and fraud detection.

### In Simple Terms — What This Does for a Bank

Every time a customer deposits, withdraws, or transfers money, the bank's database records it. Most banks only look at this data the next day, after reports have been generated overnight. By then, it is already too late to catch fraud or answer a manager's question.

This project fixes that problem. Here is what it does, in plain language:

- **It watches the bank's database every second.** The moment a new transaction happens, this pipeline sees it — no waiting for end-of-day reports.
- **It sends the information to a safe storage area.** Every transaction is copied out of the main banking system so reports and analysis do not slow the bank down.
- **It keeps a permanent record.** Every deposit, withdrawal, and transfer is saved as a file that can never be changed — this is what regulators like the Bank of Zambia require.
- **It loads the data into a place where anyone can ask questions.** Managers, auditors, and analysts can run reports in seconds instead of waiting days.
- **It catches problems early.** Suspicious activity — like someone withdrawing large amounts in different towns at once — can be flagged straight away instead of next week.
- **It turns the data into pictures.** Charts and dashboards (Metabase) let managers see what's happening at a glance — total deposits today, transaction trends, account mix — without writing a single line of code.
- **It works automatically.** Once running, the pipeline does everything by itself: capture, move, store, load, visualize. No one has to press a button.

**The short version:** this project turns a bank's raw transaction data into clean, trustworthy, real-time information that can be used for fraud detection, regulatory reporting, customer insights, and business decisions — without slowing down the bank's main systems.

---

## 2. Architecture

```
┌──────────┐   WAL   ┌──────────┐ Topics ┌─────────┐ Parquet ┌───────┐  COPY   ┌───────────┐   SQL   ┌──────┐
│ Postgres │────────▶│ Debezium │───────▶│  Kafka  │────────▶│ MinIO │────────▶│ Snowflake │────────▶│ dbt  │
└──────────┘         └──────────┘        └─────────┘         └───────┘         └───────────┘         └──────┘
     ▲                                                             ▲                   │
     │                                                             │                   │ SQL
  Faker                                                        Airflow ────────────────┤
(generator)                                                   (scheduler)              ▼
                                                                                 ┌──────────┐
                                                                                 │ Metabase │
                                                                                 │ (charts) │
                                                                                 └──────────┘
```

**Stages**

| # | Stage | Component |
|---|-------|-----------|
| 1 | Generate | Faker Python script writes synthetic customers, accounts, and transactions into PostgreSQL |
| 2 | Capture | Debezium reads the PostgreSQL WAL and publishes row-level change events to Kafka |
| 3 | Stream | Kafka distributes events across topics (one per table) |
| 4 | Land | Python consumer writes events to MinIO as Parquet, partitioned by table and date |
| 5 | Load | Airflow DAG runs every minute, bulk-loading Parquet files into Snowflake via `COPY INTO` |
| 6 | Transform | dbt models shape raw CDC data into staging views and analytics-ready marts |
| 7 | Visualize | Metabase queries Snowflake marts to render no-code dashboards for pipeline observability and business insight |

---

## 3. Pipeline in Action

The screenshots below walk through the pipeline end-to-end, using live data from a running environment. They follow the same order as the stage table in §2.

### 3.1 Source — PostgreSQL (OLTP)

Faker continuously writes synthetic `customers`, `accounts`, and `transactions` into the PostgreSQL source database. Every `INSERT` / `UPDATE` / `DELETE` is recorded in the Write-Ahead Log, which is what Debezium tails.

![PostgreSQL accounts table populated by the Faker generator](<postgres database.png>)

*`accounts` table in PostgreSQL — synthetic savings and checking balances in ZMW, timestamped at the millisecond level.*

---

### 3.2 Capture & Stream — Kafka Consumer → MinIO (Parquet)

The Python consumer subscribes to the Debezium topics (`banking.public.customers`, `banking.public.accounts`, `banking.public.transactions`), buffers messages, and uploads them to MinIO as date-partitioned Parquet files. Each line in the log below is one Parquet file landing in the raw zone.

![Kafka consumer uploading Parquet files to MinIO in real time](<images/loading kakfa to minio in parquet.png>)

*Consumer output — each `Uploaded 1 record to s3://raw/transactions/date=2026-04-22/...parquet` confirms a CDC event has been durably landed in object storage.*

---

### 3.3 Land — MinIO Raw Zone

MinIO acts as the immutable "raw layer" of the lakehouse. Data is partitioned by table and by `date=YYYY-MM-DD`, which keeps Snowflake `COPY INTO` scans cheap and gives auditors a replayable archive.

![MinIO object browser showing date-partitioned raw buckets](images/minio.png)

*The `raw/accounts/` prefix with Hive-style date partitions — 340 Parquet objects / 617.5 KiB after a short run.*

---

### 3.4 Orchestrate — Airflow DAGs

Two DAGs are registered: `minio_to_snowflake_banking` (every minute, loads Parquet into Snowflake) and `SCD2_snapshots` (daily, builds Type-2 history via dbt snapshots).

![Airflow DAGs list showing minio_to_snowflake_banking and SCD2_snapshots](<images/Dag Airflow.png>)

*Airflow home — both DAGs enabled, recent runs green, next run scheduled a minute out.*

---

### 3.5 Load — DAG Run: MinIO → Snowflake

The minute-level DAG has two tasks: `download_minio` pulls any new Parquet files, then `load_snowflake` executes a `COPY INTO` against the `BANKING.RAW` schema.

![Airflow DAG graph view showing download_minio and load_snowflake tasks succeeding](<images/Dag runn.png>)

*Gantt-side green bars = successful runs each minute. Graph view shows the two-task DAG completing.*

---

### 3.6 Alerting — Email on Failure

When a task fails (e.g. a Snowflake stage is missing or a permissions issue surfaces), Airflow's `on_failure_callback` sends a formatted email with the DAG, task, execution time, attempt number, and the exception.

![Gmail inbox showing a Banking Pipeline Failed email from Airflow](<images/notified through email on fail.png>)

*Real alert captured during development — a Snowflake `SQL compilation error: Stage 'BANKING.ANALYTICS."%CUSTOMERS"' does not exist` was surfaced within seconds of the failure.*

---

### 3.7 Transform — dbt on Snowflake

Once raw CDC rows are in Snowflake, dbt shapes them into staging views and analytics marts. The query below is `fact_transactions.sql` — an incremental model joining `stg_transactions` with `stg_accounts` to attach `customer_id`, producing a clean, query-ready fact table.

![dbt fact_transactions model executed in Snowflake with 300 rows returned](<images/dbt data transfomation.png>)

*Preview of `fact_transactions` — each row is an enriched transaction with `customer_id`, amount, type, status, and load timestamp, ready for fraud, AML, or BoZ reporting queries.*

---

### 3.8 Visualize — Metabase Dashboard

The final layer is a self-service BI dashboard built in Metabase, connected directly to the Snowflake marts. The "Banking Pipeline Overview" dashboard surfaces what data has landed end-to-end — confirming all transaction types and account types are flowing through the pipeline cleanly, and giving managers a no-code way to explore the warehouse.

![Metabase Banking Pipeline Overview dashboard with transaction type and account balance breakdowns](images/Dashboard.png)

*Banking Pipeline Overview — total transaction value by type (deposit / transfer / withdrawal) alongside the account-balance mix between checking and savings, both queried live from the Snowflake marts.*

**What the dashboard tells us at a glance:**

- All three transaction types are landing in Snowflake (deposit, transfer, withdrawal) — confirms Debezium is capturing every variety, not silently dropping any.
- Net deposit flow is positive: deposits ($619K) > withdrawals ($428K) — the bank is gaining money on net.
- Both account types (checking, savings) are present in `dim_accounts` with clean numeric balances totalling $1.6M — the snapshot model is preserving state correctly and `COPY INTO` is preserving types properly.
- Transactions and balances are queryable end-to-end, proving the full pipeline (Postgres → Debezium → Kafka → MinIO → Snowflake → dbt) is operating as designed.

---

## 4. Technology Choices

Each tool in the stack was chosen for a specific engineering reason.

| Tool | Role | Rationale |
|------|------|-----------|
| **PostgreSQL** | Source OLTP database | Industry-standard relational DB with a logical Write-Ahead Log (WAL), enabling CDC without schema changes or application-level triggers. |
| **Faker** | Synthetic data generator | Produces realistic customer and transaction records in a continuous loop, replacing the need for a live banking source. |
| **Debezium** | Change Data Capture | Streams every `INSERT`, `UPDATE`, and `DELETE` from the WAL in near real time with no impact on source performance. |
| **Apache Kafka** | Streaming message broker | Decouples producers from consumers; durable, replayable topics ensure no event loss during downstream failures. |
| **Zookeeper** | Kafka coordination | Manages broker metadata and leader election for Kafka 7.x clusters. |
| **Python Consumer** | Kafka-to-MinIO bridge | Converts streaming JSON to columnar Parquet, partitioned for efficient warehouse ingestion. |
| **MinIO** | S3-compatible object storage | Cost-effective, scalable landing zone ("raw layer"); preserves a replayable archive of every event. |
| **Apache Airflow** | Workflow orchestration | Schedules, monitors, and retries the MinIO-to-Snowflake load; bridges streaming and batch layers. |
| **Snowflake** | Cloud data warehouse | Decouples storage from compute, scales elastically, and ingests Parquet natively via `COPY INTO`. |
| **dbt** | SQL transformation layer | Version-controlled, testable SQL models with built-in lineage and documentation. |
| **Metabase** | BI / dashboarding | Open-source visualization layer with native Snowflake support; lets analysts and managers explore marts without writing SQL. |
| **Docker Compose** | Local environment | One-command reproducible setup for all services across machines. |

---

## 5. Tech Stack

**Languages:** Python 3.11, SQL
**Data Platforms:** PostgreSQL 15, Snowflake
**Streaming:** Apache Kafka 7.4, Debezium 2.2, Zookeeper
**Storage:** MinIO (S3-compatible)
**Orchestration:** Apache Airflow 2.9
**Transformation:** dbt-snowflake
**Visualization:** Metabase
**Infrastructure:** Docker, Docker Compose

---

## 6. Project Structure

```
Banking-data-engineering/
├── docker-compose.yml           # Service definitions
├── postgres/
│   └── SCHEMA.SQL               # Source database schema
├── data-generator/              # Faker-based data producer
├── kafka-debezium/              # Debezium connector registration
├── consumer/                    # Kafka → MinIO Parquet consumer
├── docker/
│   └── dags/                    # Airflow DAG definitions
└── banking_dbt/                 # dbt transformation project
```

---

## 7. Getting Started

### Prerequisites
- Docker Desktop
- Python 3.11+
- A Snowflake account with a `banking.raw` schema

### Setup

```bash
# 1. Start all services
docker-compose up -d

# 2. Create the source schema
psql -h localhost -p 5435 -U postgres -d banking -f postgres/SCHEMA.SQL

# 3. Register the Debezium CDC connector
python kafka-debezium/generate_and_post_connector.py

# 4. Start the Kafka → MinIO consumer
python consumer/kafka_to_minio.py

# 5. Generate synthetic data
python data-generator/faker_generator.py
```

### Configure Snowflake

Add your Snowflake credentials to `docker/dags/.env`, then open Airflow at `http://localhost:8080` and enable the `minio_to_snowflake_banking` DAG.

### Configure Metabase

Open Metabase at `http://localhost:3000` and complete the first-run setup wizard. When prompted to add a data source, choose **Snowflake** and enter the same credentials used for the Airflow DAG (account, user, password, warehouse, database, role). Once connected, build dashboards directly against the dbt marts (`dim_customers`, `dim_accounts`, `fact_transactions`) using the no-code question builder.

---

## 8. Business Applications — Zambian Banking Sector

This architecture addresses concrete operational and regulatory needs within Zambia's financial services industry — commercial banks (Zanaco, FNB Zambia, Stanbic, Absa Zambia, Indo-Zambia) and mobile money operators (MTN MoMo, Airtel Money, Zamtel Kwacha).

### 8.1 Use Cases

| Use Case | Business Impact |
|----------|-----------------|
| **Real-time fraud detection** | Sub-second detection of anomalies such as simultaneous ATM withdrawals across cities, replacing next-day batch reviews. |
| **Bank of Zambia regulatory reporting** | Automated, auditable daily and weekly prudential returns — capital adequacy, large exposures, AML — eliminating error-prone spreadsheet workflows. |
| **AML and CTR compliance** | Automatic flagging and reporting of Currency Transaction Reports above the FIC threshold (ZMW 100,000). |
| **Mobile money reconciliation** | High-throughput reconciliation between mobile wallets and bank settlement accounts at millions of transactions per day. |
| **Agent banking analytics** | Per-agent float tracking, commission calculation, and activity monitoring for networks such as Zanaco Xpress and FNB Cash Plus. |
| **Cross-border remittance monitoring** | Transaction-level tagging of SWIFT and RTGS inflows from the UK, South Africa, and DRC for FX exposure and AML oversight. |
| **Financial inclusion insight** | Identification of dormant accounts in underbanked provinces (Luapula, North-Western) for targeted reactivation campaigns. |
| **Multi-currency support** | Native handling of `ZMW`, `USD`, `ZAR`, and `GBP` transactions for corporate and dual-currency deposit accounts. |
| **Core banking modernisation** | CDC-based data extraction from legacy cores (Flexcube, T24, Bankfusion) without impacting source systems — a low-risk modernisation pathway. |
| **SME credit scoring** | Clean transaction history feeds into credit models, addressing the gap left by limited credit bureau coverage in the SME segment. |

### 8.2 Why This Architecture Suits the Zambian Context

- **Resilient to low bandwidth** — Kafka buffering and MinIO batching tolerate intermittent connectivity between branches in remote provinces.
- **Cost-efficient** — Self-hosted MinIO combined with Snowflake's pay-per-query pricing minimises upfront capital expenditure.
- **Regulator-ready** — Immutable Parquet archives in MinIO satisfy BoZ and FIC record-retention requirements (minimum 10 years).
- **Skills-aligned** — Built on SQL and Python, the most widely available data skills in the Zambian market; avoids niche or proprietary tooling.

---
