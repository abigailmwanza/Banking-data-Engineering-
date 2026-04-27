import time
import psycopg2
from decimal import Decimal, ROUND_DOWN
from faker import Faker
import random
import argparse
import sys
import os # to read or save env secrets
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# Project configuration (safe to hardcode here)
# -----------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
CURRENCY = "ZMW"   # Zambian Kwacha

# Transaction amount ranges in Kwacha (realistic for Zambian retail banking)
#   Small daily spend:   ZMW 10   – 500    (airtime, food, transport)
#   Medium:              ZMW 500  – 3,000  (groceries, utility bills)
#   Large:               ZMW 3,000 – 10,000 (rent, salary, school fees)
MIN_TXN_AMOUNT = 10.00
MAX_TXN_AMOUNT = 10000.00

# Initial balances in Kwacha — typical opening range for a new account
INITIAL_BALANCE_MIN = Decimal("100.00")
INITIAL_BALANCE_MAX = Decimal("25000.00")

# -----------------------------
# Zambian name pools
# Used instead of Faker's default (which gives Western names).
# These are common first and last names across Zambia's main
# language groups (Bemba, Nyanja, Tonga, Lozi, Tumbuka, etc.).
# -----------------------------
ZM_FIRST_NAMES = [
    # Male
    "Chanda", "Mwamba", "Mulenga", "Bwalya", "Kabwe", "Kondwani",
    "Mwansa", "Bupe", "Kunda", "Lubinda", "Mwila", "Chileshe",
    "Emmanuel", "Joseph", "Kelvin", "Chibuye", "Kaunda", "Musonda",
    # Female
    "Mwape", "Chisomo", "Natasha", "Lweendo", "Mutinta", "Namakau",
    "Memory", "Abigail", "Precious", "Mutale", "Chipo", "Nchimunya",
    "Kondwa", "Bwalya", "Tamara", "Thandiwe", "Mirriam", "Ruth",
]

ZM_LAST_NAMES = [
    "Banda", "Phiri", "Mwamba", "Mulenga", "Tembo", "Zulu", "Mwansa",
    "Lungu", "Sakala", "Bwalya", "Chanda", "Kabwe", "Ngoma", "Mweemba",
    "Siame", "Chisanga", "Mumba", "Kasonde", "Mwila", "Nkandu",
    "Musonda", "Chipimo", "Simbeye", "Kapata", "Kaunda", "Daka",
    "Nyirenda", "Chileshe", "Chama", "Soko",
]

ZM_EMAIL_DOMAINS = [
    "gmail.com", "yahoo.com", "outlook.com",  # most common in Zambia
    "zamtel.co.zm", "unza.zm",                 # local Zambian domains
]

# Loop config
DEFAULT_LOOP = True
SLEEP_SECONDS = 2

# CLI override (run once mode)
parser = argparse.ArgumentParser(description="Run fake data generator")
parser.add_argument("--once", action="store_true", help="Run a single iteration and exit")
args = parser.parse_args()
LOOP = not args.once and DEFAULT_LOOP

# -----------------------------
# Helpers
# -----------------------------
fake = Faker()

def random_money(min_val: Decimal, max_val: Decimal) -> Decimal:
    """Pick a random money amount between min_val and max_val, rounded to 2 decimals.
    We use Decimal (not float) because money math with floats causes rounding errors
    like 0.1 + 0.2 == 0.30000000000000004 — bad for banking."""
    # random.uniform returns a float, so we convert via str() to avoid float imprecision
    val = Decimal(str(random.uniform(float(min_val), float(max_val))))
    # quantize() forces 2 decimal places (cents/ngwee); ROUND_DOWN truncates instead of rounding up
    return val.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

def zambian_name():
    """Return a random (first_name, last_name) pair from the Zambian name pools above."""
    return random.choice(ZM_FIRST_NAMES), random.choice(ZM_LAST_NAMES)

def zambian_email(first_name: str, last_name: str) -> str:
    """Build a realistic Zambian-style email from the person's name.
    A random number suffix is appended so two people with the same name don't collide."""
    domain = random.choice(ZM_EMAIL_DOMAINS)
    suffix = random.randint(1, 9999)
    # e.g. "chanda.banda4271@gmail.com"
    return f"{first_name.lower()}.{last_name.lower()}{suffix}@{domain}"

# -----------------------------
# Connect to Postgres
# Credentials are pulled from the .env file (loaded by load_dotenv() at the top)
# so we never hardcode secrets in the source code.
# -----------------------------
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
)
# autocommit=True means every INSERT is saved immediately — no need to call conn.commit().
# This is what we want for a streaming generator: each row should land in the DB right away
# so Debezium can pick it up as a CDC event.
conn.autocommit = True
# A cursor is the object we use to actually execute SQL statements
cur = conn.cursor()

# -----------------------------
# Core generation logic (one iteration)
# -----------------------------
def run_iteration():
    """One full pass of the data generator: creates customers → their accounts → transactions.
    Called once per loop (or once total if --once is passed)."""

    # ---- 1. Generate customers ----
    # We collect each new customer's database id in this list so we can attach accounts to them next.
    customers = []
    for _ in range(NUM_CUSTOMERS):
        first_name, last_name = zambian_name()
        email = zambian_email(first_name, last_name)

        # %s placeholders + a tuple of values is psycopg2's safe way to pass parameters —
        # it prevents SQL injection. Never use f-strings to build SQL with user data.
        # RETURNING id asks Postgres to give us back the auto-generated primary key
        # so we can use it as the foreign key on the accounts table.
        cur.execute(
            "INSERT INTO customers (first_name, last_name, email) VALUES (%s, %s, %s) RETURNING id",
            (first_name, last_name, email),
        )
        customer_id = cur.fetchone()[0]  # fetchone() returns a single row tuple; [0] grabs the id
        customers.append(customer_id)

    # ---- 2. Generate accounts ----
    # Each customer gets ACCOUNTS_PER_CUSTOMER accounts (currently 2) — e.g. one savings + one checking.
    accounts = []
    for customer_id in customers:
        for _ in range(ACCOUNTS_PER_CUSTOMER):
            account_type = random.choice(["SAVINGS", "CHECKING"])
            # Opening balance is random within a realistic Kwacha range
            initial_balance = random_money(INITIAL_BALANCE_MIN, INITIAL_BALANCE_MAX)
            cur.execute(
                "INSERT INTO accounts (customer_id, account_type, balance, currency) VALUES (%s, %s, %s, %s) RETURNING id",
                (customer_id, account_type, initial_balance, CURRENCY),
            )
            account_id = cur.fetchone()[0]
            accounts.append(account_id)

    # ---- 3. Generate transactions ----
    # Transactions reference an account by id. They simulate real banking activity
    # that Debezium will capture as CDC events and push through Kafka → MinIO → Snowflake.
    txn_types = ["DEPOSIT", "WITHDRAWAL", "TRANSFER"]
    for _ in range(NUM_TRANSACTIONS):
        account_id = random.choice(accounts)        # the account being debited/credited
        txn_type = random.choice(txn_types)
        amount = round(random.uniform(MIN_TXN_AMOUNT, MAX_TXN_AMOUNT), 2)

        # For TRANSFER we need a second account (the destination).
        # The list comprehension `[a for a in accounts if a != account_id]` excludes the source
        # so a customer can't transfer money to themselves on the same account.
        related_account = None
        if txn_type == "TRANSFER" and len(accounts) > 1:
            related_account = random.choice([a for a in accounts if a != account_id])

        # Status is hardcoded to 'COMPLETED' — in a real system this would be PENDING/FAILED too.
        cur.execute(
            "INSERT INTO transactions (account_id, txn_type, amount, related_account_id, status) VALUES (%s, %s, %s, %s, 'COMPLETED')",
            (account_id, txn_type, amount, related_account),
        )

    print(f"✅ Generated {len(customers)} customers, {len(accounts)} accounts, {NUM_TRANSACTIONS} transactions.")

# -----------------------------
# Main loop
# By default the generator runs forever (one batch every SLEEP_SECONDS seconds)
# so the pipeline always has a steady stream of new CDC events to process.
# Pass --once on the command line to generate a single batch and exit.
# -----------------------------
try:
    iteration = 0
    while True:
        iteration += 1
        print(f"\n--- Iteration {iteration} started ---")
        run_iteration()
        print(f"--- Iteration {iteration} finished ---")
        if not LOOP:
            break  # --once mode: stop after the first batch
        time.sleep(SLEEP_SECONDS)  # pause before generating the next batch

# Ctrl+C raises KeyboardInterrupt — we catch it so the script exits cleanly
# instead of dumping a scary stack trace.
except KeyboardInterrupt:
    print("\nInterrupted by user. Exiting gracefully...")

# `finally` always runs — whether the loop ended normally, was interrupted,
# or hit an unexpected error. This is where we release the DB resources so we
# don't leak connections back to Postgres.
finally:
    cur.close()
    conn.close()
    sys.exit(0)