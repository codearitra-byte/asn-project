# import psycopg2
# from psycopg2.extras import execute_values
# import os

# def load_to_db(**context):
#     ti = context["ti"]

#     asn_data = ti.xcom_pull(task_ids="extract_asn")

#     if not asn_data:
#         print("No ASN data found. Skipping database load.")
#         return

#     conn = None
#     cur = None

#     try:
#         conn = psycopg2.connect(
#             host="postgres",
#             database=os.getenv("POSTGRES_DB"),
#             user=os.getenv("POSTGRES_USER"),
#             password=os.getenv("POSTGRES_PASSWORD")
#         )
#         cur = conn.cursor()
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS asn_registry (
#                 registry TEXT,
#                 country TEXT,
#                 asn INTEGER,
#                 status TEXT,
#                 PRIMARY KEY (registry, country, asn)
#             );
#         """)

#         insert_query = """
#             INSERT INTO asn_registry (registry, country, asn, status)
#             VALUES %s
#             ON CONFLICT DO NOTHING
#         """
#         batch_size = 5000
#         for i in range(0, len(asn_data), batch_size):
#             batch = asn_data[i:i + batch_size]
#             execute_values(cur, insert_query, batch)

#         conn.commit()
#         print(f"Inserted {len(asn_data)} rows into Postgres")

#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Error inserting ASN data: {e}")
#         raise  

#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()
# import psycopg2
# from psycopg2.extras import execute_values
# import os

# def load_to_db(**context):
#     ti = context["ti"]

#     asn_data = ti.xcom_pull(task_ids="extract_asn")

#     # ---- TYPE VALIDATION ----
#     if asn_data is None:
#         print("No ASN data found in XCom.")
#         return

#     if not isinstance(asn_data, list):
#         raise TypeError(
#             f"Expected list of ASN records, got {type(asn_data).__name__}"
#         )

#     if len(asn_data) == 0:
#         print("ASN list is empty. Nothing to insert.")
#         return

#     conn = None
#     cur = None

#     try:
#         conn = psycopg2.connect(
#             host="postgres",
#             database=os.getenv("POSTGRES_DB"),
#             user=os.getenv("POSTGRES_USER"),
#             password=os.getenv("POSTGRES_PASSWORD")
#         )

#         cur = conn.cursor()

#         # ---- TABLE CREATION ----
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS asn_registry (
#                 registry TEXT NOT NULL,
#                 country TEXT NOT NULL,
#                 asn INTEGER NOT NULL,
#                 status TEXT,
#                 PRIMARY KEY (registry, country, asn)
#             );
#         """)

#         insert_query = """
#             INSERT INTO asn_registry (registry, country, asn, status)
#             VALUES %s
#             ON CONFLICT DO NOTHING
#         """

#         batch_size = 5000
#         total_inserted = 0

#         # ---- BATCH INSERT ----
#         for i in range(0, len(asn_data), batch_size):
#             batch = asn_data[i:i + batch_size]

#             execute_values(cur, insert_query, batch)
#             total_inserted += len(batch)

#             print(f"Inserted batch {i} → {i + len(batch)}")

#         conn.commit()

#         print(f"Successfully inserted {total_inserted} rows into Postgres.")

#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Error inserting ASN data: {e}")
#         raise

#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()
# import psycopg2
# from psycopg2.extras import execute_values
# import os

# def load_to_db(**context):
#     ti = context["ti"]

#     asn_data = ti.xcom_pull(task_ids="extract_asn")

#     # -----------------------------
#     # HANDLE EMPTY OR NONE
#     # -----------------------------
#     if asn_data is None:
#         print("No ASN data found in XCom.")
#         return

#     # -----------------------------
#     # HANDLE CASE: extract returned COUNT (int)
#     # -----------------------------
#     if isinstance(asn_data, int):
#         print(f"Extract task returned count only: {asn_data}")
#         print("Nothing to insert because no ASN records were passed.")
#         return

#     # -----------------------------
#     # VALIDATE LIST TYPE
#     # -----------------------------
#     if not isinstance(asn_data, list):
#         raise TypeError(
#             f"Expected list of ASN records, got {type(asn_data).__name__}"
#         )

#     if len(asn_data) == 0:
#         print("ASN list is empty. Nothing to insert.")
#         return

#     conn = None
#     cur = None

#     try:
#         conn = psycopg2.connect(
#             host="postgres",
#             database=os.getenv("POSTGRES_DB"),
#             user=os.getenv("POSTGRES_USER"),
#             password=os.getenv("POSTGRES_PASSWORD")
#         )

#         cur = conn.cursor()

#         # -----------------------------
#         # CREATE TABLE IF NOT EXISTS
#         # -----------------------------
#         cur.execute("""
#             CREATE TABLE IF NOT EXISTS asn_registry (
#                 registry TEXT NOT NULL,
#                 country TEXT NOT NULL,
#                 asn INTEGER NOT NULL,
#                 status TEXT,
#                 PRIMARY KEY (registry, country, asn)
#             );
#         """)

#         insert_query = """
#             INSERT INTO asn_registry (registry, country, asn, status)
#             VALUES %s
#             ON CONFLICT DO NOTHING
#         """

#         batch_size = 5000
#         total_inserted = 0

#         # -----------------------------
#         # BATCH INSERT
#         # -----------------------------
#         for i in range(0, len(asn_data), batch_size):
#             batch = asn_data[i:i + batch_size]

#             execute_values(cur, insert_query, batch)
#             total_inserted += len(batch)

#             print(f"Inserted rows {i} to {i + len(batch)}")

#         conn.commit()

#         print(f"Successfully inserted {total_inserted} rows into Postgres.")

#     except Exception as e:
#         if conn:
#             conn.rollback()
#         print(f"Error inserting ASN data: {e}")
#         raise

#     finally:
#         if cur:
#             cur.close()
#         if conn:
#             conn.close()
import psycopg2
from psycopg2.extras import execute_values
import os
import csv


def load_asn(**context):
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="fetch_asn")

    if not file_path or not os.path.exists(file_path):
        raise ValueError("ASN CSV file not found.")

    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS asn_registry (
            registry TEXT NOT NULL,
            country TEXT NOT NULL,
            asn INTEGER NOT NULL,
            status TEXT,
            PRIMARY KEY (registry, country, asn)
        );
    """)
    conn.commit()

    insert_query = """
        INSERT INTO asn_registry (registry, country, asn, status)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    batch_size = 5000
    batch = []
    total_inserted = 0

    with open(file_path, "r") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header

        for row in reader:
            batch.append((row[0], row[1], int(row[2]), row[3]))

            if len(batch) >= batch_size:
                execute_values(cur, insert_query, batch)
                total_inserted += len(batch)
                batch.clear()

        # insert remaining rows
        if batch:
            execute_values(cur, insert_query, batch)
            total_inserted += len(batch)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Successfully inserted {total_inserted} rows.")