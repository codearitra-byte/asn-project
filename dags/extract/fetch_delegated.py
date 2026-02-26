# import requests
# import psycopg2
# import os
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# from psycopg2.extras import execute_batch
# import time
# from requests.exceptions import ChunkedEncodingError, RequestException
# RIR_URLS = [
#     "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
#     "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
#     "https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest",
#     "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
#     "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest"
# ]

# def create_session():
#     session = requests.Session()
#     retry = Retry(
#         total=5,
#         backoff_factor=2,
#         status_forcelist=[500, 502, 503, 504],
#         allowed_methods=["GET"],
#         raise_on_status=False
#     )
#     adapter = HTTPAdapter(max_retries=retry)
#     session.mount("http://", adapter)
#     session.mount("https://", adapter)
#     return session

# def fetch_asn(**kwargs):
#     session = create_session()
#     all_asns = []

#     for url in RIR_URLS:
#         print(f"Downloading: {url}")

#         try:
#             response = session.get(url, timeout=180)
#             if response.status_code != 200:
#                 print(f"Skipping {url}, status code: {response.status_code}")
#                 continue

#             for line in response.text.splitlines():
#                 if not line or line.startswith("#"):
#                     continue

#                 parts = line.split("|")
#                 if len(parts) > 6 and parts[2] == "asn":
#                     registry = parts[0]
#                     country = parts[1]
#                     start_asn = int(parts[3])
#                     count = int(parts[4])
#                     status = parts[6]

#                     for i in range(count):
#                         all_asns.append((registry, country, start_asn + i, status))

#             print(f"Completed: {url}")

#         except (ChunkedEncodingError, RequestException) as e:
#             print(f"Failed to download {url}: {e}")
#             continue  

#     print(f"Total ASNs fetched: {len(all_asns)}")
#     return all_asns
# # import requests
# # import csv
# # from requests.adapters import HTTPAdapter
# # from urllib3.util.retry import Retry
# # from requests.exceptions import RequestException

# # RIR_URLS = [
# #     "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
# #     "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
# #     "https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest",
# #     "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
# #     "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest"
# # ]

# # def create_session():
# #     session = requests.Session()

# #     retry = Retry(
# #         total=5,
# #         backoff_factor=3,
# #         status_forcelist=[500, 502, 503, 504],
# #         allowed_methods=["GET"],
# #         raise_on_status=False,
# #     )

# #     adapter = HTTPAdapter(max_retries=retry)

# #     session.mount("http://", adapter)
# #     session.mount("https://", adapter)

# #     return session


# # def fetch_asn(**context):
# #     session = create_session()
# #     output_file = "/tmp/asn_data.csv"
# #     total_rows = 0

# #     with open(output_file, "w", newline="", encoding="utf-8") as f:
# #         writer = csv.writer(f)

# #         for url in RIR_URLS:
# #             print(f"Downloading: {url}")

# #             try:
# #                 response = session.get(url, stream=True, timeout=(10, 300))
# #                 response.raise_for_status()

# #                 for raw_line in response.iter_lines():

# #                     if not raw_line:
# #                         continue

# #                     line = raw_line.decode("utf-8")

# #                     if line.startswith("#"):
# #                         continue

# #                     parts = line.split("|")

# #                     if len(parts) > 6 and parts[2] == "asn":

# #                         registry = parts[0]
# #                         country = parts[1]
# #                         start_asn = int(parts[3])
# #                         count = int(parts[4])
# #                         status = parts[6]

# #                         # ⚡ DO NOT expand range fully in memory
# #                         # Write range instead of exploding it
# #                         writer.writerow([
# #                             registry,
# #                             country,
# #                             start_asn,
# #                             count,
# #                             status
# #                         ])

# #                         total_rows += count

# #                 print(f"Completed: {url}")

# #             except RequestException as e:
# #                 print(f"Failed: {url} → {e}")
# #                 continue

# #     print(f"Total ASNs fetched: {total_rows}")

# #     context["ti"].xcom_push(key="asn_file", value=output_file)

# #     return total_rows
# # import requests

# # RIR_URLS = [
# #     "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
# #     "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
# #     "https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest",
# #     "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
# #     "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest",
# # ]

# # def fetch_asn():
# #     asn_records = []

# #     for url in RIR_URLS:
# #         print(f"Downloading: {url}")

# #         try:
# #             response = requests.get(url, timeout=60)
# #             response.raise_for_status()

# #             for line in response.text.splitlines():
# #                 if line.startswith("#"):
# #                     continue

# #                 parts = line.split("|")

# #                 if len(parts) < 7:
# #                     continue

# #                 if parts[2] == "asn":
# #                     registry = parts[0]
# #                     country = parts[1]
# #                     asn = int(parts[3])
# #                     status = parts[6]

# #                     asn_records.append(
# #                         (registry, country, asn, status)
# #                     )

# #         except Exception as e:
# #             print(f"Failed: {url} → {e}")
# #             continue

# #     print(f"Total ASN rows prepared: {len(asn_records)}")

# #     return asn_records
import requests
import psycopg2
import os
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ChunkedEncodingError, RequestException

RIR_URLS = [
    "https://ftp.arin.net/pub/stats/arin/delegated-arin-extended-latest",
    "https://ftp.ripe.net/pub/stats/ripencc/delegated-ripencc-extended-latest",
    "https://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest",
    "https://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-latest",
    "https://ftp.afrinic.net/pub/stats/afrinic/delegated-afrinic-extended-latest"
]

def create_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_asn(**context):

    session = create_session()
    all_asns = []

    for url in RIR_URLS:
        print(f"Downloading: {url}")

        try:
            response = session.get(url, timeout=180)
            if response.status_code != 200:
                continue

            for line in response.text.splitlines():
                if not line or line.startswith("#"):
                    continue

                parts = line.split("|")
                if len(parts) > 6 and parts[2] == "asn":
                    registry = parts[0]
                    country = parts[1]
                    start_asn = int(parts[3])
                    count = int(parts[4])
                    status = parts[6]

                    for i in range(count):
                        all_asns.append(
                            (registry, country, start_asn + i, status)
                        )

        except (ChunkedEncodingError, RequestException):
            continue

    print(f"Total ASNs fetched: {len(all_asns)}")

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
            country TEXT ,
            asn INTEGER NOT NULL,
            status TEXT,
            PRIMARY KEY (registry, asn)
        );
    """)
    conn.commit()

    insert_query = """
        INSERT INTO asn_registry (registry, country, asn, status)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    batch_size = 5000

    for i in range(0, len(all_asns), batch_size):
        batch = all_asns[i:i + batch_size]
        execute_values(cur, insert_query, batch)
        print(f"Inserted {i} → {i + len(batch)}")

    conn.commit()
    cur.close()
    conn.close()

    print("ASN load completed successfully.")