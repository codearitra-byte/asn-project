# # import requests
# # import psycopg2
# # import os

# # PEERINGDB_API = "https://www.peeringdb.com/api"

# # def enrich_asn():

# #     print("Fetching full PeeringDB datasets...")

# #     nets = requests.get(f"{PEERINGDB_API}/net").json()["data"]
# #     netixlan = requests.get(f"{PEERINGDB_API}/netixlan").json()["data"]
# #     netfac = requests.get(f"{PEERINGDB_API}/netfac").json()["data"]

# #     print("Building lookup maps...")

    
# #     ixp_map = {}
# #     for row in netixlan:
# #         net_id = row["net_id"]
# #         ixp_map.setdefault(net_id, 0)
# #         ixp_map[net_id] += 1

   
# #     fac_map = {}
# #     for row in netfac:
# #         net_id = row["net_id"]
# #         fac_map.setdefault(net_id, 0)
# #         fac_map[net_id] += 1

    
# #     net_map = {}
# #     for net in nets:
# #         net_map[net["asn"]] = {
# #             "net_id": net["id"],
# #             "name": net.get("name"),
# #             "website": net.get("website")
# #         }

# #     print("Connecting to Postgres...")

# #     conn = psycopg2.connect(
# #         host="postgres",
# #         database=os.getenv("POSTGRES_DB"),
# #         user=os.getenv("POSTGRES_USER"),
# #         password=os.getenv("POSTGRES_PASSWORD"),
# #     )
# #     cur = conn.cursor()

# #     cur.execute("""
# #         CREATE TABLE IF NOT EXISTS asn_enrichment (
# #             asn BIGINT PRIMARY KEY,
# #             organization_name TEXT,
# #             website TEXT,
# #             ixp_count INTEGER,
# #             facility_count INTEGER
# #         );
# #     """)

# #     conn.commit()

    
# #     cur.execute("SELECT asn FROM asn_registry;")
# #     your_asns = [row[0] for row in cur.fetchall()]

# #     print("Joining datasets...")

# #     insert_data = []

# #     for asn in your_asns:
# #         if asn in net_map:
# #             net_id = net_map[asn]["net_id"]

# #             insert_data.append((
# #                 asn,
# #                 net_map[asn]["name"],
# #                 net_map[asn]["website"],
# #                 ixp_map.get(net_id, 0),
# #                 fac_map.get(net_id, 0)
# #             ))

# #     print(f"Bulk inserting {len(insert_data)} rows...")

# #     from psycopg2.extras import execute_values

# #     execute_values(
# #         cur,
# #         """
# #         INSERT INTO asn_enrichment
# #         (asn, organization_name, website, ixp_count, facility_count)
# #         VALUES %s
# #         ON CONFLICT (asn) DO UPDATE
# #         SET organization_name=EXCLUDED.organization_name,
# #             website=EXCLUDED.website,
# #             ixp_count=EXCLUDED.ixp_count,
# #             facility_count=EXCLUDED.facility_count;
# #         """,
# #         insert_data,
# #         page_size=5000
# #     )

# #     conn.commit()
# #     cur.close()
# #     conn.close()

# #     print("Enrichment completed successfully.")
# import requests
# import psycopg2
# import os
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from psycopg2.extras import execute_values

# PEERINGDB_API = "https://www.peeringdb.com/api"

# def fetch_asn(asn):
#     headers = {"User-Agent": "Airflow-ASN-Research/1.0"}
#     try:
#         net_resp = requests.get(f"{PEERINGDB_API}/net?asn={asn}", headers=headers, timeout=20)
#         net_json = net_resp.json()

#         if "data" not in net_json or not net_json["data"]:
#             return None

#         net = net_json["data"][0]
#         net_id = net["id"]

#         ixp_resp = requests.get(f"{PEERINGDB_API}/netixlan?net_id={net_id}", headers=headers, timeout=20)
#         fac_resp = requests.get(f"{PEERINGDB_API}/netfac?net_id={net_id}", headers=headers, timeout=20)

#         ixp_count = len(ixp_resp.json().get("data", []))
#         fac_count = len(fac_resp.json().get("data", []))

#         return (
#             asn,
#             net.get("name"),
#             net.get("website"),
#             ixp_count,
#             fac_count
#         )

#     except Exception:
#         return None


# def enrich_asn():

#     conn = psycopg2.connect(
#         host="postgres",
#         database=os.getenv("POSTGRES_DB"),
#         user=os.getenv("POSTGRES_USER"),
#         password=os.getenv("POSTGRES_PASSWORD"),
#     )
#     cur = conn.cursor()

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS asn_enrichment (
#             asn BIGINT PRIMARY KEY,
#             organization_name TEXT,
#             website TEXT,
#             ixp_count INTEGER,
#             facility_count INTEGER
#         );
#     """)
#     conn.commit()

#     cur.execute("SELECT asn FROM asn_registry LIMIT 20000;")
#     asns = [row[0] for row in cur.fetchall()]

#     results = []

#     with ThreadPoolExecutor(max_workers=20) as executor:
#         futures = [executor.submit(fetch_asn, asn) for asn in asns]

#         for future in as_completed(futures):
#             result = future.result()
#             if result:
#                 results.append(result)

#     execute_values(
#         cur,
#         """
#         INSERT INTO asn_enrichment
#         (asn, organization_name, website, ixp_count, facility_count)
#         VALUES %s
#         ON CONFLICT (asn) DO UPDATE
#         SET organization_name=EXCLUDED.organization_name,
#             website=EXCLUDED.website,
#             ixp_count=EXCLUDED.ixp_count,
#             facility_count=EXCLUDED.facility_count;
#         """,
#         results,
#         page_size=1000
#     )

#     conn.commit()
#     cur.close()
#     conn.close()
# import requests
# import psycopg2
# import os
# from psycopg2.extras import execute_values

# PEERINGDB_API = "https://www.peeringdb.com/api"

# def enrich_asn(**context):

#     headers = {"User-Agent": "Airflow-ASN-Research/1.0"}

#     print("Fetching full PeeringDB datasets...")

#     nets = requests.get(f"{PEERINGDB_API}/net", headers=headers, timeout=60).json()["data"]
#     netixlan = requests.get(f"{PEERINGDB_API}/netixlan", headers=headers, timeout=60).json()["data"]
#     netfac = requests.get(f"{PEERINGDB_API}/netfac", headers=headers, timeout=60).json()["data"]

#     print("Building lookup maps...")

#     # Map ASN → net info
#     net_map = {}
#     for net in nets:
#         if net.get("asn"):
#             net_map[net["asn"]] = {
#                 "net_id": net["id"],
#                 "name": net.get("name"),
#                 "website": net.get("website")
#             }

#     # Map net_id → ixp count
#     ixp_map = {}
#     for row in netixlan:
#         net_id = row["net_id"]
#         ixp_map[net_id] = ixp_map.get(net_id, 0) + 1

#     # Map net_id → facility count
#     fac_map = {}
#     for row in netfac:
#         net_id = row["net_id"]
#         fac_map[net_id] = fac_map.get(net_id, 0) + 1

#     print("Connecting to Postgres...")

#     conn = psycopg2.connect(
#         host="postgres",
#         database=os.getenv("POSTGRES_DB"),
#         user=os.getenv("POSTGRES_USER"),
#         password=os.getenv("POSTGRES_PASSWORD"),
#     )
#     cur = conn.cursor()

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS asn_enrichment (
#             asn BIGINT PRIMARY KEY,
#             organization_name TEXT,
#             website TEXT,
#             ixp_count INTEGER,
#             facility_count INTEGER
#         );
#     """)
#     conn.commit()

#     # Fetch your ASN list
#     cur.execute("SELECT asn FROM asn_registry;")
#     your_asns = [row[0] for row in cur.fetchall()]

#     print(f"Joining {len(your_asns)} ASNs with PeeringDB...")

#     insert_data = []

#     for asn in your_asns:
#         if asn in net_map:
#             net_id = net_map[asn]["net_id"]

#             insert_data.append((
#                 asn,
#                 net_map[asn]["name"],
#                 net_map[asn]["website"],
#                 ixp_map.get(net_id, 0),
#                 fac_map.get(net_id, 0)
#             ))

#     print(f"Bulk inserting {len(insert_data)} enriched rows...")

#     execute_values(
#         cur,
#         """
#         INSERT INTO asn_enrichment
#         (asn, organization_name, website, ixp_count, facility_count)
#         VALUES %s
#         ON CONFLICT (asn) DO UPDATE
#         SET organization_name=EXCLUDED.organization_name,
#             website=EXCLUDED.website,
#             ixp_count=EXCLUDED.ixp_count,
#             facility_count=EXCLUDED.facility_count;
#         """,
#         insert_data,
#         page_size=5000
#     )

#     conn.commit()
#     cur.close()
#     conn.close()

#     print("Enrichment completed successfully.")
import requests
import psycopg2
import os
import time
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PEERINGDB_API = "https://www.peeringdb.com/api"


def create_session():
    session = requests.Session()

    retry = Retry(
        total=6,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    session.headers.update({
        "User-Agent": "Airflow-ASN-Research/1.0 (research)",
        "Accept": "application/json"
    })

    return session


def fetch_all(endpoint, session):
    results = []
    limit = 1000
    skip = 0

    while True:
        url = f"{PEERINGDB_API}/{endpoint}?limit={limit}&skip={skip}"
        print(f"Requesting {url}")

        response = session.get(url, timeout=180)

        if response.status_code != 200:
            print(f"ERROR {endpoint} - Status: {response.status_code}")
            print(response.text[:300])
            raise Exception(f"PeeringDB API failure on {endpoint}")

        payload = response.json()
        data = payload.get("data", [])

        if not data:
            break

        results.extend(data)
        skip += limit

        print(f"{endpoint}: total fetched {len(results)}")
        time.sleep(0.5)

    if not results:
        raise Exception(f"No data returned from {endpoint}")

    return results


def enrich_asn(**context):

    session = create_session()

    print("Fetching PeeringDB datasets...")

    nets = fetch_all("net", session)
    netixlan = fetch_all("netixlan", session)
    netfac = fetch_all("netfac", session)

    print("Building lookup maps...")

    net_map = {}
    for net in nets:
        asn = net.get("asn")
        if asn:
            net_map[int(asn)] = {
                "net_id": net["id"],
                "name": net.get("name"),
                "website": net.get("website")
            }

    ixp_map = {}
    for row in netixlan:
        net_id = row["net_id"]
        ixp_map[net_id] = ixp_map.get(net_id, 0) + 1

    fac_map = {}
    for row in netfac:
        net_id = row["net_id"]
        fac_map[net_id] = fac_map.get(net_id, 0) + 1

    print("Connecting to Postgres...")

    conn = psycopg2.connect(
        host="postgres",
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS asn_enrichment (
            asn BIGINT PRIMARY KEY,
            organization_name TEXT,
            website TEXT,
            ixp_count INTEGER,
            facility_count INTEGER
        );
    """)
    conn.commit()

    cur.execute("SELECT asn FROM asn_registry;")
    your_asns = [row[0] for row in cur.fetchall()]

    print(f"Joining {len(your_asns)} ASNs...")

    insert_data = []

    for asn in your_asns:
        if asn in net_map:
            net_id = net_map[asn]["net_id"]
            insert_data.append((
                asn,
                net_map[asn]["name"],
                net_map[asn]["website"],
                ixp_map.get(net_id, 0),
                fac_map.get(net_id, 0)
            ))

    if not insert_data:
        raise Exception("Enrichment resulted in 0 rows — API likely blocked")

    print(f"Inserting {len(insert_data)} enriched ASNs...")

    execute_values(
        cur,
        """
        INSERT INTO asn_enrichment
        (asn, organization_name, website, ixp_count, facility_count)
        VALUES %s
        ON CONFLICT (asn) DO UPDATE
        SET organization_name=EXCLUDED.organization_name,
            website=EXCLUDED.website,
            ixp_count=EXCLUDED.ixp_count,
            facility_count=EXCLUDED.facility_count;
        """,
        insert_data,
        page_size=5000
    )

    conn.commit()
    cur.close()
    conn.close()

    print("Enrichment completed successfully.")