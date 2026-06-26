from google.cloud import bigquery
from google.cloud import storage

PROJECT_ID = "insurance-data-platform-500423"

# Test 1 — BigQuery
print("Testing BigQuery...")
bq = bigquery.Client(project=PROJECT_ID)
print(f"✅ BigQuery connected — project: {bq.project}")

# Test 2 — Cloud Storage
print("\nTesting Cloud Storage...")
gcs = storage.Client(project=PROJECT_ID)
buckets = list(gcs.list_buckets())
print(f"✅ Cloud Storage connected — {len(buckets)} buckets found")

# Test 3 — Run a simple BigQuery query
print("\nTesting BigQuery query...")
query = "SELECT 1 + 1 AS result"
result = bq.query(query).result()
for row in result:
    print(f"✅ BigQuery query works — 1 + 1 = {row.result}")

print("\n🎉 All GCP connections working!")