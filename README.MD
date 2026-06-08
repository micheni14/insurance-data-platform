# 🏥 Insurance Data Platform
### End-to-End Data Engineering Portfolio Project

> Built by **Lewis Micheni** | Aspiring Data Engineer  
> Stack: PostgreSQL · Python · Apache Kafka · Docker · SQLAlchemy · Pandas · Matplotlib

---

## 📌 Project Overview

A production-style data engineering platform for an insurance company operating across Kenya.  
The platform ingests raw transactional data, transforms it through a star schema data warehouse, runs Python analytics, and streams real-time events via Apache Kafka — all running locally with Docker.

---

## 🏗️ Architecture

```
Raw Data (Faker)
      │
      ▼
┌─────────────────────────────────────────────────────┐
│                  PostgreSQL Database                 │
│                                                     │
│  ┌─────────────┐   ETL Pipelines   ┌─────────────┐ │
│  │  OLTP Layer │ ───────────────►  │ Star Schema │ │
│  │  customers  │                   │  fact_sales │ │
│  │  policies   │                   │ fact_claims │ │
│  │  claims     │                   │fact_payments│ │
│  │  agents     │                   │  dim_date   │ │
│  │  payments   │                   │ dim_policy  │ │
│  └─────────────┘                   └─────────────┘ │
└─────────────────────────────────────────────────────┘
      │                                     │
      ▼                                     ▼
┌──────────────┐                  ┌──────────────────┐
│ Kafka Stream │                  │  Python Analytics│
│  Producer    │                  │  10 Charts +     │
│  Consumer    │                  │  Master Dashboard│
│  kafka_      │                  │  (Matplotlib /   │
│  events_log  │                  │   Seaborn)       │
└──────────────┘                  └──────────────────┘
```

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 15 |
| ORM / Connections | SQLAlchemy, psycopg2 |
| Data Generation | Faker, NumPy |
| ETL & Analytics | Python, Pandas |
| Visualisation | Matplotlib, Seaborn |
| Streaming | Apache Kafka 4.3.0 (KRaft mode) |
| Containerisation | Docker Desktop |
| Environment | python-dotenv |

---

## 📁 Project Structure

```
insurance-data-platform/
│
├── scripts/
│   ├── 01_generate_customers.py     # Generates 2,000 synthetic customers
│   ├── 02_load_policies.py          # Creates 2,000 insurance policies
│   ├── 03_load_claims.py            # Loads 814 claims with statuses
│   ├── 04_load_agents.py            # Loads 10 agents across counties
│   ├── 05_load_sales.py             # Sales transactions
│   ├── 06_load_payments.py          # 16,679 payment records
│   ├── 07_populate_star_schema.py   # Builds dimensional model
│   ├── 08_load_fact_sales.py        # Populates fact_sales
│   ├── 09_load_fact_claims.py       # Populates fact_claims
│   ├── etl_fact_payments.py         # ETL pipeline for fact_payments
│   ├── 11_analytics.py              # Analytics & 10 chart generation
│   ├── 10_kafka_producer.py         # Real-time event producer
│   └── 10_kafka_consumer.py         # Real-time event consumer → PostgreSQL
│
├── reports/
│   ├── 00_master_dashboard.png
│   ├── 01_monthly_premium_trend.png
│   ├── 02_premium_by_county.png
│   ├── 03_policy_type_distribution.png
│   ├── 04_claims_by_status.png
│   ├── 05_loss_ratio.png
│   ├── 06_agent_performance.png
│   ├── 07_payment_methods.png
│   ├── 08_late_payments.png
│   ├── 09_age_band_analysis.png
│   └── 10_collections_vs_premium.png
│
├── docker-compose.yml               # Kafka (KRaft mode, no Zookeeper)
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- PostgreSQL 15+
- Docker Desktop

### 1. Clone the repository
```bash
git clone https://github.com/lewismicheni/insurance-data-platform.git
cd insurance-data-platform
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure environment
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 4. Run the data pipeline (in order)
```bash
python scripts/01_generate_customers.py
python scripts/02_load_policies.py
python scripts/03_load_claims.py
python scripts/04_load_agents.py
python scripts/05_load_sales.py
python scripts/06_load_payments.py
python scripts/07_populate_star_schema.py
python scripts/08_load_fact_sales.py
python scripts/09_load_fact_claims.py
python scripts/etl_fact_payments.py
python scripts/11_analytics.py
```

### 5. Start Kafka (Docker)
```bash
docker-compose up -d
```

### 6. Run real-time streaming
```bash
# Terminal 1 — Consumer (start first)
python scripts/10_kafka_consumer.py

# Terminal 2 — Producer
python scripts/10_kafka_producer.py
```

---

## 📊 Analytics Dashboard

![Master Dashboard](/00_master_dashboard.png)

The platform generates 10 analytical charts + a master dashboard:

| Chart | Insight |
|---|---|
| Monthly Premium Trend | Revenue growth over time |
| Premium by County | Geographic distribution across Kenya |
| Policy Type Distribution | Auto, Home, Life, Health, Business split |
| Claims by Status | Pending / Approved / Rejected breakdown |
| Loss Ratio | Claims paid vs premiums collected |
| Agent Performance | Top performing agents by premium |
| Payment Methods | M-Pesa, Bank Transfer, Credit Card, Cash |
| Late Payment Rates | Delinquency trends |
| Customer Age Bands | Demographic segmentation |
| Collections vs Premium | Cash collection efficiency |

---

## ⚡ Real-Time Kafka Streaming

The platform simulates a live insurance event stream:

```
Producer ──► insurance-events (Kafka Topic, 3 partitions)
                    │
                    ▼
             Consumer reads events
                    │
                    ▼
         kafka_events_log (PostgreSQL)
```

**Event types streamed:**
- `new_policy` — simulates a customer buying a policy
- `new_claim` — simulates an incident and claim filing
- `new_payment` — simulates a premium payment

**Results after 3 minutes of streaming:**

| Event Type | Events | Total Value (KES) |
|---|---|---|
| new_claim | 68 | 17,837,817 |
| new_payment | 73 | 1,968,513 |
| new_policy | 57 | 2,558,432 |

---

## 🗄️ Database Summary

| Metric | Value |
|---|---|
| Total Customers | 2,000 |
| Total Policies | 2,000 |
| Total Claims | 814 |
| Total Payments | 16,679 |
| Gross Premium | KES 54,728,078 |
| Total Claims Amount | KES 85,141,367 |
| Total Collected | KES 42,634,634 |

---

## 🌍 Data Context

Data is modelled for the **Kenyan insurance market**:
- Counties: Nairobi, Mombasa, Kisumu, Nakuru, Eldoret, and more
- Payment methods include **M-Pesa** (Kenya's dominant mobile money platform)
- Currency: KES (Kenyan Shilling)

---

## 📋 Environment Variables

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=insurance_db
DB_USER=postgres
DB_PASSWORD=your_password
```

---

## 📦 Requirements

```
faker
matplotlib
numpy
psycopg2-binary
python-dotenv
seaborn
SQLAlchemy
kafka-python
```

