# iRacing Telemetry & Analytics Pipeline — Project Blueprint

> **Single source of truth** for the architecture, data model, implementation plan, and design rationale of this project. All other planning documents (`README.md`, `PROYECTO.md`, `FIRST-CONSIDERATIONS.md`) are superseded by this file.

---

## 1. Introduction & Objectives

This project is a hybrid **Data Engineering (Modern Data Stack)** and **Advanced Analytics** solution built around high-frequency virtual motorsport telemetry from **iRacing**. It solves real problems in pit-stop strategy, mechanical reliability control, and driver performance analysis through a portable, decoupled, containerized architecture.

| Pillar | Objective |
|--------|-----------|
| **Data Engineering (DE)** | Build a dual-ingestion ecosystem combining **real-time streaming** (via local SDK over shared memory) and **batch processing** (via the official Web REST API), with a **Stream Replay** abstraction that enables demos and CI/CD without the simulator running. |
| **Data Analytics (DA)** | Deliver low-latency visualization layers: a **Live Dashboard** for fuel management and projected pit stops, and a **Post-Race Dashboard** for deep diagnostics on weight transfer, consistency, and premature tyre wear from understeer. |

---

## 2. Theoretical Framework

### Pillar A: Data Movement & Ingestion

1. **Batch vs. Streaming.** Batch processes accumulated records in scheduled time windows. Streaming evaluates events individually and continuously at the exact millisecond of emission.
2. **ETL vs. ELT.** This project follows the modern **ELT** pattern — raw data is extracted and loaded intact into the destination to ensure persistence against failures; heavy transformation is then delegated to the storage engine's SQL.
3. **Message Broker / Pub-Sub.** A distributed messaging architecture that decouples producers from consumers. Incoming bursts are stored in persistent, disk-backed topic queues, isolating the simulator and providing backpressure protection.
4. **Idempotency.** A design guarantee that running the same pipeline multiple times with the same input data produces an identical destination state — no duplicates, no corruption.
5. **Ingestion Agnosticism (Stream Replay).** The pipeline can hot-swap its physical data source for a pre-recorded, indexed file sequence that emulates the original stream's network behaviour and latency — enabling QA and testing.

### Pillar B: Storage & Modelling

1. **OLTP vs. OLAP.** OLTP databases are optimised for fast, row-by-row transactional writes. OLAP databases process data columnar-first, optimised for massive aggregations across millions of rows.
2. **Time-Series Database (TSDB).** Engines where the absolute primary index is chronological (`Timestamp`). They follow an **append-only** structural philosophy — continuous linear writes with strict immutability restrictions on historical data.
3. **Dimensional Modelling (Star Schema).** A denormalisation strategy for analytics composed of:
   - **Fact Tables:** High-granularity tables recording cumulative numerical metrics of quantitative events (e.g., `fact_telemetry_logs`).
   - **Dimension Tables:** Descriptive context attributes that enrich fact tables (e.g., `dim_drivers`, `dim_tracks`, `dim_cars`).
4. **Data Lake vs. Data Warehouse.** The Lake stores massive, unstructured flat objects in their original format (JSON/CSV in Object Storage). The Warehouse hosts clean, transformed, catalogued data under a strict corporate schema.

### Pillar C: Analytics & Value Generation

1. **Analytical Levels.** Sequential value evolution: Descriptive (*What happened?*), Diagnostic (*Why did it happen?*), Predictive (*What will happen?*), Prescriptive (*What action mitigates the problem?*).
2. **SQL Window Functions.** Analytical functions that compute calculations over adjacent rows or specific partitions without collapsing the physical granularity of rows as `GROUP BY` does.
3. **Proxy Metric.** A synthetic mathematical indicator designed to quantify a complex or abstract physical variable that lacks a direct analogue/boolean sensor signal in the source hardware.

---

## 3. iRacing Data Extraction Fundamentals

### 3.1 Polling over Shared Memory (MMF)

The simulator uses a passive data architecture — it does not propagate network signals by default. When `iRacingSim64DX11.exe` initialises, Windows reserves a memory-mapped file accessible under the global local identifier **`Local\IRSDKMemMapFileName`**.

The physics engine updates this map at a constant rate of **60 Hz (every 16.6 ms)**. Our Python script acts as a continuous asynchronous poller with explicit kernel-level read permissions on this virtual memory address.

### 3.2 Active Session Detection

The shared memory organises its content into indexed binary blocks (*Telemetry Variables*) and heavy relational metadata encoded in plain-text YAML (*Session Info*). The ingestion flow first evaluates the boolean connection bit **`IsConnected`**. When the driver joins a session, the bit flips to `True`. The operational context is extracted dynamically by parsing the YAML hierarchy: `SessionInfo → Sessions → SessionType` (`Practice`, `Lone Qualify`, `Race`).

### 3.3 Lap Crossing Logic (State Change Detection)

iRacing has no native "push" event for crossing the start/finish line. The pipeline infers this analytical milestone by implementing a **State Change Detection** algorithm on the integer `Lap` variable:

```python
ir = irsdk.IRSDK()
ir.startup()

prev_lap = 0
while True:
    if ir.is_connected():
        current_lap = ir['Lap']
        if current_lap > prev_lap:  # Increment confirms the exact crossing
            lap_time = ir['LapLastLapTime']
            fuel_remaining = ir['FuelLevel']
            # Dispatch structured analytical payload for lap closure
            prev_lap = current_lap
    time.sleep(1 / 60)
```

---

## 4. Optimisation Patterns

Direct processing of 60 Hz signals against a relational database would generate **1,296,000 SQL write statements per 6-hour race**, saturating the storage I/O subsystem. Four advanced architectural patterns solve this:

### 4.1 Downsampling (Programmatic Sub-sampling to 10 Hz)

The collector reads shared memory but applies a frame-discard filter, processing only 1 out of every 6 available RAM states. This transforms the flow to **10 Hz** — reducing computational load by **83.33%** without compromising telemetry resolution in fast corners.

### 4.2 Micro-batching (1-Second Temporal Buffer)

The script linearly accumulates the 10 samples obtained per second in an in-memory list (`buffer = []`). When the 1000 ms window completes, it packages the batch into a single structured JSON array and makes one asynchronous bulk call to Kafka. This **reduces network transactions and database engine stress by 98.33%** — from 1.29M individual inserts to only **21,600 bulk operations** per race.

### 4.3 Event-Driven Deadband

Maximum-priority interruption rule: the buffer operates on a 1-second timer, **except when an instant mutation occurs on the `Lap` variable**. If the driver crosses the start/finish line, the buffer aborts the current timer, flushes immediately, and transmits the state to Kafka synchronously. This guarantees **zero latency** on critical pit-wall metric updates.

### 4.4 Stream Replay Pattern (Demo Isolation)

For evaluation environments or technical interviews where the simulator is inaccessible, the producer implements an environment-variable-controlled logic switch. In replay mode, the script reads a pre-captured `.json` file from real on-track conditions and injects it sequentially into the Kafka topic at the same 10 Hz cadence — producing an **identical and indistinguishable replica** of the live flow for Grafana.

---

## 5. Pipeline Architecture

```text
       [ iRacing Sim ] (Windows RAM MMF 60Hz)          [ JSON Replay File ] (GitHub Storage)
              │                                                │
              └───────────────┬────────────────────────────────┘
                              ▼
               ┌───────────────────────────────┐
               │ INGESTION LAYER (PRODUCER)    │
               │ Python Daemon + pyirsdk       │ <── Controlled by Environment:
               │ - PIPELINE_MODE=LIVE          │     [LIVE / REPLAY]
               │ - PIPELINE_MODE=REPLAY        │
               └──────────────┬────────────────┘
                              │
                              ▼ (JSON Bulk Arrays / 1 Second)
               ┌───────────────────────────────┐
               │ STREAMING LAYER               │
               │ Apache Kafka (KRaft Mode)     │
               └──────────────┬────────────────┘
                              │
                              ▼ (Event Consumption)
               ┌───────────────────────────────┐
               │ PROCESSING LAYER (CONSUMER)   │
               │ Python Stream Writer          │ <── Batch Processing Line (Airflow DAG)
               └──────────────┬────────────────┘     [ Web REST API → MinIO S3 → dbt ]
                              │                                       │
                              ▼                                       ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│ STORAGE & ANALYTICAL MODELLING LAYER                                          │
│ PostgreSQL + TimescaleDB (TSDB Hypertables & OLAP Dimensional Star Schema)    │
└─────────────────────────────┬─────────────────────────────────────────────────┘
                              │
                              ▼ (High Frequency Clean SQL Queries)
               ┌───────────────────────────────┐
               │ VISUALISATION LAYER            │
               │ Grafana Enterprise Dashboards │
               └───────────────────────────────┘
```

### Technology Stack

| Component | Technology | Role |
|-----------|-----------|------|
| Core Language | **Python** | Ingestion logic, state-delta computation, network call automation |
| SDK Wrapper | **pyirsdk** | Compiles Windows binary memory mapping into native Python dicts |
| REST Client | **iracingdataapi** | Cookie-persistent REST client for historical batch downloads from iRacing servers |
| Message Broker | **Apache Kafka (KRaft)** | Async event messaging layer; KRaft mode eliminates Zookeeper, operates under 1 GB RAM |
| Time-Series DB | **PostgreSQL + TimescaleDB** | Hybrid relational engine with Hypertables — automatic chronological partitioning for accelerated temporal queries |
| Data Lake | **MinIO** | Open-source S3-compatible object storage for landing raw historical JSON payloads |
| Batch Orchestrator | **Apache Airflow** | Enterprise scheduler for batch DAG orchestration, monitoring, and automatic retries |
| Transformation | **dbt (Data Build Tool)** | Analytical engineering layer (the T in ELT) — modular, parameterised SQL to build the final dimensional warehouse |
| Visualisation | **Grafana** | Dynamic dashboard engine; executes optimised analytical reads against TimescaleDB Hypertables |
| Infrastructure | **Docker & Docker Compose** | OS-level isolation and virtualisation; packages the entire topology for single-command deployment (`docker-compose up`) |

---

## 6. Data Dictionary & KPIs

### A. Lap Consistency & Timing

| Variable | Type | Description |
|----------|------|-------------|
| `SessionTime` | Float (PK) | Absolute accumulated session time in seconds |
| `Lap` | Integer | Current lap number identifier |
| `LapLastLapTime` | Float | Chronometric time of the last completed lap |

### B. Fuel Management (Real-Time Analytics)

| Variable | Type | Description |
|----------|------|-------------|
| `FuelLevel` | Float | Volumetric fuel available in the tank (litres) |
| `Fuel_Burned_Last_Lap` | Computed | $\text{FuelLevel}_{\text{Lap } N-1} - \text{FuelLevel}_{\text{Lap } N}$ |
| **Projected Lap Autonomy** | KPI | $\frac{\text{FuelLevel}}{\text{Fuel\_Burned\_Last\_Lap}}$ |

### C. Tyre & Chassis Diagnostics (Understeer / Oversteer)

| Variable | Type | Description |
|----------|------|-------------|
| `SteeringWheelAngle` | Float | Geometric steering wheel rotation angle (radians) |
| `Throttle` | Float | Throttle pedal pressure (0.0 – 1.0) |
| `Brake` | Float | Brake pedal pressure (0.0 – 1.0) |
| `LfTemp`, `RfTemp` | Float | Front-axle tyre surface temperatures (°C) |
| `LrTemp`, `RrTemp` | Float | Rear-axle tyre surface temperatures (°C) |

**Proxy KPI — Thermal Differential:**

$$\Delta T = \text{Avg}(T_{\text{front}}) - \text{Avg}(T_{\text{rear}})$$

| Condition | Signature | Physics |
|-----------|-----------|---------|
| **Understeer** | $\Delta T \gg 0$ sustained during mid-corner load + extreme `SteeringWheelAngle` + open `Throttle` | Front tyres scrub excessively; car pushes wide |
| **Oversteer** | $\Delta T \ll 0$ with abrupt localised rear-tyre friction spikes + severe yaw acceleration deviations | Rear traction loss; car rotates beyond driver intent |

---

## 7. Repository Structure

```text
iracing-telemetry-pipeline/
├── .gitignore
├── README.md                      # Portfolio-facing documentation with architecture diagrams
├── BLUEPRINT.md                   # This file — single source of truth for design & specs
├── docker-compose.yml             # Centralised deployment: Kafka, TimescaleDB, MinIO, Airflow, Grafana
├── collector/                     # Ingestion & Extraction Module (Native Windows Environment)
│   ├── requirements.txt           # Dependencies (pyirsdk, kafka-python)
│   ├── main_collector.py          # Collector script with Downsampling, Micro-batch & Replay Switcher
│   └── sample_data/               # Static datasets recorded from real on-track sessions
│       └── recorded_race_5laps.json
├── consumer/                      # Consumer & Loading Layer (Dockerised Linux Container)
│   ├── Dockerfile
│   ├── requirements.txt           # Dependencies (kafka-python, psycopg2-binary)
│   └── db_writer.py               # Kafka event reader & bulk SQL injector into TimescaleDB
├── dbt_transformations/           # dbt project for OLAP analytical transformations
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/               # Raw JSON cleansing & indexing
│       └── marts/                 # Final fact & dimension models (Star Schema)
├── db/                            # Database Initialisation & Schemas
│   └── init.sql                   # DDL for telemetry tables, composite indices & Hypertables
└── dashboards/                    # Observability Templates
    └── grafana_telemetry.json     # Exported JSON dashboard definition
```

---

## 8. Database Schema

```sql
-- Core relational table
CREATE TABLE telemetry_logs (
    time           TIMESTAMPTZ NOT NULL,
    session_time   NUMERIC,
    lap_number     INT,
    lap_time       NUMERIC,
    fuel_level     NUMERIC,
    steering_angle NUMERIC,
    throttle       NUMERIC,
    brake          NUMERIC,
    temp_lf        NUMERIC,
    temp_rf        NUMERIC,
    temp_lr        NUMERIC,
    temp_rr        NUMERIC
);

-- Convert to TimescaleDB Hypertable for automatic time-based partitioning
SELECT create_hypertable('telemetry_logs', 'time');

-- Composite index to accelerate per-lap analytical queries
CREATE INDEX ix_telemetry_lap_analysis ON telemetry_logs (lap_number, time DESC);
```

---

## 9. Implementation Plan

### Phase 1: Infrastructure & Hypertables (Docker & SQL)

1. Write `docker-compose.yml` orchestrating: **Kafka (KRaft mode — no Zookeeper)**, **TimescaleDB**, **MinIO**, and **Grafana**.
2. Configure internal Docker networks for secure inter-container communication.
3. Deploy the physical schema via `db/init.sql` (see Section 8 above).
4. Validate that all services start correctly with `docker-compose up` and basic health checks.

### Phase 2: Multi-Purpose Producer (Collector Script)

Build the modularised capture logic in `main_collector.py` with the environment-driven mode switch:

```python
import json
import os
import time
from kafka import KafkaProducer

PIPELINE_MODE = os.getenv('PIPELINE_MODE', 'LIVE')
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)


def stream_generator():
    if PIPELINE_MODE == 'LIVE':
        import irsdk

        ir = irsdk.IRSDK()
        ir.startup()
        prev_lap = 0
        while True:
            if ir.is_connected():
                current_lap = ir['Lap']
                payload = {
                    'session_time': ir['SessionTime'],
                    'lap': current_lap,
                    'fuel': ir['FuelLevel'],
                    'steering': ir['SteeringWheelAngle'],
                    'throttle': ir['Throttle'],
                    'brake': ir['Brake'],
                    'temp_lf': ir['LFtempCL'],
                    'temp_rf': ir['RFtempCL'],
                    'temp_lr': ir['LRtempCL'],
                    'temp_rr': ir['RRtempCL'],
                }
                # Event-Driven Deadband: immediate flush on lap change
                if current_lap > prev_lap:
                    payload['lap_time'] = ir['LapLastLapTime']
                    payload['lap_crossed'] = True
                    prev_lap = current_lap
                yield payload
            time.sleep(0.1)  # Downsampling to 10 Hz
    else:
        # REPLAY MODE: Pre-recorded telemetry, no simulator dependency
        with open('sample_data/recorded_race_5laps.json', 'r') as f:
            mock_data = json.load(f)
        for data_point in mock_data:
            yield data_point
            time.sleep(0.1)  # Exact 10 Hz streaming cadence


buffer = []
for payload in stream_generator():
    buffer.append(payload)
    # Flush on lap crossing (Deadband) or when 1-second buffer is full
    lap_crossed = payload.get('lap_crossed', False)
    if lap_crossed or len(buffer) >= 10:
        producer.send('iracing-live-telemetry', value=buffer)
        buffer = []
```

### Phase 3: Stream Consumer & Distributed Loading

1. Develop the `db_writer.py` microservice that continuously drains the Kafka topic.
2. Use `psycopg2.extras.execute_values` for efficient bulk-insert transactions into TimescaleDB.
3. Implement **stateful lap-change detection**: track `Lap` value changes to compute `Fuel_Burned_Last_Lap` and persist per-lap summary records.
4. Configure automatic reconnection with **exponential backoff retry policies** so the consumer doesn't crash if the database is slow to start.

### Phase 4: Batch Orchestration, dbt Transformation & Grafana

1. **Airflow DAGs**: Schedule tasks to download extended historical data from the iRacing REST API and land them as raw JSON payloads in **MinIO** (S3-compatible) buckets.
2. **dbt Models**:
   - `staging/`: Cleanse and index raw JSON objects from MinIO into normalised relational tables.
   - `marts/`: Build the final **Star Schema** with fact tables (`fact_telemetry_logs`, `fact_lap_summaries`) and dimension tables (`dim_drivers`, `dim_tracks`, `dim_cars`).
3. **Grafana Dashboards**:
   - **Live Dashboard**: Gauge panels for current fuel level, projected remaining laps, and a bar chart of last lap time.
   - **Post-Race Dashboard**: Scatter plot pairing `SteeringWheelAngle` against the tyre temperature differential ($\Delta T$) to visually map which sectors/corners generate understeer.
4. Connect Grafana to TimescaleDB/PostgreSQL as the official data source.

---

## 10. Future Enhancements

These are stretch goals for expanding the project beyond v1:

| Enhancement | Description |
|-------------|-------------|
| **Real-Time Stream Processing Engine** | Replace the Python consumer's in-process calculations with **ksqlDB** or **Apache Flink** over Kafka for enterprise-grade windowed aggregations before data reaches TimescaleDB. |
| **Data Quality & Alerts** | Use Grafana's alerting engine or dbt tests to fire notifications (e.g., Discord/Slack webhook) when a metric breaches thresholds ("Critical Fuel", "Severe Understeer Detected"). |
| **Schema Registry** | Add Confluent Schema Registry (Avro or Protobuf instead of JSON) to enforce data contracts between the Windows Collector and the Dockerised Consumer — reducing payload size and ensuring schema validation. |
| **Pipeline Observability** | Monitor pipeline health metrics (consumer lag, buffer overflows, DB write latency) alongside the business dashboards. |
| **Security Hardening** | Kafka SASL/SCRAM authentication, secrets management for DB credentials, Grafana auth configuration. |
| **Automated Testing** | Unit tests for the producer/consumer, integration tests using Replay mode, and dbt data validation tests in CI. |

---

## 11. Why This Project Stands Out

Unlike generic data analysis portfolios built on static Kaggle CSVs, this design demonstrates **Senior / Upper-Intermediate** competencies:

- **Real Streaming.** Processes asynchronous event flows produced by external simulation software — not flat files.
- **Modern Data Stack.** Combines Kafka for messaging, specialised time-series databases, and Grafana — emulating the telemetry infrastructure of real-world racing teams (Formula 1, WEC).
- **Business & Domain Focus.** Demonstrates the ability to transform raw accelerator and temperature data into actionable analytical metrics (mechanical fault detection and fuel strategy).
- **Software Engineering Applied to Data.** The decoupled Replay Mode is not just a convenience — it's an agnostic engine designed for **CI/CD**, enabling regression tests, data audits, and automated dbt tests on every commit without human interaction.
- **Cloud Cost Efficiency.** The coupling of downsampling and micro-batching patterns demonstrates engineering thinking oriented toward extreme reduction of operational I/O costs in production cloud storage infrastructure.
