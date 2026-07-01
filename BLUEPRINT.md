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

For evaluation environments or technical interviews where the simulator is inaccessible, a **dedicated cross-platform Replayer component** (`replayer/replay.py`) reads a pre-captured CSV file (e.g., exported from Garage61 or a previous session) and injects it sequentially into the Kafka topic at the same 10 Hz cadence — producing an **identical and indistinguishable replica** of the live flow for Grafana. The Replayer is architecturally separated from the Collector: it has no dependency on `pyirsdk` or Windows, and can run on any machine with network access to Kafka (including the backend server itself).

---

## 5. Pipeline Architecture

```text
  MACHINE A — Windows (iRacing Host)
  ════════════════════════════════════════════════════════════════

       [ iRacing Sim ] (Windows RAM MMF 60Hz)
              │
              ▼
       ┌───────────────────────────────┐
       │ COLLECTOR (LIVE ONLY)         │
       │ iRacingCollector.exe          │ ◄── Standalone Windows Executable
       │ config.ini → Kafka IP         │     (PyInstaller packaged)
       └──────────────┬────────────────┘
                      │
                      ▼ (JSON Bulk Arrays / 1 Second)
              ┌───────────────┐
              │   LAN / WiFi  │
              └───────┬───────┘

  ANY MACHINE (Windows, Linux, Mac)             MACHINE B — Linux (Backend & Analytics Server)
  ═════════════════════════════════════════     ════════════════════════════════════════════════════════════════════

  [ CSV Telemetry File ]                                        │
         │                                                      │
         ▼                                                      ▼
  ┌───────────────────────────────────┐             ┌───────────────────────────────┐
  │ REPLAYER (Cross-Platform)         │             │ STREAMING LAYER               │
  │ replay.py                         │             │ Apache Kafka (KRaft Mode)     │
  │ - Reads CSV (Garage61, etc.)      │ ─── LAN ──▶ └──────────────┬────────────────┘
  │ - Emits at 10Hz cadence           │                            │
  └───────────────────────────────────┘                            │
                                                                   ▼ (Event Consumption)
                                        ┌───────────────────────────────┐
                                        │ PROCESSING LAYER (CONSUMER)   │
                                        │ Python Stream Writer          │ ◄── Batch Processing Line (Airflow DAG)
                                        └──────────────┬────────────────┘     [ Web REST API → MinIO S3 → dbt ]
                                                       │                                       │
                                                       ▼                                       ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│ STORAGE & ANALYTICAL MODELLING LAYER                                          │
│ PostgreSQL + TimescaleDB (TSDB Hypertables & OLAP Dimensional Star Schema)    │
│                                                                               │
│   telemetry_logs  ◄── Live / Replay stream data                               │
│   reference_laps  ◄── External CSV telemetry imports                          │
└─────────────────────────────┬─────────────────────────────────────────────────┘
                              │
                              ▼ (High Frequency Clean SQL Queries)
               ┌───────────────────────────────────────────────┐
               │ VISUALISATION LAYER — Grafana Dashboards      │
               │                                               │
               │  ┌───────────────────┐ ┌────────────────────┐ │
               │  │ Mode 1:           │ │ Mode 2:            │ │
               │  │ Lap Consistency   │ │ Reference Lap      │ │
               │  │ Analysis          │ │ Comparison         │ │
               │  └───────────────────┘ └────────────────────┘ │
               └───────────────────────────────────────────────┘

  CSV INGESTION PATH (On-demand, Machine B)
  ──────────────────────────────────────────
       [ Garage61 CSV / External Telemetry ]
                      │
                      ▼
        ┌──────────────────────────────┐
        │ tools/csv_importer.py        │
        │ - Parses & normalises CSV    │
        │ - Aligns to LapDistPct       │
        │ - Inserts into reference_laps│
        └──────────────────────────────┘
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
| Packaging | **PyInstaller** | Compiles the Python collector script into a standalone Windows `.exe` executable — no Python installation required on the host |
| Configuration | **config.ini** | External configuration file for runtime settings (Kafka broker IP, pipeline mode). Allows manual network setup without modifying source code |
| CSV Processing | **pandas / numpy** | Parsing, normalisation, and spatial interpolation of external CSV telemetry files for the reference lap comparison system |

---

## 6. Data Dictionary & KPIs

### A. Lap Consistency & Timing

| Variable | Type | Description |
|----------|------|-------------|
| `SessionTime` | Float (PK) | Absolute accumulated session time in seconds |
| `Lap` | Integer | Current lap number identifier |
| `LapLastLapTime` | Float | Chronometric time of the last completed lap |

**KPI — Consistency Index (per stint):**

$$\sigma_{\text{stint}} = \sqrt{\frac{1}{N-1} \sum_{i=1}^{N} (t_i - \bar{t})^2}$$

Where $t_i$ is the lap time of lap $i$ and $\bar{t}$ is the mean lap time across the stint. A lower $\sigma$ indicates higher consistency. This metric is tracked per stint to account for tyre degradation resets after pit stops.

### B. Fuel Management (Real-Time Analytics)

| Variable | Type | Description |
|----------|------|-------------|
| `FuelLevel` | Float | Volumetric fuel available in the tank (litres) |
| `Fuel_Burned_Last_Lap` | Computed | $\text{FuelLevel}_{\text{Lap } N-1} - \text{FuelLevel}_{\text{Lap } N}$ |
| **Projected Lap Autonomy** | KPI | $\frac{\text{FuelLevel}}{\text{Fuel\_Burned\_Last\_Lap}}$ |

**Enhanced KPI — Moving Average Fuel Prediction:**

$$\text{Avg Consumption} = \frac{1}{N} \sum_{i=0}^{N-1} \text{Fuel\_Burned}_{(\text{Lap } K-i)}$$

$$\text{Laps Remaining} = \frac{\text{FuelLevel}}{\text{Avg Consumption}}$$

$$\text{Fuel To Add In Pits} = (\text{Laps Target} - \text{Laps Remaining}) \times \text{Avg Consumption}$$

Where $N$ is the rolling window size (default: 3 laps). Using a moving average instead of a single-lap snapshot smooths out anomalies (safety car laps, off-track incidents) and produces a more stable and reliable pit-stop fuel strategy.

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

### D. Spatial Alignment & Reference Comparison

| Variable | Type | Description |
|----------|------|-------------|
| `LapDistPct` | Float | Percentage of the current lap distance completed (0.0 – 1.0). Provided natively by iRacing shared memory. Used as the primary alignment axis for all lap-overlay visualisations |

This variable replaces time as the X-axis when overlaying telemetry curves. Two laps with different absolute times can be compared metre-by-metre using `LapDistPct`, enabling both internal consistency analysis and external reference comparison.

---

## 7. Repository Structure

```text
iracing-telemetry-pipeline/
├── .gitignore
├── README.md                      # Portfolio-facing documentation with architecture diagrams
├── BLUEPRINT.md                   # This file — single source of truth for design & specs
├── docker-compose.yml             # Centralised deployment: Kafka, TimescaleDB, MinIO, Airflow, Grafana
├── collector/                     # Live Telemetry Capture (Native Windows Environment — LIVE only)
│   ├── requirements.txt           # Dependencies (pyirsdk, kafka-python)
│   ├── config.ini                 # External runtime configuration (Kafka IP, topic)
│   ├── main_collector.py          # Live collector with Downsampling, Micro-batch & Deadband
│   └── build_exe.bat              # PyInstaller build script for generating iRacingCollector.exe
├── replayer/                      # Telemetry Replay Component (Cross-Platform — CSV input)
│   ├── requirements.txt           # Dependencies (kafka-python, pandas)
│   ├── config.ini                 # Kafka connection & replay parameters (CSV path, sample rate)
│   ├── replay.py                  # CSV-based telemetry replayer with Micro-batch & Deadband
│   └── sample_data/               # Demo datasets for replay without simulator
│       └── sample_race_5laps.csv
├── tools/                         # Auxiliary Utilities
│   └── csv_importer.py            # CLI script to parse, normalise and ingest external CSV telemetry into reference_laps
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

-- Add spatial alignment column for lap overlay analysis
ALTER TABLE telemetry_logs ADD COLUMN IF NOT EXISTS lap_dist_pct NUMERIC;

-- Reference laps table for external CSV telemetry imports
CREATE TABLE IF NOT EXISTS reference_laps (
    reference_id   TEXT NOT NULL,          -- Unique identifier ("quali_best_spa", "pro_driver_monza")
    lap_dist_pct   NUMERIC NOT NULL,       -- Normalised lap distance (0.0 – 1.0)
    speed          NUMERIC,
    throttle       NUMERIC,
    brake          NUMERIC,
    steering_angle NUMERIC,
    fuel_level     NUMERIC,
    temp_lf        NUMERIC,
    temp_rf        NUMERIC,
    temp_lr        NUMERIC,
    temp_rr        NUMERIC,
    PRIMARY KEY (reference_id, lap_dist_pct)
);

-- Index for fast reference lap lookups
CREATE INDEX IF NOT EXISTS ix_reference_lap_id ON reference_laps (reference_id);
```

---

## 9. Implementation Plan

### Phase 1: Infrastructure & Hypertables (Docker & SQL)

1. Write `docker-compose.yml` orchestrating: **Kafka (KRaft mode — no Zookeeper)**, **TimescaleDB**, **MinIO**, and **Grafana**.
2. Configure internal Docker networks for secure inter-container communication.
3. Deploy the physical schema via `db/init.sql` (see Section 8 above).
4. Validate that all services start correctly with `docker-compose up` and basic health checks.

### Phase 2A: Live Collector (Windows Native)

Build the dedicated live capture component in `collector/main_collector.py`. This component **only** operates in live mode — it has a single responsibility: read iRacing shared memory and transmit to Kafka.

```python
import json
import time
import configparser
from kafka import KafkaProducer
import irsdk


def live_generator():
    ir = irsdk.IRSDK()
    ir.startup()
    prev_lap = 0
    frame_counter = 0
    while True:
        if ir.is_connected():
            frame_counter += 1
            if frame_counter % 6 != 0:  # Downsampling 60Hz → 10Hz
                time.sleep(1 / 60)
                continue
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
                'lap_dist_pct': ir['LapDistPct'],
            }
            # Event-Driven Deadband: immediate flush on lap change
            if current_lap > prev_lap:
                payload['lap_time'] = ir['LapLastLapTime']
                payload['lap_crossed'] = True
                prev_lap = current_lap
            yield payload
        time.sleep(1 / 60)
```

1. Implement an external `config.ini` configuration file for runtime parameters:
   - `bootstrap_server`: IP and port of the Kafka broker on the backend machine (e.g., `192.168.1.50:9092`).
   - `topic`: Kafka topic name.
2. Implement `KafkaBufferedProducer` class with micro-batching (buffer of 10 samples) and event-driven deadband (immediate flush on lap crossing).
3. Add `LapDistPct` to the captured telemetry payload for spatial alignment in downstream analysis.
4. Package the collector as a standalone Windows executable using **PyInstaller** (`--onefile` mode), bundling `pyirsdk`, `kafka-python`, and the default `config.ini`.
5. Validate that `iRacingCollector.exe` can produce messages to a remote Kafka broker across the local network.

### Phase 2B: Telemetry Replayer (Cross-Platform)

Build the dedicated replay component in `replayer/replay.py`. This component is **independent from the Collector** — it has no dependency on `pyirsdk` or Windows and can run on any machine (including the backend server itself).

```python
import json
import time
import csv
import configparser
from kafka import KafkaProducer


def csv_replay_generator(csv_path, sample_rate_hz=10):
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        prev_lap = 0
        for row in reader:
            payload = {
                'session_time': float(row['session_time']),
                'lap': int(row['lap']),
                'fuel': float(row['fuel']),
                'steering': float(row['steering']),
                'throttle': float(row['throttle']),
                'brake': float(row['brake']),
                'temp_lf': float(row['temp_lf']),
                'temp_rf': float(row['temp_rf']),
                'temp_lr': float(row['temp_lr']),
                'temp_rr': float(row['temp_rr']),
                'lap_dist_pct': float(row['lap_dist_pct']),
            }
            current_lap = payload['lap']
            if current_lap > prev_lap:
                payload['lap_time'] = float(row.get('lap_time', 0))
                payload['lap_crossed'] = True
                prev_lap = current_lap
            yield payload
            time.sleep(1 / sample_rate_hz)
```

1. Implement `config.ini` for Kafka connection and replay parameters (CSV file path, sample rate).
2. Reuse the same `KafkaBufferedProducer` pattern (micro-batching + deadband) to produce to the **same Kafka topic** as the Collector.
3. Accept CSV files as input — compatible with Garage61 exports and any standard telemetry CSV format.
4. Include `sample_data/sample_race_5laps.csv` as a bundled demo dataset.
5. Validate that the Replayer produces an indistinguishable stream from the Collector when consumed by the downstream pipeline.

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
3. **Analysis Mode 1 — Lap Consistency Dashboard**:
   - Line chart overlaying throttle, brake, and steering curves of multiple laps (filtered by `lap_number`) aligned on the `LapDistPct` axis.
   - Lap time bar chart with the stint mean displayed as a reference line.
   - Consistency Index gauge ($\sigma$ per stint) showing how stable the driver's pace is.
   - Degradation trend line plotting lap time evolution across a full stint to visualise tyre drop-off.
4. **Analysis Mode 2 — Reference Lap Comparison Dashboard**:
   - Dropdown selector to choose a `reference_id` from the `reference_laps` table.
   - Dual-line overlay comparing the driver's selected lap against the reference lap, aligned by `LapDistPct`.
   - Speed delta chart ($\Delta V = V_{\text{driver}} - V_{\text{reference}}$) highlighting braking zones where the driver loses or gains time.
   - Cumulative time delta curve showing progressive gain/loss across the lap.
5. **CSV Ingestion Tooling**:
   - Develop `tools/csv_importer.py` — a CLI utility that parses external CSV files (e.g., Garage61 exports), normalises column names, interpolates to a uniform `LapDistPct` grid using numpy, and bulk-inserts into the `reference_laps` table.
6. Connect Grafana to TimescaleDB/PostgreSQL as the official data source.

### Phase 5: Data Lifecycle & Retention Policies

1. Configure **TimescaleDB Continuous Aggregates** to automatically materialise downsampled summaries (e.g., per-minute averages) from the high-frequency 10 Hz telemetry data.
2. Implement **retention policies** using TimescaleDB's `add_retention_policy` to automatically drop raw 10 Hz chunks older than a configurable threshold (e.g., 30 days).
3. Configure an export job to archive aged raw data to **MinIO** in **Parquet** format before deletion, ensuring long-term cold storage availability.
4. Document the full data lifecycle: hot (real-time queries) → warm (continuous aggregates) → cold (Parquet in MinIO/S3).

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
| **Intermediate Stream Processor** | Introduce a lightweight Python stream processor (e.g., **Faust**) between the Kafka raw topic and the consumer. This layer would emit processed analytical events (e.g., `lap-completed`, `pit-stop-detected`) into secondary Kafka topics, decoupling raw ingestion from business-event generation. |
| **Auto-Discovery Configuration** | Replace the manual `config.ini` IP setup with an auto-discovery mechanism (e.g., mDNS/Bonjour or a simple broadcast UDP handshake) so the Windows collector automatically finds the backend server on the local network. |

---

## 11. Why This Project Stands Out

Unlike generic data analysis portfolios built on static Kaggle CSVs, this design demonstrates **Senior / Upper-Intermediate** competencies:

- **Real Streaming.** Processes asynchronous event flows produced by external simulation software — not flat files.
- **Modern Data Stack.** Combines Kafka for messaging, specialised time-series databases, and Grafana — emulating the telemetry infrastructure of real-world racing teams (Formula 1, WEC).
- **Business & Domain Focus.** Demonstrates the ability to transform raw accelerator and temperature data into actionable analytical metrics (mechanical fault detection and fuel strategy).
- **Software Engineering Applied to Data.** The decoupled Replay Mode is not just a convenience — it's an agnostic engine designed for **CI/CD**, enabling regression tests, data audits, and automated dbt tests on every commit without human interaction.
- **Cloud Cost Efficiency.** The coupling of downsampling and micro-batching patterns demonstrates engineering thinking oriented toward extreme reduction of operational I/O costs in production cloud storage infrastructure.
