# ARCHITECTURE.md

## 1. System Architecture

### Data Ingestion

BankSim CSV / Synthetic Data → Python Replay Producer → Kafka Topic `transactions.raw`

### Stream Processing

Kafka → Spark Structured Streaming

Spark is responsible for:
- JSON parsing.
- Schema validation.
- Data cleaning.
- Duplicate detection.
- Event-time processing.
- Watermarking.
- Window aggregations.
- Customer behavior analytics.
- ML inference.

### Storage

Processed events → Parquet Data Lake.

Transaction predictions and aggregated insights → PostgreSQL.

Invalid events → Dead Letter Storage.

### Visualization

Streamlit Dashboard reads PostgreSQL and displays:
- Real-time transaction statistics.
- Fraud alerts.
- Customer Insights.
- Transaction history.
- Streaming performance metrics.

## 2. Machine Learning

ML models are trained offline.

Training pipeline:

BankSim → Preprocessing → Feature Engineering → Training → Evaluation → Model Artifact.

Streaming inference:

Spark → Feature Transformation → Loaded ML Model → Fraud Risk Score → PostgreSQL.

The true fraud label must not be used as a prediction feature.

## 3. Event-Time Semantics

- `step`: original BankSim simulation step.
- `event_time`: derived simulation timestamp.
- `ingestion_time`: timestamp when an event enters the streaming system.
- `processing_time`: timestamp when processing results are generated.

The conversion from `step` to `event_time` must be deterministic and documented.

## 4. Customer Insights

Customer Insights may include:
- Transaction count by customer.
- Total spending.
- Average transaction amount.
- Category distribution.
- Transaction frequency.
- Changes relative to historical behavior.
- Suspicious transaction patterns.

## 5. Recovery

Spark must use checkpointing.

Kafka retains messages according to its configured retention policy.

Database writes must be designed to avoid duplicate results after retries.

## 6. Airflow

Airflow orchestrates:
- Dataset validation.
- Data preparation.
- Model retraining.
- Replay experiments.
- Experiment reporting.

Airflow does not replace Kafka or Spark.

## 7. Architecture Constraints

- Streaming results must originate from Spark.
- Dashboard must not fabricate transaction metrics.
- The application must work without the `ai/` directory.
- Production code must not depend on AI documentation.