# PROJECT_CONTEXT.md

## Project Title

Real-time Financial Transaction Processing, Fraud Detection and Customer Behavior Analytics using Apache Spark Structured Streaming.

## Background

Financial transaction systems generate continuous streams of data. Traditional batch processing is not sufficient for use cases requiring timely monitoring, anomaly detection, and continuously updated analytics.

This project demonstrates a streaming-oriented Big Data architecture using historical financial transaction data replayed as a live event stream.

## Objectives

1. Build a real Kafka-based transaction ingestion pipeline.
2. Process transaction events using Spark Structured Streaming.
3. Perform event-time window aggregations.
4. Detect suspicious transactions using Machine Learning.
5. Generate customer-specific behavioral insights.
6. Store processed transactions and analytics.
7. Visualize streaming results through a dashboard.
8. Evaluate throughput, latency, and fault recovery.
9. Automate selected workflows using Apache Airflow.

## Dataset

Primary dataset: BankSim.

Original columns:

`step, customer, age, gender, zipcodeOri, merchant, zipMerchant, category, amount, fraud`

BankSim is simulated financial transaction data.

The project will also generate synthetic test scenarios for customer behavior analysis.

## Project Scope

### Phase 1 — Weeks 1–6

Build and validate the core streaming architecture.

### Phase 2 — Weeks 7–8

Integrate Apache Airflow, automate experiments, and finalize evaluation.

## Expected Deliverables

- Reproducible Docker-based environment.
- Data preprocessing pipeline.
- Kafka transaction producer.
- Spark Structured Streaming application.
- Fraud detection ML model.
- Customer Insights dashboard.
- PostgreSQL and Parquet storage.
- Automated tests.
- Performance experiment results.
- Airflow DAGs.
- Final technical report and demo.

## Project Limitations

- Historical transactions are replayed to simulate streaming.
- This is not a real banking production system.
- Model predictions are research outputs, not definitive fraud judgments.
- Synthetic customer scenarios must be identified separately.
- Performance claims must be supported by actual measurements.