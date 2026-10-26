# ROADMAP.md

## Phase 1 — Core Streaming System

### Week 1: Dataset and Schema
- Inspect BankSim.
- Clean and normalize data.
- Design transaction schema.
- Generate synthetic customer scenarios.
- Split train/validation/test.
- Validate data quality.

### Week 2: Kafka Producer
- Configure Kafka.
- Build CSV replay producer.
- Support configurable replay speed.
- Implement pause, resume and restart.
- Verify message delivery.

### Week 3: Spark Structured Streaming
- Consume Kafka messages.
- Parse and validate schema.
- Implement event-time windows.
- Implement watermarking.
- Compute streaming aggregations.

### Week 4: Storage and Recovery
- Store events in Parquet.
- Store predictions and insights in PostgreSQL.
- Configure checkpoints.
- Test restart and recovery.
- Prevent duplicate database writes.

### Week 5: Machine Learning
- Engineer transaction features.
- Train fraud detection model.
- Evaluate precision, recall, F1 and PR-AUC.
- Integrate inference with Spark.
- Validate model output.

### Week 6: Dashboard and Benchmark
- Build Streamlit dashboard.
- Display real-time transactions.
- Display fraud alerts.
- Display customer-specific insights.
- Measure throughput and latency.
- Test system under different loads.

## Phase 2 — Automation and Evaluation

### Week 7: Apache Airflow
- Configure Airflow.
- Build data preparation DAG.
- Build model training DAG.
- Build replay experiment DAG.
- Verify orchestration.

### Week 8: Final Evaluation
- Run benchmark scenarios.
- Test fault recovery.
- Collect experimental results.
- Complete technical documentation.
- Prepare final report and demonstration.

## Completion Policy

Do not advance to the next phase before the required core functionality has been verified.