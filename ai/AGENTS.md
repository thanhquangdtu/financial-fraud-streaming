# AGENTS.md — AI Development Rules

## 1. Project Identity

**Project:** Real-time Financial Fraud Detection and Customer Behavior Analytics using Apache Spark Structured Streaming.

**Duration:** 8 weeks.

**Team size:** 2 students.

**Primary focus:** Data Engineering.

**Secondary focus:** Machine Learning and Analytics.

**Dataset:** BankSim.

## 2. Project Architecture

The system must follow this architecture:

BankSim CSV → Python Producer → Apache Kafka → Apache Spark Structured Streaming → ML Fraud Detection + Customer Insights → PostgreSQL / Parquet → Streamlit Dashboard.

Apache Airflow will be added in Phase 2 to orchestrate batch workflows, model training, and experiments.

## 3. Mandatory Development Rules

1. Always read the relevant documentation inside `ai/` before implementing a task.
2. Follow the architecture and approved technology stack.
3. Implement only the current week's tasks.
4. Never replace Kafka or Spark with frontend simulations.
5. Preserve the original BankSim dataset.
6. Keep real BankSim records separate from synthetic test records.
7. Never use the `fraud` label as an ML inference feature.
8. Prevent data leakage between training, validation, and testing.
9. Use event-time processing for Spark window aggregations.
10. Use checkpointing and idempotent database writes where required.
11. Write tests for implemented features.
12. Do not introduce Airflow until Phase 1 is stable.
13. Do not modify unrelated modules without explaining why.
14. Never claim a feature works without verification.
15. Update `ai/TASKS.md` and `ai/PROJECT_STATUS.md` after completing tasks.

## 4. Required Workflow

### Before coding
- Read `ai/PROJECT_CONTEXT.md`.
- Read `ai/ARCHITECTURE.md`.
- Read `ai/DATA_CONTRACT.md`.
- Check `ai/TASKS.md`.
- Check `ai/PROJECT_STATUS.md`.
- Identify the smallest unfinished task.
- Explain which files need to be created or modified.

### During coding
- Implement one task at a time.
- Keep modules separated by responsibility.
- Use configuration files and environment variables.
- Add error handling and logging.
- Avoid unnecessary dependencies.

### After coding
- Run relevant tests.
- Report test results.
- Identify remaining issues.
- Update project status.
- Suggest the next task.

## 5. Restrictions

Do not:
- Change the approved technology stack without permission.
- Create fake dashboard metrics and present them as real measurements.
- Train ML models using test data.
- Mix synthetic and original datasets without marking their sources.
- Hardcode credentials.
- Commit large datasets or model artifacts unnecessarily.
- Delete project files without permission.
- Implement Phase 2 prematurely.

## 6. Definition of Done

A task is completed only when:
- Code is implemented.
- Relevant tests pass.
- Integration behavior is verified when applicable.
- Documentation is updated.
- Remaining limitations are recorded.

## 7. AI Documentation Isolation

All AI development instructions must remain inside `ai/`.

The production application must not import or depend on files inside `ai/`.

Removing `ai/` after development must not affect the application runtime.