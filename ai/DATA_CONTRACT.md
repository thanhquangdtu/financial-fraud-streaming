# DATA_CONTRACT.md

## 1. Dataset

Name: BankSim.

Original schema:

| Field | Type | Description |
|---|---|---|
| step | integer | Simulation time step |
| customer | string | Customer identifier |
| age | string | Encoded age group |
| gender | string | Encoded gender |
| zipcodeOri | string | Customer postal code |
| merchant | string | Merchant identifier |
| zipMerchant | string | Merchant postal code |
| category | string | Transaction category |
| amount | double | Transaction amount |
| fraud | integer | Ground-truth fraud label |

## 2. Normalized Event Schema

| Field | Type |
|---|---|
| transaction_id | string |
| customer_id | string |
| merchant_id | string |
| step | integer |
| event_time | timestamp |
| ingestion_time | timestamp |
| amount | double |
| category | string |
| age_group | string |
| gender | string |
| fraud_label | integer, nullable |
| data_source | string |

## 3. Data Rules

- Preserve the original dataset.
- Remove surrounding quote characters from categorical fields when needed.
- Treat customer and merchant identifiers as strings.
- Treat postal codes as strings.
- Validate numeric values.
- Reject negative transaction amounts unless explicitly supported by the dataset contract.
- Validate fraud labels as 0 or 1.
- Generate stable transaction IDs from the source dataset identity and original row number.
- Keep the same transaction ID when replaying the same event.
- Do not use `fraud_label` as an ML input feature.

## 4. Event Time

The original `step` field represents simulated time.

For reproducible experiments:

`event_time = simulation_start + step × simulation_step_duration`

The simulation start time and step duration must be configurable and recorded.

`ingestion_time` must be assigned when events are ingested, not during offline preprocessing.

## 5. Dataset Splitting

Use chronological splitting based on `step`.

Train, validation and test sets must be separated.

The final split boundaries must be determined after inspecting the complete dataset.

## 6. Synthetic Data

Synthetic transactions must include:

- Stable customer identifiers.
- Consistent behavioral profiles.
- Repeated transactions per customer.
- Controlled anomaly scenarios.
- Explicit `data_source = synthetic`.

Synthetic test scenarios must not be silently mixed into the original model evaluation.

## 7. Data Quality Checks

Validate:
- Required columns.
- Data types.
- Null values.
- Duplicate transaction IDs.
- Amount ranges.
- Fraud label distribution.
- Time-step ordering.
- Customer transaction counts.

## 8. Schema Versioning

Schema version: 1.0.

Any incompatible change requires updating this document and the relevant tests.