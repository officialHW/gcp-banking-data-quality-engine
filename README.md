# Banking Data Quality Engine

A production-style portfolio project for validating banking transaction data before it reaches analytical systems.

## Why this project

Financial data pipelines fail quietly when duplicate, malformed, incomplete or out-of-domain records enter downstream systems. This project demonstrates a testable quality layer that can run locally and be adapted to Apache Beam/Dataflow.

## Architecture

`Source transactions → validation rules → quality report → trusted / rejected records`

The validation layer checks:
- required identifiers
- positive transaction amounts
- supported currencies
- supported transaction statuses
- overall data-quality rate

## Stack

- Python
- Apache Beam (extension point for Dataflow)
- PyTest
- GCP-oriented architecture
- Terraform/Airflow integration planned as infrastructure extensions

## Run locally

```bash
python -m pytest -q
```

## Engineering decisions

The core validation functions are pure Python so they can be unit-tested independently and reused inside distributed Beam transforms. The project separates validation logic from orchestration, making the quality rules easier to maintain as the dataset grows.

## Portfolio relevance

This project demonstrates Python, automated testing, data-quality engineering, pipeline design and cloud-ready thinking rather than only notebook-based analysis.
