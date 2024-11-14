# Databricks ETL Project

## Overview
This project provides a robust framework for running ETL (Extract, Transform, Load) jobs on Databricks, with a focus on modularity, configurability, and scalability.

## Project Structure
```
databricks_etl_project/
│
├── src/
│   └── jobs/
│       └── customer_etl.py      # Sample ETL job
│
├── tests/                       # Unit and integration tests
│
├── config/
│   └── etl_config.yaml          # Configuration management
│
├── notebooks/                   # Databricks notebook examples
│
├── requirements.txt              # Project dependencies
└── README.md                    # Project documentation
```

## Prerequisites
- Python 3.8+
- Databricks Connect
- Databricks Workspace Access

## Installation
1. Clone the repository
2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

## Configuration
Modify `config/etl_config.yaml` to set:
- Input/output paths
- Databricks cluster details
- Environment-specific configurations

## Running ETL Jobs
```bash
# Run customer ETL job
python src/jobs/customer_etl.py
```

## Testing
```bash
# Run tests
pytest tests/
```

## Deployment
1. Configure Databricks Connect
2. Use Databricks CLI or UI to upload and schedule jobs

## Key Components
- PySpark for distributed data processing
- Delta Lake for reliable data storage
- Modular job design
- Environment-specific configurations

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push and create a pull request
