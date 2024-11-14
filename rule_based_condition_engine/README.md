# Rule-Based Condition Engine (RBCE)

## Overview

The Rule-Based Condition Engine (RBCE) is a flexible data processing framework that allows dynamic rule-based classification and transformation of datasets. It supports multiple processing engines (Polars and PySpark) with automatic fallback mechanisms.

## Key Features

- Multi-engine support (Polars and PySpark)
- Dynamic rule processing
- Configurable scenarios and rules
- Automatic engine fallback
- Comprehensive logging
- Flexible data handling

## Prerequisites

- Python 3.8+
- Polars
- PySpark (optional)
- PyYAML

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/rule-based-condition-engine.git
cd rule-based-condition-engine
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Quick Start

1. Configure your scenarios in `config.yaml`
2. Run the engine:
```bash
python main.py
```

## Configuration

The engine uses a YAML configuration file to define:
- Datasets
- Scenarios
- Classification rules
- Processing engines

See `CONFIGURATION.md` for detailed configuration instructions.

## Usage Example

```python
from main import DataClassificationEngine

# Initialize and run the engine
engine = DataClassificationEngine('config.yaml')
engine.process_scenarios()
```

## Documentation

- [Architecture Overview](ARCHITECTURE.md)
- [Configuration Guide](CONFIGURATION.md)
- [Contributing Guidelines](CONTRIBUTING.md)

## License

[Specify your license]
