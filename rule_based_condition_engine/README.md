# Rule-Based Condition Engine

## Configuration Management

### Configuration Sources

The `ConfigManager` now supports two configuration sources:

1. **YAML File Configuration (Traditional)**
   ```python
   config_manager = ConfigManager(config_path='config.yaml')
   ```

2. **Databricks Delta Table Configuration**
   ```python
   from pyspark.sql import SparkSession
   
   spark = SparkSession.builder.getOrCreate()
   config_manager = ConfigManager(config_source=spark, table_name='config_table')
   ```

#### Databricks Table Structure
The Databricks configuration table should have the following columns:
- `category`: Configuration category (e.g., 'datasets', 'scenarios')
- `key`: Configuration key
- `value`: Configuration value

### Usage Example

```python
# Load from YAML
yaml_config = ConfigManager(config_path='config.yaml')

# Or load from Databricks table
spark_config = ConfigManager(config_source=spark, table_name='my_config_table')

# Common methods work the same way
datasets = config_manager.get_datasets()
scenarios = config_manager.get_scenarios()
```

### Saving Configurations
- For YAML: Saves to the original file
- For Databricks: Overwrites the existing configuration table
