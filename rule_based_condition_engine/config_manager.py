import yaml
from typing import Union, Optional
from pyspark.sql import SparkSession
from delta import *

class ConfigManager:
    def __init__(self, config_source: Union[str, SparkSession] = None, 
                 config_path: Optional[str] = None, 
                 table_name: Optional[str] = None):
        """
        Initialize ConfigManager with either a YAML file or a Databricks table.
        
        :param config_source: Either a file path or a SparkSession
        :param config_path: Path to YAML config file (for traditional loading)
        :param table_name: Name of the Databricks delta table to load config from
        """
        self.config = None
        
        # YAML file configuration (existing method)
        if config_path:
            self.config_path = config_path
            self.config = self.load_config_from_yaml()
        
        # Databricks table configuration
        elif isinstance(config_source, SparkSession) and table_name:
            self.spark = config_source
            self.table_name = table_name
            self.config = self.load_config_from_databricks()
        
        # Default fallback to existing config.yaml
        elif config_path is None:
            self.config_path = 'config.yaml'
            self.config = self.load_config_from_yaml()
        
        else:
            raise ValueError("Invalid configuration source. Provide either config_path or (config_source and table_name).")

    def load_config_from_yaml(self) -> dict:
        """
        Load the configuration from the YAML file.
        
        :return: Dictionary containing the configuration
        """
        try:
            with open(self.config_path, 'r') as config_file:
                return yaml.safe_load(config_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")

    def load_config_from_databricks(self) -> dict:
        """
        Load configuration from a Databricks Delta table.
        
        :return: Dictionary containing the configuration
        """
        try:
            # Read the entire table as a DataFrame
            config_df = self.spark.table(self.table_name)
            
            # Convert DataFrame to dictionary
            config_data = {}
            
            # Assuming the table has columns: category, key, value
            for row in config_df.collect():
                category = row['category']
                key = row['key']
                value = row['value']
                
                if category not in config_data:
                    config_data[category] = {}
                
                config_data[category][key] = value
            
            return config_data
        
        except Exception as e:
            raise ValueError(f"Error loading configuration from Databricks table {self.table_name}: {e}")

    def get_datasets(self) -> list:
        """
        Get the list of datasets from the configuration.
        
        :return: List of datasets
        """
        return list(self.config.get('datasets', {}).keys())

    def get_scenarios(self) -> list:
        """
        Get the list of scenarios from the configuration.
        
        :return: List of scenarios
        """
        return self.config.get('scenarios', [])

    def get_rules(self) -> list:
        """
        Get the list of all rules from all scenarios.
        
        :return: List of rules
        """
        rules = []
        for scenario in self.config.get('scenarios', []):
            for rule in scenario.get('rules', []):
                rule['scenario'] = scenario['name']
                rules.append(rule)
        return rules

    def get_conditions(self) -> list:
        """
        Get the list of all conditions from all scenarios.
        
        :return: List of conditions
        """
        conditions = []
        for scenario in self.config.get('scenarios', []):
            for condition in scenario.get('conditions', []):
                condition['scenario'] = scenario['name']
                conditions.append(condition)
        return conditions

    def save_config(self):
        """
        Save the current configuration. 
        For YAML, save to file. For Databricks, implement table update logic.
        """
        if hasattr(self, 'config_path'):
            # YAML file save
            with open(self.config_path, 'w') as config_file:
                yaml.dump(self.config, config_file, default_flow_style=False)
        elif hasattr(self, 'spark') and hasattr(self, 'table_name'):
            # Databricks table save (simplified example)
            config_rows = []
            for category, items in self.config.items():
                for key, value in items.items():
                    config_rows.append((category, key, value))
            
            config_df = self.spark.createDataFrame(config_rows, ['category', 'key', 'value'])
            config_df.write.format('delta').mode('overwrite').saveAsTable(self.table_name)
        else:
            raise ValueError("Cannot save configuration: no valid save method found")

    def add_dataset(self, dataset_name: str, dataset_config: dict):
        """
        Add a new dataset to the configuration.
        
        :param dataset_name: Name of the dataset
        :param dataset_config: Configuration for the dataset
        """
        if 'datasets' not in self.config:
            self.config['datasets'] = {}
        self.config['datasets'][dataset_name] = dataset_config
        self.save_config()

    def add_scenario(self, scenario_config: dict):
        """
        Add a new scenario to the configuration.
        
        :param scenario_config: Configuration for the scenario
        """
        if 'scenarios' not in self.config:
            self.config['scenarios'] = []
        self.config['scenarios'].append(scenario_config)
        self.save_config()

    def update_scenario(self, scenario_name: str, scenario_config: dict):
        """
        Update an existing scenario in the configuration.
        
        :param scenario_name: Name of the scenario to update
        :param scenario_config: New configuration for the scenario
        """
        for i, scenario in enumerate(self.config.get('scenarios', [])):
            if scenario['name'] == scenario_name:
                self.config['scenarios'][i] = scenario_config
                self.save_config()
                return
        raise ValueError(f"Scenario '{scenario_name}' not found.")

    def delete_scenario(self, scenario_name: str):
        """
        Delete a scenario from the configuration.
        
        :param scenario_name: Name of the scenario to delete
        """
        self.config['scenarios'] = [s for s in self.config.get('scenarios', []) if s['name'] != scenario_name]
        self.save_config()
