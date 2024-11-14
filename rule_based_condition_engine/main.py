import logging
import yaml
from typing import Dict, Any

from config_manager import ConfigManager
from data_handler import DataHandler
from rule_processor import RuleProcessor

class DataClassificationEngine:
    def __init__(self, config_path: str = 'config.yaml'):
        """
        Initialize Data Classification Engine.
        
        :param config_path: Path to configuration file
        """
        self.logger = self._setup_logger()
        self.config_manager = ConfigManager(config_path)
        self.config = self.config_manager.load_config()
        
        # Determine processing engine
        self.engine = self.config.get('engine', {}).get('default', 'polars')
        self.fallback_engine = self.config.get('engine', {}).get('fallback', 'pyspark')

    def _setup_logger(self) -> logging.Logger:
        """
        Set up logging for the Data Classification Engine.
        
        :return: Configured logger
        """
        logger = logging.getLogger('DataClassificationEngine')
        logger.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        return logger

    def process_scenarios(self):
        """
        Process all scenarios defined in the configuration.
        """
        scenarios = self.config.get('scenarios', [])
        
        for scenario in scenarios:
            try:
                self.process_scenario(scenario)
            except Exception as e:
                self.logger.error(f"Error processing scenario {scenario.get('name', 'Unknown')}: {e}")

    def process_scenario(self, scenario: Dict[str, Any]):
        """
        Process a single scenario.
        
        :param scenario: Scenario configuration
        """
        self.logger.info(f"Processing scenario: {scenario.get('name', 'Unnamed')}")
        
        # Get dataset configuration
        dataset_name = scenario.get('dataset')
        dataset_config = self.config['datasets'].get(dataset_name)
        
        if not dataset_config:
            raise ValueError(f"Dataset configuration not found for {dataset_name}")
        
        # Initialize data handler and rule processor
        try:
            data_handler = DataHandler(self.config, engine=self.engine)
            rule_processor = RuleProcessor(self.config, engine=self.engine)
            
            # Load dataset
            dataset = data_handler.load_dataset(dataset_config)
            
            # Process rules
            classified_dataset = rule_processor.process_rules(dataset, [scenario])
            
            # Export results
            output_config = scenario.get('output', {})
            rule_processor.export_results(
                classified_dataset, 
                output_config.get('path'), 
                output_config.get('format', 'parquet')
            )
            
        except Exception as e:
            self.logger.warning(f"Failed with {self.engine} engine. Attempting fallback to {self.fallback_engine}")
            
            # Fallback to alternative engine
            data_handler = DataHandler(self.config, engine=self.fallback_engine)
            rule_processor = RuleProcessor(self.config, engine=self.fallback_engine)
            
            # Repeat processing with fallback engine
            dataset = data_handler.load_dataset(dataset_config)
            classified_dataset = rule_processor.process_rules(dataset, [scenario])
            
            output_config = scenario.get('output', {})
            rule_processor.export_results(
                classified_dataset, 
                output_config.get('path'), 
                output_config.get('format', 'parquet')
            )
        
        finally:
            # Close resources
            data_handler.close()

def main():
    """
    Main entry point for the Data Classification Engine.
    """
    engine = DataClassificationEngine()
    engine.process_scenarios()

if __name__ == "__main__":
    main()
