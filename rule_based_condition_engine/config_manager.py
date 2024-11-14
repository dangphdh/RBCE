import yaml

class ConfigManager:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self.load_config()

    def load_config(self) -> dict:
        """
        Load the configuration from the YAML file.
        
        :return: Dictionary containing the configuration
        """
        with open(self.config_path, 'r') as config_file:
            return yaml.safe_load(config_file)

    def get_datasets(self) -> list:
        """
        Get the list of datasets from the configuration.
        
        :return: List of datasets
        """
        return list(self.config['datasets'].keys())

    def get_scenarios(self) -> list:
        """
        Get the list of scenarios from the configuration.
        
        :return: List of scenarios
        """
        return self.config['scenarios']

    def get_rules(self) -> list:
        """
        Get the list of all rules from all scenarios.
        
        :return: List of rules
        """
        rules = []
        for scenario in self.config['scenarios']:
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
        for scenario in self.config['scenarios']:
            for condition in scenario.get('conditions', []):
                condition['scenario'] = scenario['name']
                conditions.append(condition)
        return conditions

    def save_config(self):
        """
        Save the current configuration to the YAML file.
        """
        with open(self.config_path, 'w') as config_file:
            yaml.dump(self.config, config_file, default_flow_style=False)

    def add_dataset(self, dataset_name: str, dataset_config: dict):
        """
        Add a new dataset to the configuration.
        
        :param dataset_name: Name of the dataset
        :param dataset_config: Configuration for the dataset
        """
        self.config['datasets'][dataset_name] = dataset_config
        self.save_config()

    def add_scenario(self, scenario_config: dict):
        """
        Add a new scenario to the configuration.
        
        :param scenario_config: Configuration for the scenario
        """
        self.config['scenarios'].append(scenario_config)
        self.save_config()

    def update_scenario(self, scenario_name: str, scenario_config: dict):
        """
        Update an existing scenario in the configuration.
        
        :param scenario_name: Name of the scenario to update
        :param scenario_config: New configuration for the scenario
        """
        for i, scenario in enumerate(self.config['scenarios']):
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
        self.config['scenarios'] = [s for s in self.config['scenarios'] if s['name'] != scenario_name]
        self.save_config()

    # Similar methods can be added for updating and deleting datasets, rules, and conditions