import logging
import re
from typing import Union, List, Dict, Any

try:
    import polars as pl
    import pyspark.sql.functions as F
    from pyspark.sql import DataFrame as SparkDataFrame
except ImportError:
    pl = None
    SparkDataFrame = None

class RuleProcessor:
    def __init__(self, config: Dict[str, Any], engine: str = 'polars'):
        """
        Initialize RuleProcessor with configuration and engine type.
        
        :param config: Configuration dictionary
        :param engine: Processing engine ('polars' or 'pyspark')
        """
        self.config = config
        self.engine = engine
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """
        Set up logging for the RuleProcessor.
        
        :return: Configured logger
        """
        logger = logging.getLogger('RuleProcessor')
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        return logger

    def process_rules(self, dataset: Union[pl.DataFrame, SparkDataFrame], # type: ignore
                      scenarios: List[Dict[str, Any]]) -> Union[pl.DataFrame, SparkDataFrame]: # type: ignore
        """
        Process rules across different scenarios.
        
        :param dataset: Input dataset (Polars or PySpark DataFrame)
        :param scenarios: List of scenario configurations
        :return: Classified dataset
        """
        self.logger.info(f"Processing rules using {self.engine} engine")
        
        # Validate engine compatibility
        if self.engine == 'polars' and pl is None:
            raise ImportError("Polars is not installed. Please install it to use Polars engine.")
        elif self.engine == 'pyspark' and SparkDataFrame is None:
            raise ImportError("PySpark is not installed. Please install it to use PySpark engine.")
        
        # Process rules for each scenario
        classified_results = []
        for scenario in scenarios:
            try:
                classified_scenario = self._process_scenario_rules(dataset, scenario)
                classified_results.append(classified_scenario)
            except Exception as e:
                self.logger.error(f"Error processing scenario {scenario.get('name', 'Unknown')}: {e}")
        
        # Combine results based on engine
        if not classified_results:
            return self._create_empty_result(dataset)
        
        return self._combine_results(classified_results)

    def _process_scenario_rules(self, dataset: Union[pl.DataFrame, SparkDataFrame], # type: ignore
                                 scenario: Dict[str, Any]) -> Union[pl.DataFrame, SparkDataFrame]: # type: ignore
        """
        Process rules for a specific scenario.
        
        :param dataset: Input dataset
        :param scenario: Scenario configuration
        :return: Classified dataset for the scenario
        """
        rules = scenario.get('rules', [])
        sorted_rules = sorted(rules, key=lambda r: r.get('priority', 0), reverse=True)
        
        for rule in sorted_rules:
            try:
                dataset = self._apply_rule(dataset, rule)
            except Exception as e:
                self.logger.warning(f"Rule {rule.get('name', 'Unknown')} failed: {e}")
        
        return dataset

    def _apply_rule(self, dataset: Union[pl.DataFrame, SparkDataFrame], # type: ignore
                    rule: Dict[str, Any]) -> Union[pl.DataFrame, SparkDataFrame]: # type: ignore
        """
        Apply a single rule to the dataset.
        
        :param dataset: Input dataset
        :param rule: Rule configuration
        :return: Filtered dataset
        """
        condition = rule.get('condition', '')
        classification = rule.get('classification', 'Unclassified')
        
        if self.engine == 'polars':
            return self._apply_polars_rule(dataset, condition, classification)
        else:
            return self._apply_pyspark_rule(dataset, condition, classification)

    def _apply_polars_rule(self, dataset: pl.DataFrame, # type: ignore
                            condition: str, 
                            classification: str) -> pl.DataFrame: # type: ignore
        """
        Apply rule using Polars DataFrame.
        
        :param dataset: Polars DataFrame
        :param condition: Rule condition
        :param classification: Classification label
        :return: Classified Polars DataFrame
        """
        # Lazy evaluation for performance
        expr = self._parse_condition(condition, engine='polars')
        return dataset.with_columns(
            pl.when(expr).then(pl.lit(classification)).otherwise(pl.col('classification')).alias('classification')
        )

    def _apply_pyspark_rule(self, dataset: SparkDataFrame, # type: ignore
                             condition: str, 
                             classification: str) -> SparkDataFrame: # type: ignore
        """
        Apply rule using PySpark DataFrame.
        
        :param dataset: PySpark DataFrame
        :param condition: Rule condition
        :param classification: Classification label
        :return: Classified PySpark DataFrame
        """
        expr = self._parse_condition(condition, engine='pyspark')
        return dataset.withColumn(
            'classification', 
            F.when(expr, classification).otherwise(F.col('classification'))
        )

    def _parse_condition(self, condition: str, engine: str = 'polars') -> Any:
        """
        Parse complex condition with support for multiple operators.
        
        :param condition: Condition string
        :param engine: Processing engine
        :return: Parsed condition expression
        """
        # Implement advanced condition parsing with multiple operators
        # This is a simplified version and should be expanded
        if engine == 'polars':
            import polars as pl
            return pl.col('column_name').str.contains(condition)
        else:
            from pyspark.sql.functions import col, expr
            return expr(condition)

    def _create_empty_result(self, dataset: Union[pl.DataFrame, SparkDataFrame]) -> Union[pl.DataFrame, SparkDataFrame]: # type: ignore
        """
        Create an empty result dataset with classification column.
        
        :param dataset: Original dataset
        :return: Empty dataset with classification column
        """
        if self.engine == 'polars':
            return dataset.with_columns(pl.lit('Unclassified').alias('classification'))
        else:
            return dataset.withColumn('classification', F.lit('Unclassified'))

    def _combine_results(self, results: List[Union[pl.DataFrame, SparkDataFrame]]) -> Union[pl.DataFrame, SparkDataFrame]: # type: ignore
        """
        Combine classification results.
        
        :param results: List of classified datasets
        :return: Combined dataset
        """
        if self.engine == 'polars':
            return pl.concat(results)
        else:
            from functools import reduce
            return reduce(lambda a, b: a.union(b), results)

    def export_results(self, dataset: Union[pl.DataFrame, SparkDataFrame], # type: ignore
                       output_path: str, 
                       format: str = 'parquet'):
        """
        Export classified results.
        
        :param dataset: Classified dataset
        :param output_path: Path to export results
        :param format: Output file format
        """
        self.logger.info(f"Exporting results to {output_path} in {format} format")
        
        if self.engine == 'polars':
            dataset.write_parquet(output_path) if format == 'parquet' else dataset.write_csv(output_path)
        else:
            dataset.write.mode('overwrite').format(format).save(output_path)

        self.logger.info("Results exported successfully")
