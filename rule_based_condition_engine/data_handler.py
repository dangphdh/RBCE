import logging
from typing import Union, Dict, Any

try:
    import polars as pl
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
except ImportError:
    pl = None
    SparkSession = None
    SparkDataFrame = None

class DataHandler:
    def __init__(self, config: Dict[str, Any], engine: str = 'polars'):
        """
        Initialize DataHandler with configuration and engine type.
        
        :param config: Configuration dictionary
        :param engine: Processing engine ('polars' or 'pyspark')
        """
        self.config = config
        self.engine = engine
        self.logger = self._setup_logger()
        self.spark_session = None
        
        if self.engine == 'pyspark':
            self._initialize_spark_session()

    def _setup_logger(self) -> logging.Logger:
        """
        Set up logging for the DataHandler.
        
        :return: Configured logger
        """
        logger = logging.getLogger('DataHandler')
        logger.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
        return logger

    def _initialize_spark_session(self):
        """
        Initialize Spark session for PySpark engine.
        """
        if SparkSession is None:
            raise ImportError("PySpark is not installed. Cannot create Spark session.")
        
        self.spark_session = SparkSession.builder \
            .appName("RuleBasedClassificationEngine") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()
        
        self.logger.info("Spark session initialized")

    def load_dataset(self, dataset_config: Dict[str, Any]) -> Union[pl.DataFrame, SparkDataFrame]: # type: ignore
        """
        Load dataset based on configuration and engine type.
        
        :param dataset_config: Dataset configuration
        :return: Loaded dataset
        """
        self.logger.info(f"Loading dataset using {self.engine} engine")
        
        if self.engine == 'polars':
            return self._load_polars_dataset(dataset_config)
        else:
            return self._load_pyspark_dataset(dataset_config)

    def _load_polars_dataset(self, dataset_config: Dict[str, Any]) -> pl.DataFrame: # type: ignore
        """
        Load dataset using Polars.
        
        :param dataset_config: Dataset configuration
        :return: Polars DataFrame
        """
        if pl is None:
            raise ImportError("Polars is not installed.")
        
        tables = dataset_config.get('tables', [])
        dataframes = []
        
        for table in tables:
            path = table.get('path')
            name = table.get('name')
            
            if path.endswith('.parquet'):
                df = pl.read_parquet(path)
            elif path.endswith('.csv'):
                df = pl.read_csv(path)
            else:
                raise ValueError(f"Unsupported file format for {path}")
            
            df = df.with_columns(pl.lit('Unclassified').alias('classification'))
            dataframes.append(df)
        
        # Perform joins if multiple tables
        if len(dataframes) > 1:
            result = dataframes[0]
            for df in dataframes[1:]:
                result = result.join(df, on=dataset_config['joins'][0]['conditions'], how='left')
            return result
        
        return dataframes[0]

    def _load_pyspark_dataset(self, dataset_config: Dict[str, Any]) -> SparkDataFrame: # type: ignore
        """
        Load dataset using PySpark.
        
        :param dataset_config: Dataset configuration
        :return: PySpark DataFrame
        """
        if self.spark_session is None:
            raise RuntimeError("Spark session not initialized")
        
        tables = dataset_config.get('tables', [])
        dataframes = []
        
        for table in tables:
            path = table.get('path')
            name = table.get('name')
            
            if path.endswith('.parquet'):
                df = self.spark_session.read.parquet(path)
            elif path.endswith('.csv'):
                df = self.spark_session.read.csv(path, header=True, inferSchema=True)
            else:
                raise ValueError(f"Unsupported file format for {path}")
            
            df = df.withColumn('classification', F.lit('Unclassified'))
            dataframes.append(df)
        
        # Perform joins if multiple tables
        if len(dataframes) > 1:
            result = dataframes[0]
            for df in dataframes[1:]:
                join_conditions = [F.expr(cond) for cond in dataset_config['joins'][0]['conditions']]
                result = result.join(df, on=join_conditions, how='left')
            return result
        
        return dataframes[0]

    def close(self):
        """
        Close resources based on the engine type.
        """
        if self.engine == 'pyspark' and self.spark_session:
            self.spark_session.stop()
            self.logger.info("Spark session stopped")
