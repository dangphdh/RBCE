from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import polars as pl
class DataSet:
    def __init__(self, spark=None):
        """
        Initialize the DataSet class.
        :param spark: Optional SparkSession object. If provided, the class will use PySpark.
        """
        self.spark = spark
        self.dataframes = {}

    def add_table(self, name, df):
        """
        Add a table to the DataSet.
        :param name: Name of the table.
        :param df: DataFrame (Polars or PySpark).
        """
        # check if df is a DataFrame
        if not isinstance(df, (DataFrame, pl.DataFrame)) :
            df = spark.table(df)
        self.dataframes[name] = df

    def join_tables_pyspark(self, tables, table_aliases, join_conditions, join_type='inner'):
        """
        Join multiple tables based on the provided join conditions.
        :param tables: List of table names.
        :param table_aliases: List of table aliases.
        :param join_conditions: List of lists of join conditions as [left_value, operator, right_value] (e.g., [["t1.id", "==", "t2.id"], ["t1.name", "==", "t2.name"]]).
        :param join_type: Type of join (default is 'inner').
        :return: Joined DataFrame.
        """
        if len(tables) < 2 or len(join_conditions) != len(tables) - 1:
            raise ValueError("Number of tables must be at least 2, and number of join conditions must be equal to number of tables minus 1.")

        joined_df = self.dataframes[tables[0]].alias(table_aliases[0])

        for i in range(1, len(tables)):
            right_table = tables[i]
            right_alias = table_aliases[i]
            join_condition_list = join_conditions[i - 1]

            left_df = joined_df
            right_df = self.dataframes[right_table].alias(right_alias)

            if self.spark:
                # PySpark join
                # Parse the join conditions to use col function
                join_exprs = []
                for condition in join_condition_list:
                    left_value, operator, right_value = condition
                    if operator == "==":
                        join_exprs.append(col(left_value) == col(right_value))
                    elif operator == "!=":
                        join_exprs.append(col(left_value) != col(right_value))
                    elif operator == ">":
                        join_exprs.append(col(left_value) > col(right_value))
                    elif operator == "<":
                        join_exprs.append(col(left_value) < col(right_value))
                    elif operator == ">=":
                        join_exprs.append(col(left_value) >= col(right_value))
                    elif operator == "<=":
                        join_exprs.append(col(left_value) <= col(right_value))
                    else:
                        raise ValueError(f"Unsupported operator: {operator}")

                joined_df = left_df.join(right_df, join_exprs, join_type)
            else:
                # Polars join (not applicable here since we are using PySpark)
                raise NotImplementedError("Polars join is not implemented in this example.")

        return joined_df

    def join_tables_sql(self, tables, table_aliases, join_conditions, join_type='inner'):
        """
        Join multiple tables using SQL syntax.
        :param tables: List of table names.
        :param table_aliases: List of table aliases.
        :param join_conditions: List of lists of join conditions as [left_value, operator, right_value] (e.g., [["t1.id", "==", "t2.id"], ["t1.name", "==", "t2.name"]]).
        :param join_type: Type of join (default is 'inner').
        :return: Joined DataFrame.
        """
        if len(tables) < 2 or len(join_conditions) != len(tables) - 1:
            raise ValueError("Number of tables must be at least 2, and number of join conditions must be equal to number of tables minus 1.")

        # Register DataFrames as temporary views
        for table, alias in zip(tables, table_aliases):
            self.dataframes[table].createOrReplaceTempView(alias)

        # Build the SQL query
        sql_query = f"SELECT * FROM {table_aliases[0]}"
        for i in range(1, len(tables)):
            right_alias = table_aliases[i]
            join_condition_list = join_conditions[i - 1]

            # Convert join conditions to SQL syntax
            join_condition_sql = " AND ".join([f"{left_value} {operator} {right_value}" for left_value, operator, right_value in join_condition_list])
            sql_query += f" {join_type} JOIN {right_alias} ON {join_condition_sql}"

        # Execute the SQL query
        joined_df = self.spark.sql(sql_query)
        return joined_df
    
    def join_tables(self, tables, table_aliases, join_conditions, join_type='inner', option = 'pyspark'):
        """
        Join multiple tables based on the provided join conditions.
        :param tables: List of table names.
        :param table_aliases: List of table aliases.
        :param join_conditions: List of lists of join conditions as [left_value, operator, right_value] (e.g., [["t1.id", "==", "t2.id"], ["t1.name", "==", "t2.name"]]).
        :param join_type: Type of join (default is 'inner').
        :return: Joined DataFrame.
        """
        if option == 'pyspark':
            return self.join_tables_pyspark(tables, table_aliases, join_conditions, join_type)
        else:
            return self.join_tables_sql(tables, table_aliases, join_conditions, join_type)

    def select_columns(self, df, columns_with_alias):
        """
        Select columns from the DataFrame with alias names.
        :param df: DataFrame (Polars or PySpark).
        :param columns_with_alias: Dictionary of columns with alias names.
        :return: DataFrame with selected columns and aliases.
        """
        if self.spark:
            # PySpark select with alias
            selected_df = df.select([col(col_name).alias(alias) for col_name, alias in columns_with_alias.items()])
        else:
            # Polars select with alias (not applicable here since we are using PySpark)
            raise NotImplementedError("Polars select is not implemented in this example.")

        return selected_df

    def get_dataframe(self, config):
        """
        Get the final DataFrame after joining tables and selecting columns with aliases.
        :param config: Configuration dictionary.
        :return: Final DataFrame.
        """
        tables = config["tables"]
        table_aliases = config["table_aliases"]
        join_conditions = config["join_conditions"]
        join_type = config["join_type"]
        selected_columns = config["selected_columns"]
        columns_alias = config["columns_alias"]


        joined_df = self.join_tables(tables, table_aliases, join_conditions, join_type)
        final_df = self.select_columns(joined_df, columns_alias)
        return final_df

# Example usage
if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Example").getOrCreate()

    # Create DataSet instance
    dataset = DataSet(spark=spark)

    # Example PySpark DataFrames
    df1 = spark.createDataFrame([
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie")
    ], ["id", "name"])

    df2 = spark.createDataFrame([
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35)
    ], ["id", "name", "age"])

    df3 = spark.createDataFrame([
        (1, "New York"),
        (2, "Los Angeles"),
        (3, "Chicago")
    ], ["id", "city"])

    # Add tables to DataSet
    dataset.add_table("table1", df1)
    dataset.add_table("table2", df2)
    dataset.add_table("table3", df3)

    # Configuration dictionary
    config = {
        "tables": ["table1", "table2", "table3"],
        "table_aliases": ["t1", "t2", "t3"],
        "join_type": "inner",
        "join_conditions": [
            [["t1.id", "==", "t2.id"], ["t1.name", "==", "t2.name"]],
            [["t2.id", "==", "t3.id"]]
        ],
        "selected_columns": [
            "t1.id",
            "t1.name",
            "t2.age",
            "t3.city"
        ],
        "columns_alias": {
            "t1.id": "user_id",
            "t1.name": "user_name",
            "t2.age": "user_age",
            "t3.city": "user_city"
        }
    }

    # Get final DataFrame using the configuration
    final_df = dataset.get_dataframe(config)

    # Display the result
    final_df.show()