# Databricks notebook source
# sdf = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/FileStore/tables/maint/predictive_maintenance.csv')
# display(sdf)


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Any

import logging
from logging.handlers import RotatingFileHandler

def setup_logging():
    """Configure logging with rotation."""
    logger = logging.getLogger('MaintDemoLogger')
    logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG to capture all types of logs

    # Create a file handler which logs even debug messages
    fh = RotatingFileHandler('maint_demo.log', maxBytes=1024*1024*5, backupCount=5)
    fh.setLevel(logging.DEBUG)

    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.ERROR)  # Change to INFO or DEBUG as needed for console output

    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

logger = setup_logging()


## Class definition for Loading data

class DataLoader:
    """Responsible for loading data from a specified source."""
    
    def __init__(self, url: str):
        """Initialize the DataLoader with a data URL."""
        self.data_url = url

    def load_data(self) -> DataFrame:
        """Load data from the specified URL into a DataFrame."""
        try:
            #df = spark.read.format("delta").load(self.data_url)
            df = spark.read.format('csv').options(header='true', inferSchema='true').load(self.data_url)
            logger.info("Data loaded successfully.")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            raise


## Class definition for Processing data

class DataProcessor:
    """Handles data processing including cleaning and transformations."""
    
    def generate_silver(self, df: DataFrame) -> DataFrame:
        """Generate the silver layer by deduplicating and transforming the input DataFrame."""
        try:
            df = df.dropDuplicates()
            for col in df.columns:
                df = df.withColumnRenamed(col, col.replace(" ", "_").lower())

            cols_changes = {
                "orderdate": F.date_format(F.col("orderdate"), "yyyy-MM-dd"),
                "amount": F.mask(F.col("amt"))
            }
            df = df.withColumns(cols_changes)
            logger.info("Silver data processed successfully.")
            return df
        except Exception as e:
            logger.error(f"Error processing silver data: {e}", exc_info=True)
            raise

    def process_gold(self, df: DataFrame) -> DataFrame:
        """Aggregate the silver DataFrame to generate the gold layer, focusing on key metrics."""
        try:
            df = df.groupBy("product_id").agg(
                F.sum("target").alias("tgt")
            )
            logger.info("Gold data processed successfully.")
            return df
        except Exception as e:
            logger.error(f"Error processing gold data: {e}", exc_info=True)
            raise



# COMMAND ----------


def process_all(url):

    data_loader = DataLoader(url)
        
    data_processor = DataProcessor()

    try:
        bronze_df = data_loader.load_data()
        silver_df = data_processor.generate_silver(bronze_df)
        gold_df = data_processor.process_gold(silver_df)
        display(gold_df)
        display(silver_df)

        gold_df.write.mode('overwrite').saveAsTable("gold")
        logger.info("Data processing complete. Results saved.")
    except Exception as e:
        logger.error(f"An error occurred in the main processing block: {e}", exc_info=True)



process_all("dbfs:/FileStore/tables/maint/predictive_maintenance.csv")



# COMMAND ----------

import re
from pyaml_env import parse_config

parameter_value = 'L56802'
myparams = {"{{product_id}}": parameter_value}
config = parse_config('/dbfs/FileStore/config/config2.YML')
sql_query = config['query']

def custom_translation(text, translation):
    regex = re.compile('|'.join(map(re.escape, translation)))
    return regex.sub(lambda match: translation[match.group(0)], text)

def read_sql_from_yaml(query,params):
        sql_query = custom_translation(query, params)
        print(sql_query)
        return sql_query

def run_sql_in_databricks(sql_query):
    df = spark.sql(sql_query)
    df.show()

sql_from_config = read_sql_from_yaml(sql_query, myparams)
run_sql_in_databricks(sql_from_config)



# COMMAND ----------

import yaml
from pyaml_env import parse_config
config = parse_config('/dbfs/FileStore/config/versions.yml')
versions_list= ["0.1.0-3572c21", "0.1.1", "0.0.2"]
key_val = ['svc4']

for item, doc in config.items():
  for key in doc:
    if any([key[data] in versions_list for data in key]):
        print(key['name'])
        print(key['version'])

for item, doc in config.items():
    for key in doc:
        if key['name'] == 'svc4':
            print(key['revision'])

###### versons.yml######
# services:
#   - name: svc1
#     version: 0.1.0-3572c21
#   - name: svc2
#     revision: 0.0.1
#   - name: svc3
#     revision: 1.0.0
#   - name: svc4
#     revision: 2.0.1        

