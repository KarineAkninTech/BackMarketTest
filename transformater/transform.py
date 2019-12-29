import logging
from logging.handlers import RotatingFileHandler
import sys
import datetime
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

logger = logging.getLogger('pyspark')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler("transform.log", mode="a", maxBytes=1000000000, backupCount=0, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def generate_paths(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir):
    """
    Generate paths to interact with the datalake folder with a given source and the input file product_catalog.csv.

    :param source: the source name, String
    :param ingress_dir: the path to ingress folder, String
    :param parquet_dir: the path to parquet folder, String
    :param valid_dir: the path to valid folder, String
    :param invalid_dir: the path to invalid folder, String
    :param archive_dir: the path to archive folder, String
    :return: csv_path, parquet_path, valid_path, invalid_path, archive_path, String
    """
    csv_path = ingress_dir + "product_catalog.csv"
    parquet_path = parquet_dir + source + "/product_catalog.parquet"
    valid_path = valid_dir + source + "/product_catalog.csv"
    invalid_path = invalid_dir + source + "/product_catalog.csv"
    archive_path = archive_dir + source + "/product_catalog.csv"
    logger.info("SUCCESS: create datalake paths :\n{}\n{}\n{}\n{}\n{}"
                .format(csv_path, parquet_path, valid_path, invalid_path, archive_path))
    return csv_path, parquet_path, valid_path, invalid_path, archive_path


def is_file_already_processed(archive_path):
    """
    Check if the file given by the archive_path parameter already exists.

    :param archive_path: path to the archive file in the archive directory, String
    :return: True if file already exists, False if not, Boolean
    """
    return os.path.isfile(archive_path)


def init_spark():
    """
    Init Spark Session with master set to local[*] and appName "transformater".

    :return: the Spark Session object, SparkSession
    :raise: raise an exception if the Spark Session cannot be build, Exception
    """
    try:
        spark = SparkSession.builder.master("local[*]").appName("transformater").getOrCreate()
        logger.info("SUCCESS: create Spark Session with appName 'transformater'")
        return spark
    except Exception as error:
        logger.error("FAILURE: cannot create Spark Session: {}".format(str(error)))
        raise Exception(error)


def csv_to_parquet(spark, csv_path, parquet_path):

    try:
        df = spark.read.format("csv").option("header", "true").load(csv_path)
        logger.info("SUCCESS: read {} rows in csv file {}".format(str(df.count()), csv_path))
        df.coalesce(1).write.mode("overwrite").parquet(parquet_path)
        logger.info("SUCCESS: write {} rows on parquet file {}".format(str(df.count()), parquet_path))
    except Exception as error:
        logger.error("FAILURE: Cannot transform csv {} to parquet file {} : {}"
                     .format(csv_path, parquet_path, str(error)))
        raise Exception(error)


def create_dataframe(spark, parquet_path):

    try:
        df = spark.read.format("parquet").load(parquet_path).cache()
        logger.info("SUCCESS: read {} rows in parquet file {}".format(str(df.count()), parquet_path))
        return df
    except Exception as error:
        logger.error("FAILURE: Unable to read data from parquet file {}: {}".format(parquet_path, str(error)))
        raise Exception(error)


def filter_valid_records(df):

    try:
        valid_df = df.filter(col("image").isNotNull())
        logger.info("SUCCESS: Filter found {} valid rows".format(str(valid_df.count())))
        return valid_df
    except Exception as error:
        logger.error("FAILURE: Unable to filter Dataframe on Not Null values for column image: {}".format(str(error)))
        raise Exception(error)


def filter_invalid_records(df):

    try:
        invalid_df = df.filter(col("image").isNull())
        logger.info("SUCCESS: Filter found {} invalid rows".format(str(invalid_df.count())))
        return invalid_df
    except Exception as error:
        logger.error("FAILURE: Unable to filter Dataframe on  Null values for column image: {}".format(str(error)))
        raise Exception(error)


def write_dataframe_to_csv(df, dest_path):

    try:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(dest_path)
        logger.info("SUCCESS: write {} rows on csv file {}".format(str(df.count()), dest_path))
    except Exception as error:
        logger.error("FAILURE: Unable to write Dataframe to csv file {}: {}".format(dest_path, str(error)))
        raise Exception(error)


def archive_input_csv(archive_dir, source, csv_path, archive_path):

    try:
        if not os.path.exists(archive_dir + source):
            os.mkdir(archive_dir + source)
        shutil.move(csv_path, archive_path)
        logger.info("SUCCESS: archive csv file {} to {}".format(csv_path, archive_path))
    except Exception as error:
        logger.error("FAILURE: Unable to move csv file to archive folder: {}".format(str(error)))
        raise Exception(error)


def main(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir):

    csv_path, parquet_path, valid_path, invalid_path, archive_path = \
        generate_paths(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir)
    if not is_file_already_processed(archive_path):
        spark = init_spark()
        csv_to_parquet(spark, csv_path, parquet_path)
        df = create_dataframe(spark, parquet_path)
        valid_df = filter_valid_records(df)
        invalid_df = filter_invalid_records(df)
        write_dataframe_to_csv(valid_df, valid_path)
        write_dataframe_to_csv(invalid_df, invalid_path)
        archive_input_csv(archive_dir, source, csv_path, archive_path)
        spark.stop()
    else:
        logger.info("ABORTING: File already proccess, found in {}, ending spark job".format(archive_path))


if __name__ == '__main__':

    if len(sys.argv) != 7:
        logger.error(
            "USAGE: transform.py <source> <ingress_dir> <parquet_dir> <valid_dir> <invalid_dir> <archive_dir>".format())
        exit(-1)

    logger.info("==================================================")
    logger.info("==== STARTING TRANSFORM: {} =====".format(datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')))
    logger.info("==================================================")

    try:
        main(
            source=sys.argv[1],
            ingress_dir=sys.argv[2],
            parquet_dir=sys.argv[3],
            valid_dir=sys.argv[4],
            invalid_dir=sys.argv[5],
            archive_dir=sys.argv[6]

        )
    except Exception as error:
        logger.fatal(str(error))

    finally:
        logger.info("==================================================")
        logger.info("================ END OF TRANSFORM ================")
        logger.info("==================================================")

