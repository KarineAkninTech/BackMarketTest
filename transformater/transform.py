import logging
from logging.handlers import RotatingFileHandler
import sys
import datetime
import os

from pyspark.sql import SparkSession

logger = logging.getLogger('pyspark')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler("transform.log", mode="a", maxBytes=1000000000, backupCount=0, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def generate_paths(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir):

    csv_path = ingress_dir + "product_catalog.csv"
    parquet_path = parquet_dir + source + "/product_catalog.parquet"
    valid_path = valid_dir + source + "/product_catalog.csv"
    invalid_path = invalid_dir + source + "/product_catalog.csv"
    archive_path = archive_dir + source + "/product_catalog.csv"
    logger.info("SUCCESS: create datalake paths :\n{}\n{}\n{}\n{}\n{}"
                .format(csv_path, parquet_path, valid_path, invalid_path, archive_path))
    return csv_path, parquet_path, valid_path, invalid_path, archive_path


def is_file_already_processed(archive_path):
    return os.path.isfile(archive_path)


def init_spark():

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


def main(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir):

    csv_path, parquet_path, valid_path, invalid_path, archive_path = \
        generate_paths(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir)
    if not is_file_already_processed(archive_path):
        spark = init_spark()
        csv_to_parquet(spark, csv_path, parquet_path)
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

