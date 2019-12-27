import logging
from logging.handlers import RotatingFileHandler
import sys
import datetime

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


def main(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir):

    csv_path, parquet_path, valid_path, invalid_path, archive_path = \
        generate_paths(source, ingress_dir, parquet_dir, valid_dir, invalid_dir, archive_dir)


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

