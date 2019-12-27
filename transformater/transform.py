import logging
from logging.handlers import RotatingFileHandler
import sys

logger = logging.getLogger('pyspark')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = RotatingFileHandler("transform.log", mode="a", maxBytes=1000000000, backupCount=0, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == '__main__':
    if len(sys.argv) != 7:
        logger.error(
            "USAGE: transform.py <source> <ingress_dir> <parquet_dir> <valid_dir> <invalid_dir> <archive_dir>".format())
        exit(-1)

