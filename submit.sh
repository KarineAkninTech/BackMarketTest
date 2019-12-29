#!/usr/bin/env bash

absolute_path=$(pwd)
source="product_catalog"
ingress="datalake/ingress/"
parquet="datalake/raw/copyRawFiles/"
valid="datalake/raw/valid/"
invalid="datalake/raw/invalid/"
archive="datalake/archive/"

ingress_dir="${absolute_path}/${ingress}"
parquet_dir="${absolute_path}/${parquet}"
valid_dir="${absolute_path}/${valid}"
invalid_dir="${absolute_path}/${invalid}"
archive_dir="${absolute_path}/${archive}"

spark-submit --master local transformater/transform.py $source $ingress_dir $parquet_dir $valid_dir $invalid_dir $archive_dir