import os
import shutil
import wget

# list of the datalake folder paths to create
paths = ["./datalake", "./datalake/archive", "./datalake/ingress", "./datalake/raw", "./datalake/raw/copyRawFiles", "./datalake/raw/valid", "./datalake/raw/invalid"]

# delete older datalake folder
try:
    shutil.rmtree("./datalake", ignore_errors=True)
    print("INFO : Successfully remove datalake folder")
except Exception as error:
    print("ERROR : Cannot delete datalake folder: {}".format(str(error)))
    exit(-1)

# create datalake folder architecture
try:
    for path in paths:
        os.mkdir(path)
    print("INFO : Successfully create datalake folder architecture")
except Exception as error:
    print("ERROR : Unable to create datalake folder architecture: {}".format(str(error)))
    exit(1)

# download product_catalog.csv file to ingress folder
try:
    url = "https://backmarket-data-jobs.s3-eu-west-1.amazonaws.com/data/product_catalog.csv"
    path = os.path.abspath("datalake/ingress/product_catalog.csv")
    wget.download(url, path)
    print("\nINFO : Successfully download product_catalog.csv to {}".format(path))
except Exception as error:
    print("ERROR: Unable to upload csv file on ingress folder: {}".format(str(error)))
    exit(-1)
