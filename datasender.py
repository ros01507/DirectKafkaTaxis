#!/usr/bin/python

import csv
import time
#----------------------------------------------------------------------
def csv_reader(file_obj):
    reader = csv.reader(file_obj)
    for row in reader:
        print(" ".join(row))
        time.sleep( 0.1 )
#----------------------------------------------------------------------
if __name__ == "__main__":
    csv_path = "/home/ubuntu_descarga_taxis/yellow_tripdata_2016-01.csv"
    with open(csv_path, "rb") as f_obj:
        csv_reader(f_obj)

		