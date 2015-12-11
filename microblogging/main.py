# -*- coding: utf-8 -*-

import os
import json
import re
from data.collection import SimpleDataImporter
import csv, codecs
import re

DATASET_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dataset'))
INPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2.csv')
CSV_OUTPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2_2.csv')
OUTPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2.json')


# ● id - a unique identifier of the tuple
# ● id_member - a unique identifier of the user who posted the message
# ● timestamp - a UTC timestamp of when the message was published
# ● text - the microblog message that was published
# ● geo_lat - the latitude coordinate of where the message was posted from
# ● geo_lng - the longitude coordinate of where the message was posted from.

def main(*agrs):
    read_csv(INPUT_FILE, CSV_OUTPUT_FILE, format)


# Read CSV File
def read_csv(file, csv_output, format):
    csv_rows = []
    # with open(file, 'rb') as csvfile, open(csv_output, 'a') as outfile:
    #     print("Opening csv file...")
    #     filtered = (line.replace('\r', '') for line in csvfile)
    #     error_counter = 0
    #     line_no = 0
    #     for row in filtered:
    #         outfile.write(row)
    #         line_no += 1
    #     print("Finished parsing csv file")
    #     print('\n' + str(error_counter) + " errors found")
    #     outfile.close()
    clean(csv_output)


def clean(csv_output):
    with open(csv_output, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        line_no = 0
        for row in reader:
            line_no += 1
            print row
            if line_no == 2300:
                exit()


if __name__ == '__main__': main()
