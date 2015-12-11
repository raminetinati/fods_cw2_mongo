# -*- coding: utf-8 -*-

import os
import json
import re
from data.collection import SimpleDataImporter
import csv, codecs
import re

DATASET_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dataset'))
INPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2.csv')
OUTPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2_3.csv')


# ● id - a unique identifier of the tuple
# ● id_member - a unique identifier of the user who posted the message
# ● timestamp - a UTC timestamp of when the message was published
# ● text - the microblog message that was published
# ● geo_lat - the latitude coordinate of where the message was posted from
# ● geo_lng - the longitude coordinate of where the message was posted from.

def main(*agrs):
    read_csv(INPUT_FILE, OUTPUT_FILE, format)

# Read CSV File
def read_csv(file, csv_output, format):
    clean(file)

"""
def clean(file):
    with open(file, 'rU') as csvfile:
        reader = csv.DictReader(csvfile)
        line_no = 0
        for row in reader:
            current_id = row['id']
            match1 = re.match("^[0-9]+$", current_id)
            if match1 is None:
                print(type(current_id))
                print(current_id)
                """

def clean2(file):
    with open(file, 'rU') as csvfile:
        reader = csv.reader(csvfile)
        line_no = 0
        pendingArr = []

        for row in reader:
            if len(row) < 6:
                pendingArr.extend(row)
                continue

            rightRecord = []
            if len(pendingArr) > 0:
                #you have pendingArr and currentRow(row) to care about
                #slice 3 first element of pendingArr to be id , id_member, timestamp
                #slice 2 last element of pendingArr to be geolocation
                #in the middle to be a whole text
                firstInfo = pendingArr[:3]
                wholeText = pendingArr[3:-2]
                locations = pendingArr[-2:]

                rightRecord.extend(firstInfo)
                rightRecord.extend(' '.join(wholeText))
                rightRecord.extend(locations)

            #execute rightRecord(pending) and row
            current_id = row['id']
            match1 = re.match("^[0-9]+$", current_id)
            if match1 is None:
                print(type(current_id))
                print(current_id)




if __name__ == '__main__': main()
