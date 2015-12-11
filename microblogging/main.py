# -*- coding: utf-8 -*-

import os
import json
import re
from data.collection import SimpleDataImporter
import csv, codecs
import re

DATASET_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dataset'))
INPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2.csv')


def main(*agrs):
    print("Opening csv ...")
    csv_rows = clean(INPUT_FILE)
    try:
        print("Importing rows in mongo")
        import_in_mongo(csv_rows)
    except Exception as ex:
        print(ex)


# Read CSV File
def import_in_mongo(rows):
    importer = SimpleDataImporter()
    importer.run(rows)
    importer.finish()


def clean(file):
    with open(file, 'rb') as infile:
        filtered = (line.replace('\r', '') for line in infile)
        reader = csv.reader(filtered)
        totalRows = []
        counter = 0

        print("Cleaning csv ...")
        for row in reader:
            if counter == 0:
                counter = 1
                continue

            row_dict = dict()
            row_dict['id'] = row[0]
            row_dict['id_member'] = clean_id_member(row[1])
            row_dict['timestamp'] = row[2]
            row_dict['text'] = unicode(row[3], errors='replace')
            row_dict['geo_lat'] = row[4]
            row_dict['geo_lng'] = row[5]

            totalRows.append(row_dict)
        return totalRows


def clean_id_member(tweet_id_member):
    match = re.match("^\d*$", tweet_id_member)
    if match is None:
        tweet_id_member = str(abs(int(tweet_id_member)))
    return tweet_id_member


if __name__ == '__main__': main()
