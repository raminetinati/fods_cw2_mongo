# -*- coding: utf-8 -*-

import os
import json
from data.collection import SimpleDataImporter
import csv, codecs

DATASET_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dataset'))
INPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2.csv')
CSV_OUTPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2_2.csv')
OUTPUT_FILE = os.path.join(DATASET_PATH, 'microblogDataset_COMP6235_CW2.json')


def main(*agrs):
    dataset_name = None
    # ● id - a unique identifier of the tuple
    # ● id_member - a unique identifier of the user who posted the message
    # ● timestamp - a UTC timestamp of when the message was published
    # ● text - the microblog message that was published
    # ● geo_lat - the latitude coordinate of where the message was posted from
    # ● geo_lng - the longitude coordinate of where the message was posted from.

    def main(argv):
        if len(agrs) > 0:
            dataset_name = agrs[0]
        else:
            dataset_name = 'microblogDataset_COMP6235_CW2.json'

        format = 'pretty'
        # clean_file(INPUT_FILE)
        # read_csv(CSV_OUTPUT_FILE, OUTPUT_FILE, format)
        read_csv(INPUT_FILE, OUTPUT_FILE, format)

        simpleImporter = SimpleDataImporter()
        simpleImporter.run(dataset_name, True)
        simpleImporter.finish()

    # Read CSV File
    def read_csv(file, json_file, format):
        csv_rows = []

        with open(file, 'rU') as csvfile:
            reader = csv.DictReader(csvfile)
            title = reader.fieldnames
            for row in reader:
                csv_rows.extend([{title[i]: row[title[i]] for i in range(len(title))}])
            write_json(csv_rows, json_file, format)

    # Convert csv data into json and write it
    def write_json(data, json_file, format):

        with codecs.open(json_file, "w", encoding='utf-8') as f:
            if format == "pretty":
                f.write(json.dumps(data, sort_keys=False, indent=4, separators=(',', ': '), encoding="utf-8",ensure_ascii=False))
            else:
                f.write(json.dumps(data))

    # def clean_file(file):
    #     with open(file) as bigfile:
    #         for lineno, line in enumerate(bigfile):
    #             encoded_line = line.decode('latin-1')
    #             smallfile = codecs.open(CSV_OUTPUT_FILE, 'a', encoding='utf-8')
    #             smallfile.write(encoded_line)
    #         if smallfile:
    #             smallfile.close()

if __name__ == '__main__': main()
