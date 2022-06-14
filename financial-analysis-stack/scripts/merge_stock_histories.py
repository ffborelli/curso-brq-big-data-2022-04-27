#!/usr/bin/env python3.8
from argparse import ArgumentParser
import sys
import re
import os
import csv

r_symbol = re.compile('(.+)\.csv$')

def get_histories(folder):
    with os.scandir(args.folder) as it:
        for entry in it:
            if match := r_symbol.match(entry.name):
                symbol = match.group(1)

                with open(entry) as history:
                    reader = csv.reader(history)
                    next(reader) # skip header

                    yield symbol, reader

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('folder')
    parser.add_argument('out')
    args = parser.parse_args(sys.argv[1:])

    with open(args.out, 'w') as outfile:
        writer = csv.writer(outfile)

        for symbol, reader in get_histories(args.folder):
            for row in reader:
                writer.writerow([symbol, *row])

