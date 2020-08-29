import argparse
import corenlp
import sys
import openreview_lib as orl
import openreview_db as ordb
import sqlite3

parser = argparse.ArgumentParser(
    description='Load OpenReview data into a sqlite3 database.')
parser.add_argument('-d', '--dbfile', default="sqlite/db/pythonsqlite.db",
    type=str, help='path to database file')
parser.add_argument('-i', '--inputfile', default="splits/iclr19_split.json",
    type=str, help='path to database file')
parser.add_argument('-s', '--debug', action="store_true",
    help='if True, truncate the example list')

ANNOTATORS = "ssplit tokenize".split()


def main():

  args = parser.parse_args()
  conn = ordb.create_connection(args.dbfile)
  with corenlp.CoreNLPClient(
      annotators=ANNOTATORS, output_format='conll') as corenlp_client:
    datasets = orl.get_datasets(
        args.inputfile, corenlp_client, conn, debug=args.debug)
    

if __name__ == "__main__":
  main()
