import argparse
import corenlp
import sys
import lib.openreview_lib as orl
import lib.openreview_db as ordb
import sqlite3

parser = argparse.ArgumentParser(
    description='Load OpenReview data into a sqlite3 database.')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')
parser.add_argument('-i', '--inputfile', default="splits/iclr19_split.json",
    type=str, help='path to database file')
parser.add_argument('-d', '--djangoinputfile', 
        default="django_input.json", type=str,
        help='path to output that will be django input')
parser.add_argument('-s', '--debug', action="store_true",
    help='if True, truncate the example list')

ANNOTATORS = "ssplit tokenize".split()

def create_metadata_json(conn):
    c = conn.cursor()
    rows = c.execute(("SELECT DISTINCT forum_id, split, parent_supernote,"
            "comment_supernote, author FROM traindev WHERE comment_type=?"),
            ("rebuttal",))
    print(len(rows))

def main():
  args = parser.parse_args()
  conn = ordb.create_connection(args.dbfile)
  if conn is not None:
    ordb.create_table(conn, ordb.CREATE_COMMENTS_TABLE)
  else:
    print("Error! cannot create the database connection.")


  conn = ordb.create_connection(args.dbfile)
  with corenlp.CoreNLPClient(
      annotators=ANNOTATORS, output_format='conll') as corenlp_client:
    orl.get_datasets(
        args.inputfile, corenlp_client, conn, debug=args.debug)

  metadata = create_metadata_json(conn)
  relevant_text = create_text_json(conn)
    

if __name__ == "__main__":
  main()
