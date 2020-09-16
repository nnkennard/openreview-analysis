import argparse
import sqlite3
import sys

import lib.db_lib as dbl

parser = argparse.ArgumentParser(
    description='Load OpenReview data from a sqlite3 database.')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')


def main():

  args = parser.parse_args()
  conn = dbl.create_connection(args.dbfile)
  if conn is not None:
    cur = conn.cursor()
    cur.execute("SELECT * FROM traindev_pairs WHERE split=? LIMIT 10",
        ("train",))
    rows = cur.fetchall()
    for row in rows:
      cur.execute("SELECT * FROM traindev WHERE comment_supernote=?",
          (row["review_supernote"],))
      
      k = dbl.crunch_text_rows(cur.fetchall())

      for _, chunks in k.items():
        for chunk in chunks:
          for sentence in chunk:
            print(" ".join(sentence))
          print()
        print("*" * 80)


if __name__ == "__main__":
  main()
