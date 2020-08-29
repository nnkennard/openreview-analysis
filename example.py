import sqlite3
import argparse
import openreview_db as ordb

parser = argparse.ArgumentParser(
    description='Example for accessing OpenReview data')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def main():

  args = parser.parse_args()

  conn = ordb.create_connection(args.dbfile)
  conn.row_factory = dict_factory


  cur = conn.cursor()
  cur.execute("SELECT * FROM text WHERE split=?", ("train",))

  rows = cur.fetchall()
  obj = ordb.crunch_text_rows(rows)
  print(obj[sorted(obj.keys())[0]])



if __name__ == "__main__":
  main()



