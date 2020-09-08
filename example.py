import sqlite3
import argparse
import lib.openreview_db as ordb

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
  cur.execute("SELECT * FROM comments WHERE split=?", ("train",))

  rows = cur.fetchall()
  print(len(rows))
  obj = ordb.crunch_text_rows(rows)
  num_tokens = 0
  num_comments = 0
  for comment, text in obj.items():
    if len(text) == 1: # One chunk
      print(" ".join(sum(text[0], [])))
      num_tokens += len(sum(text[0], []))
      num_comments += 1


  print(num_tokens)
  print(num_comments)
  print(num_tokens / num_comments)


  #print(obj.keys())
  #print(len(obj.keys()))



if __name__ == "__main__":
  main()



