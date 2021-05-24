import argparse
import sys

import openreview_db as ordb


parser = argparse.ArgumentParser(
    description='Example for accessing OpenReview data')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')


def main():
  
  args = parser.parse_args()

  conn = ordb.create_connection(args.dbfile)
  cur = conn.cursor()
  cur.execute("SELECT * FROM text WHERE split=?", ("train",))
  rows = cur.fetchall()
  text_data = ordb.crunch_text_rows(rows)

  print("num_chunks num_tokens")
  for comment_id, chunks in text_data.items():
    sentences = sum(chunks, [])
    num_tokens = len(sum(sentences, []))
    print(" ".join([str(len(chunks)), str(num_tokens)]))


  
if __name__ == "__main__":
  main()
