import argparse
import sqlite3
import sys

from tqdm import tqdm

import lib.db_lib as dbl
import lib.karp_rabin as kr

parser = argparse.ArgumentParser(
    description='Load OpenReview data from a sqlite3 database.')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')

EXACT_MATCH_TABLE = """CREATE TABLE IF NOT EXISTS {0} (
    review_supernote text NOT NULL,
    rebuttal_supernote text NOT NULL,
    review_chunk_idx text NOT NULL,
    rebuttal_chunk_idx text NOT NULL,
    review_token_offset text NOT NULL,
    rebuttal_token_offset text NOT NULL,
    lcs text NOT NULL,
    split text NOT NULL)"""

FIELDS = ("review_supernote rebuttal_supernote review_chunk_idx "
          "rebuttal_chunk_idx review_token_offset rebuttal_token_offset "
          "lcs split")
COM_SEPARATED_EM = ", ".join(FIELDS.split())

def flatten_match(match, set_split):
  return (match.review_location.supernote, match.rebuttal_location.supernote,
      match.review_location.chunk_idx, match.rebuttal_location.chunk_idx,
      match.review_location.token_idx, match.rebuttal_location.token_idx,
      match.lcs, set_split)

def main():

  args = parser.parse_args()
  conn = dbl.create_connection(args.dbfile)
  if conn is not None:
    cur = conn.cursor()

    for set_split in ["train", "dev", "test"]:
      for table_name in dbl.TextTables.ALL:
        cur.execute(EXACT_MATCH_TABLE.format(table_name + "_em"))
        print(set_split)
        cur.execute("SELECT * FROM {0} WHERE split=(?)".format(
              table_name +"_pairs"), (set_split, ))
        rows = cur.fetchall()
        for row in tqdm(rows):
          cur.execute(
            "SELECT * FROM {0} WHERE comment_supernote=?".format(table_name),
              (row["review_supernote"],))
          review_chunk_map = dbl.crunch_text_rows(cur.fetchall())
          cur.execute(
              "SELECT * FROM {0} WHERE comment_supernote=?".format(table_name),
              (row["rebuttal_supernote"],))
          rebuttal_chunk_map = dbl.crunch_text_rows(cur.fetchall())


          assert len(review_chunk_map) == len(rebuttal_chunk_map) == 1
          matches = kr.find_matches(review_chunk_map, rebuttal_chunk_map)
          for match in matches:
            cur.execute(
                "INSERT INTO {0} ({1}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)".format(
                  table_name + "_em", COM_SEPARATED_EM), flatten_match(match,
                    set_split))
          conn.commit()



if __name__ == "__main__":
  main()
