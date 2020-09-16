import argparse
import collections

import lib.db_lib as dbl

parser = argparse.ArgumentParser(
        description='Load OpenReview data from a sqlite3 database.')
parser.add_argument('-d', '--dbfile', default="../db/or.db",
        type=str, help='path to database file')


def col_counter(input_list, column_name):
  return  collections.Counter([k[column_name] for k in input_list])

def main():

  args = parser.parse_args()
  conn = dbl.create_connection(args.dbfile)
  if conn is None:
    print("Connection error")
    exit()

  cur = conn.cursor()

  table_1 = {}


  for table_name in dbl.TextTables.ALL:
    for set_split in ["train", "dev", "test"]:
      results = {}

      # Total papers
      cur.execute(
          ("SELECT COUNT(DISTINCT forum_id) FROM {0} WHERE "
          "split=?").format(table_name), (set_split,))
      x, = cur.fetchall()
      if set(x.values()) == set([0]):
        continue
      results["Total forums"] = x["COUNT(DISTINCT forum_id)"]

      # Total comments
      cur.execute(
          ("SELECT COUNT(DISTINCT comment_supernote) FROM {0} WHERE "
          "split=?").format(table_name), (set_split,))
      x, = cur.fetchall()
      results["Total comments"] = x["COUNT(DISTINCT comment_supernote)"]

      # Comment types
      cur.execute(
          ("SELECT DISTINCT comment_type, comment_supernote FROM {0} WHERE "
          "split=?").format(table_name), (set_split,))
      x = cur.fetchall()
      results["Comment types"] = col_counter(x, "comment_type")

      # Author types
      cur.execute(
          ("SELECT DISTINCT author_type, comment_supernote FROM {0} WHERE "
          "split=?").format(table_name), (set_split,))
      x = cur.fetchall()
      results["Author types"] =  col_counter(x, "author_type")

      table_1[(table_name, set_split)] = results

  for k, v in table_1.items():
    print(k)
    print(v)
    print()

if __name__ == "__main__":
  main()
