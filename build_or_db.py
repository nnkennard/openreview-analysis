import corenlp
import sys
import openreview_lib as orl
import sqlite3

ANNOTATORS = "ssplit tokenize".split()

def create_connection(db_file):
  """Create a database connection to a SQLite database."""
  conn = None
  try:
    conn = sqlite3.connect(db_file)
    return conn
  except sqlite3.Error as e:
    print(e)


def main():

  dataset_file = sys.argv[1]
  conn = create_connection("sqlite/db/pythonsqlite.db")
  with corenlp.CoreNLPClient(
      annotators=ANNOTATORS, output_format='conll') as corenlp_client:
    datasets = orl.get_datasets(
        dataset_file, corenlp_client, conn, debug=True)
    

if __name__ == "__main__":
  main()
