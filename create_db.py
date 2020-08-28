import argparse
import openreview_db as ordb

parser = argparse.ArgumentParser(
    description='Create sqlite3 database for OpenReview data.')
parser.add_argument('-d', '--dbfile', default="sqlite/db/pythonsqlite.db",
    type=str, help='path to database file')


def main():

  args = parser.parse_args()
  conn = ordb.create_connection(args.dbfile)
  if conn is not None:
    ordb.create_table(conn, ordb.CREATE_DATASET_TABLE)
    ordb.create_table(conn, ordb.CREATE_STRUCTURE_TABLE)
    ordb.create_table(conn, ordb.CREATE_TEXT_TABLE)
    conn.close()
  else:
    print("Error! cannot create the database connection.")


if __name__ == '__main__':
  main()
