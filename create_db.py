import sqlite3
from sqlite3 import Error

CREATE_DATASET_TABLE = """ CREATE TABLE IF NOT EXISTS datasets (
                                        forum text PRIMARY KEY,
                                        split text NOT NULL,
                                        conference text NOT NULL
                                    ); """

CREATE_STRUCTURE_TABLE = """ CREATE TABLE IF NOT EXISTS structure (
                                        forum text NOT NULL,
                                        parent text NOT NULL,
                                        comment text PRIMARY KEY,
                                        timestamp text NOT NULL,
                                        author text NOT NULL,
                                        split text NOT NULL
                                    ); """

CREATE_TEXT_TABLE = """ CREATE TABLE IF NOT EXISTS text (
                                        supernote text NOT NULL,
                                        chunk integer NOT NULL,
                                        original_note text NOT NULL,
                                        note_type text NOT NULL,
                                        sentence integer NOT NULL,
                                        tok_index integer NOT NULL,
                                        token text NOT NULL,
                                        split text NOT NULL,
                                        PRIMARY KEY (original_note, chunk, sentence, tok_index)
                                    ); """


def create_connection(db_file):
  """ create a database connection to a SQLite database """
  conn = None
  try:
    conn = sqlite3.connect(db_file)
    return conn
  except Error as e:
    print(e)


def create_table(conn, create_table_sql):
  """ create a table from the create_table_sql statement
  :param conn: Connection object
  :param create_table_sql: a CREATE TABLE statement
  :return:
  """
  try:
    c = conn.cursor()
    c.execute(create_table_sql)
  except Error as e:
    print(e)

def main():
  database = r"sqlite/db/pythonsqlite.db"
  conn = create_connection(database)
  if conn is not None:
    create_table(conn, CREATE_DATASET_TABLE)
    create_table(conn, CREATE_STRUCTURE_TABLE)
    create_table(conn, CREATE_TEXT_TABLE)
    conn.close()
  else:
    print("Error! cannot create the database connection.")


 

if __name__ == '__main__':
  main()
