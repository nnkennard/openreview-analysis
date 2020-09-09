import collections
import sqlite3
from sqlite3 import Error

def create_connection(db_file):
  """ create a database connection to a SQLite database """
  conn = None
  try:
    conn = sqlite3.connect(db_file)
    conn.row_factory = dict_factory
    return conn
  except Error as e:
    print(e)

def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d



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

CREATE_COMMENTS_TABLE = """ CREATE TABLE IF NOT EXISTS comments (
    forum text NOT NULL,
    parent_supernote text NOT NULL,
    comment_supernote text NOT NULL,
    original_note text NOT NULL,

    timestamp text NOT NULL,
    author text NOT NULL,
    note_type text NOT NULL,

    chunk integer NOT NULL,
    sentence integer NOT NULL,
    tok_index integer NOT NULL,
    token text NOT NULL,

    split text NOT NULL,
    PRIMARY KEY (original_note, chunk, sentence, tok_index)); """


def insert_into_comments(conn,
    forum, parent_supernote, comment_supernote, original_note,
    timestamp, author, note_type, chunk,
    sentence, tok_index, token, split):
  """Insert a record into the datasets table (train-test split)."""
  cmd = ''' INSERT INTO
              comments(
    forum, parent_supernote, comment_supernote, original_note,
    timestamp, author, note_type, chunk,
    sentence, tok_index, token, split
              )
              VALUES(?, ?, ?, ?,
                     ?, ?, ?, ?,
                     ?, ?, ?, ?); '''
  cur = conn.cursor()
  cur.execute(cmd, (
    forum, parent_supernote, comment_supernote, original_note,
    timestamp, author, note_type, chunk,
    sentence, tok_index, token, split))
  conn.commit()


def collapse_dict(input_dict):
  assert sum(input_dict.keys()) + 5 > 0
  # This is a garbage way to get an error if the keys are not ints
  return [input_dict[i] for i in sorted(input_dict.keys())]


def crunch_text_rows(rows):
  """Crunch rows from text table back into a more readable format.

  TODO(nnk): This, but in a non-horrible way
  """
  texts_builder = collections.defaultdict(lambda : collections.defaultdict(lambda:
    collections.defaultdict(list)))

  for row in rows:
    supernote, chunk, sentence, token = (row["comment_supernote"],
        row["chunk"], row["sentence"], row["token"])
    texts_builder[supernote][chunk][sentence].append(token)

  texts = {}
  for supernote, chunk_dict in texts_builder.items():
    texts[supernote] = [collapse_dict(sentence) for sentence in 
    collapse_dict(chunk_dict)]

  return texts
