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


CREATE_DATASET_TABLE = """ CREATE TABLE IF NOT EXISTS datasets (
                                        forum text PRIMARY KEY,
                                        split text NOT NULL,
                                        conference text NOT NULL
                                    ); """


def insert_into_datasets(conn, forum, split, conference):
  """Insert a record into the datasets table (train-test split)."""
  cmd = ''' INSERT INTO
              datasets(forum, split, conference)
              VALUES(?, ?, ?); '''
  cur = conn.cursor()
  cur.execute(cmd, (forum, split, conference))
  conn.commit()

CREATE_STRUCTURE_TABLE = """ CREATE TABLE IF NOT EXISTS structure (
                                        forum text NOT NULL,
                                        parent text NOT NULL,
                                        comment text PRIMARY KEY,
                                        timestamp text NOT NULL,
                                        author text NOT NULL,
                                        split text NOT NULL
                                    ); """



def insert_into_structure(conn, forum, parent, comment, timestamp, author, split):
  """Insert a record into the structure table (forum-level structure)."""
  cmd = ''' INSERT INTO
              structure(forum, parent, comment, timestamp, author, split)
              VALUES(?, ?, ?, ?, ?, ?);'''
  cur = conn.cursor()
  cur.execute(cmd, (forum, parent, comment, timestamp, author, split))
  conn.commit()
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


def insert_into_text(conn, supernote, chunk_idx, original_note, note_type,
    chunk, split):
  """Insert a record into the text table (actual comment text)."""
  cmd = ''' INSERT INTO
              text(supernote, chunk, original_note, note_type, sentence,
              tok_index, token, split)
              VALUES(?, ?, ?, ?, ?, ?, ?, ?) '''
  cur = conn.cursor()
  for sentence_i, sentence in enumerate(chunk):
    for token_i, token in enumerate(sentence):
      cur.execute(cmd, (supernote, chunk_idx, original_note, note_type,
        sentence_i, token_i, token, split))
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
    supernote, chunk, sentence, token = (row["supernote"], row["chunk"],
    row["sentence"], row["token"])
    texts_builder[supernote][chunk][sentence].append(token)

  texts = {}
  for supernote, chunk_dict in texts_builder.items():
    texts[supernote] = [collapse_dict(sentence) for sentence in 
    collapse_dict(chunk_dict)]

  return texts


Comment = collections.namedtuple("Comment",
    "forum_id parent_id comment_id timestamp author")

    
def crunch_structure_rows(rows):
  """Crunch rows from text table back into a more readable format.

  TODO(nnk): This, but in a non-horrible way
  """
  structure_builder = collections.defaultdict(lambda : collections.defaultdict(lambda:
    collections.defaultdict(list)))

  comment_map = {}
  structure_map = collections.defaultdict(dict)

  for row in rows:
    forum, parent, comment = row["forum"], row["parent"], row["comment"]
    timestamp, author = row["timestamp"], row["author"]
    comment_map[comment] = Comment(forum, parent, comment, timestamp, author)
    structure_map[forum][comment] = parent

  return structure_map, comment_map
    

