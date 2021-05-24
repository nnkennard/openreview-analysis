import collections
import sqlite3

class TextTables(object):
  UNSTRUCTURED = "unstructured"
  TRAIN_DEV = "traindev"
  TRUE_TEST = "truetest"
  ALL = [UNSTRUCTURED, TRAIN_DEV, TRUE_TEST]


def create_connection(db_file):
  """ create a database connection to a SQLite database """
  conn = None
  try:
    conn = sqlite3.connect(db_file)
    conn.row_factory = dict_factory
    return conn
  except sqlite3.Error as e:
    print(e)

def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d


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
        row["chunk_idx"], row["sentence_idx"], row["token"])
    texts_builder[supernote][chunk][sentence].append(token)

  texts = {}
  for supernote, chunk_dict in texts_builder.items():
    texts[supernote] = [collapse_dict(sentence) 
        for sentence in collapse_dict(chunk_dict)]

  return texts
