def insert_into_datasets(conn, forum, split, conference):
  """Insert a record into the datasets table (train-test split)."""
  cmd = ''' INSERT INTO
              datasets(forum, split, conference)
              VALUES(?, ?, ?); '''
  cur = conn.cursor()
  cur.execute(cmd, (forum, split, conference))
  conn.commit()


def insert_into_structure(conn, forum, parent, comment, timestamp, author, split):
  """Insert a record into the structure table (forum-level structure)."""
  cmd = ''' INSERT INTO
              structure(forum, parent, comment, timestamp, author, split)
              VALUES(?, ?, ?, ?, ?, ?);'''
  cur = conn.cursor()
  cur.execute(cmd, (forum, parent, comment, timestamp, author, split))
  conn.commit()

def insert_into_text(conn, supernote, chunk_idx, original_note, note_type,
    chunk, split):
  """INsert a record into the text table (actual comment text)."""
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


