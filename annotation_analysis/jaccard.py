import argparse
import collections
import sys
import nltk

from nltk.corpus import stopwords

import sqlite3
from sqlite3 import Error

import agreement

parser = argparse.ArgumentParser(
    description='Calculate agreement from rd-annotator datables')
parser.add_argument('-d', '--dbfile', default="/Users/nnayak/git_repos/rd-annotator/rdasite/db.sqlite3",
    type=str, help='path to database file')


def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d


def collapse_dict(input_dict):
  assert sum(input_dict.keys()) + 5 > 0
  # This is a garbage way to get an error if the keys are not ints
  return [input_dict[i] for i in sorted(input_dict.keys())]



def create_connection(db_file):
  """ create a database connection to a SQLite database """
  conn = None
  try:
    conn = sqlite3.connect(db_file)
    conn.row_factory = dict_factory
    return conn
  except Error as e:
    print(e)


STOPWORDS = set(stopwords.words('english'))

def jaccard(chunk_1_tokens, chunk_2_tokens):
    tokens_1 = set([c.lower() for c in chunk_1_tokens]) - STOPWORDS
    tokens_2 = set([c.lower() for c in chunk_2_tokens]) - STOPWORDS


    return len(tokens_1.intersection(tokens_2)) / len(tokens_1.union(tokens_2))


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

def get_text(c, supernote):
    rows = c.execute("SELECT * FROM alignments_text WHERE comment_supernote=?",
            (supernote, ))
    return crunch_text_rows(rows)

def best_jaccard_match(chunk, review_chunks):
    chunk_tokens = sum(chunk, [])
    max_jaccard_index = -1
    max_jaccard = 0.0
    for i, review_chunk in enumerate(review_chunks):
        review_chunk_tokens = sum(review_chunk, [])
        new_jaccard = jaccard(chunk_tokens, review_chunk_tokens)
        if new_jaccard > max_jaccard:
            max_jaccard = new_jaccard
            max_jaccard_index = i

    return max_jaccard_index

def main():
    args = parser.parse_args()
    conn = create_connection(args.dbfile)

    c = conn.cursor()

    pairs = c.execute(("SELECT DISTINCT review_supernote, rebuttal_supernote "
    "FROM alignments_annotatedpair;")).fetchall()
    matches = []
    for pair in pairs:
        review_chunks, = get_text(c, pair["review_supernote"]).values()
        rebuttal_chunks,  = get_text(c, pair["rebuttal_supernote"]).values()
        for i, chunk in enumerate(rebuttal_chunks):
            human_labels = c.execute(("SELECT * FROM "
            "alignments_alignmentannotation WHERE rebuttal_supernote=? AND "
            "rebuttal_chunk=?"),
            (pair["rebuttal_supernote"], i))
            label_set = []
            for x in human_labels:
                if x["annotator"] == "TJO" or "test" in x["annotator"]:
                    continue
                else:
                    label_set.append(x["label"])
            matches.append((pair["rebuttal_supernote"], i,
                best_jaccard_match(chunk, review_chunks),
                agreement.get_match_value(label_set), *label_set))
            
    for i in matches:
        print("\t".join(str(j) for j in i))
            

if __name__ == "__main__":
    main()
