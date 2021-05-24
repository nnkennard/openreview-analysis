import argparse
import json
import math
import sys

import lib.openreview_db as ordb


parser = argparse.ArgumentParser(
    description='Example for accessing OpenReview data')
parser.add_argument('-d', '--dbfile', default="../db/or.db",
    type=str, help='path to database file')
parser.add_argument('-p', '--pathsfile', default="characterized_paths.json",
    type=str, help='path to characteristic paths file')


def load_characteristic_paths(filename):
  with open(filename, 'r') as f:
    list_paths = json.loads(f.read())

  characteristic_paths = []
  for nodes, char in list_paths:
    comments = [ordb.Comment(*node) for node in nodes]
    characteristic_paths.append(ordb.CharacterizedPath(comments, char))

  return characteristic_paths

WINDOW = 7
Q = 2124749677  # Is this too big for Python int


def myhash(tokens):
  tok_str = "".join(tokens)
  hash_acc = 0
  for i, ch in enumerate(reversed(tok_str)):
    hash_acc += math.pow(2, i) * ord(ch)

  return hash_acc % Q


def get_hashes(tokens):
  return {i:myhash(tokens[i:i+WINDOW]) for i in range(len(tokens) - WINDOW)}


def karp_rabin(tokens_1, tokens_2):
  hashes_1 = get_hashes(tokens_1)
  hashes_2 = get_hashes(tokens_2)
  results = []
  for k1, v1 in hashes_1.items():
    for k2, v2 in hashes_2.items():
      if v1 == v2:
        results.append((k1, k2))
  return sorted(results)


def check_exact_match(parent_chunks, child_chunks):
  child_chunks_mapped = {i:None for i in range(len(child_chunks))}
  parent_chunks_mapped = {i:None for i in range(len(parent_chunks))}
  lcs_map ={}
  for i, child_chunk in enumerate(child_chunks):
    for j, parent_chunk in enumerate(parent_chunks):
      x = karp_rabin(sum(child_chunk, []), sum(parent_chunk, []))
      if x:
        child_chunks_mapped[i], parent_chunks_mapped[j] = j, i
        lcs_map[(i,j)] = child_chunk[x[0][0]:x[0][0]+WINDOW] 
  assert len(parent_chunks) == len(parent_chunks_mapped)
  assert len(child_chunks) == len(child_chunks_mapped)

  return parent_chunks_mapped, child_chunks_mapped, lcs_map


def get_url(comment):
  return "https://openreview.net/forum?id={0}&noteId={1}".format(
      comment.forum_id, comment.comment_id)

def flatten(chunk):
  return " ".join(sum(chunk, []))


def main():
  
  args = parser.parse_args()

  characteristic_paths = load_characteristic_paths(args.pathsfile)

  conn = ordb.create_connection(args.dbfile)
  cur = conn.cursor()
  cur.execute("SELECT * FROM text WHERE split=?", ("train",))
  rows = cur.fetchall()
  text_data = ordb.crunch_text_rows(rows)

  with open('temp.txt', 'wb') as f:
    for path in characteristic_paths:
      if path.char.startswith("Conference_Reviewer0_Author"):
        _, review_meta, rebuttal_meta = path.comments[:3]
        review = text_data[review_meta.comment_id]
        rebuttal = text_data[rebuttal_meta.comment_id]
        print("\t".join(str(i) for i in [len(review), len(rebuttal)]))

        review_map, rebuttal_map, lcs_map = check_exact_match(review, rebuttal)

        for review_chunk_id, rebuttal_match_chunk_id in review_map.items():
          if rebuttal_match_chunk_id is None:
            continue
          maybe_lcs = lcs_map[(rebuttal_match_chunk_id, review_chunk_id)]
          if maybe_lcs:
            lcs = " ".join(maybe_lcs[0]).encode("utf-8")
            f.write("*" * 80 + "\n" + lcs + "\n")
          f.write(str((review_chunk_id, rebuttal_match_chunk_id)) + "\n")
          review_chunk = flatten(review[review_chunk_id])
          rebuttal_chunk = flatten(rebuttal[rebuttal_match_chunk_id])

          f.write(review_chunk.encode("utf-8"))
          f.write("\n-\n")
          f.write(rebuttal_chunk.encode("utf-8"))
          f.write("\n-\n")
          try:
            f.write(flatten(rebuttal[rebuttal_match_chunk_id + 1]).encode("utf-8"))
          except IndexError:
            f.write("END")
          f.write("\n" + "-" * 80 + "\n")


  
    
if __name__ == "__main__":
  main()

