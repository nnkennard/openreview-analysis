import argparse
import collections
import csv
import pandas as pd
import sqlite3
import sys
import lib.openreview_db as ordb

parser = argparse.ArgumentParser(
    description='Example for accessing OpenReview data')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')
def count_comment_types(rows):
  forums = collections.defaultdict(dict)
  authors = {}
  reviews = []
  not_reviews = []
  rebuttals = []

  comments_by_author_type = collections.defaultdict(list)

  for row in rows:
    shortened_author = shorten_author(row["author"])
    comments_by_author_type[shortened_author].append(row["comment_supernote"])
    forums[row["forum"]][row["comment_supernote"]] = row["parent_supernote"]
    authors[row["comment_supernote"]] = row["author"]
    if (row["parent_supernote"] == row["forum"]
        and shortened_author == AuthorCategories.REVIEWER):
      reviews.append(row["comment_supernote"])
    else:
      not_reviews.append(row["comment_supernote"])
    if shortened_author == AuthorCategories.AUTHOR:
      if row["parent_supernote"] in reviews:
        rebuttals.append(row["comment_supernote"])
      elif row["parent_supernote"] == row["forum"]:
        continue
      else:
        assert row["parent_supernote"] in not_reviews

  result = {key:len(val) for key, val in comments_by_author_type.items()}
  result["reviews"] = len(reviews)
  result["rebuttals"] = len(rebuttals)
  return result

def get_examples(rows):
  forums = collections.defaultdict(dict)
  reviews = []
  not_reviews = []
  review_rebuttals = collections.defaultdict(list)

  for row in rows:
    shortened_author = shorten_author(row["author"])
    if (row["parent_supernote"] == row["forum"]
        and shortened_author == AuthorCategories.REVIEWER):
      reviews.append(row["comment_supernote"])
    else:
      not_reviews.append(row["comment_supernote"])
    if shortened_author == AuthorCategories.AUTHOR:
      if row["parent_supernote"] in reviews:
        review_rebuttals[row["forum"]].append(
            (row["parent_supernote"], row["comment_supernote"]))
      elif row["parent_supernote"] == row["forum"]:
        continue
      else:
        assert row["parent_supernote"] in not_reviews


  inter_pairs = collections.defaultdict(list)
  for forum, intra_pairs in review_rebuttals.items():
    reviews, rebuttals = zip(*intra_pairs)
    for review in reviews:
      for rebuttal in rebuttals:
        maybe_inter_pair = (review, rebuttal)
        if maybe_inter_pair not in intra_pairs:
          inter_pairs[forum].append(maybe_inter_pair)
    #print("*", len(intra_pairs))
    #print("**", len(inter_pairs[forum]))

  print(len(inter_pairs), len(review_rebuttals))
  print(
      len(sum(inter_pairs.values(), [])),
      len(sum(review_rebuttals.values(), [])))

def main():

  args = parser.parse_args()

  conn = ordb.create_connection(args.dbfile)

  cur = conn.cursor()

  table_maps = collections.defaultdict(dict)

  for split in ["train", "dev", "test"]:
    print(split)
    cur.execute("SELECT COUNT(DISTINCT forum) FROM comments WHERE split=?", (split,))
    update_map, = cur.fetchall()
    table_maps[split].update(update_map)
    cur.execute("SELECT COUNT(DISTINCT comment_supernote) FROM comments WHERE split=?", (split,))
    update_map, = cur.fetchall()
    table_maps[split].update(update_map)
    cur.execute("SELECT DISTINCT forum, parent_supernote, comment_supernote, author, note_type, timestamp FROM comments WHERE split=? ORDER BY timestamp", (split,))
    rows = cur.fetchall()
    table_maps[split].update(count_comment_types(rows))
    get_examples(rows)

  for k, v in table_maps.items():
    for g, s in v.items():
      print(k, g, s)

  print(pd.DataFrame.from_dict(data=table_maps, orient='columns')
         .to_csv('table_1.csv', header=True))


if __name__ == "__main__":
  main()
