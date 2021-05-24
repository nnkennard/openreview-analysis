import argparse
import collections
import sys

import sqlite3
from sqlite3 import Error

parser = argparse.ArgumentParser(
    description='Calculate agreement from rd-annotator datables')
parser.add_argument('-d', '--dbfile', default="/Users/nnayak/git_repos/rd-annotator/rdasite/db.sqlite3",
    type=str, help='path to database file')


def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d


def create_connection(db_file):
  """ create a database connection to a SQLite database """
  conn = None
  try:
    conn = sqlite3.connect(db_file)
    conn.row_factory = dict_factory
    return conn
  except Error as e:
    print(e)


def get_agreement(annotation_list):
    num_annotations = len(annotation_list) / 2
    for i in range(num_annotations):
        pass


def sort_label(label):
    return sorted(label.split("|"))


class MatchValues(object):
    EXACT_MATCH = "Exact match"
    PARTIAL_MATCH = "Partial match"
    NOTHING_MATCH = "Matching no-alignment"
    SOME_AND_NONE = "Mismatch some v/s no alignment"
    NO_OVERLAP = "Both aligned; no overlap"


def get_match_value(labels):
   label_1, label_2 = sorted(labels)
   if label_1 == label_2:
       if label_1 == "-1":
           return MatchValues.NOTHING_MATCH
       else:
        return MatchValues.EXACT_MATCH
   elif set(label_1).intersection(set(label_2)):
       return MatchValues.PARTIAL_MATCH
   else:
       if label_1 == "-1" or label_2 == "-1":
           return MatchValues.SOME_AND_NONE
       else:
        return MatchValues.NO_OVERLAP


def main():
    args = parser.parse_args()
    conn = create_connection(args.dbfile)

    c = conn.cursor()

    instances = c.execute(
    ("SELECT DISTINCT review_supernote, rebuttal_supernote, annotator, label, rebuttal_chunk "
     "FROM alignments_alignmentannotation")).fetchall()

    instances = [x for x in instances if not x["annotator"].startswith("test")]

    instance_by_id = collections.defaultdict(list)
    for instance in instances:
        instance_by_id[(instance["review_supernote"],
        instance["rebuttal_supernote"])].append(instance)

    exact_scores = []
    partial_scores = []
    match_types_accumulator = collections.Counter()

    for key, instances in instance_by_id.items():
        labels = collections.defaultdict(list)
        #exact_accumulator = 0
        #partial_accumulator = 0
        for instance in instances:
            if "test" in instance["annotator"]:
                continue
            elif instance["annotator"] == "TJO":
                continue
            labels[instance["rebuttal_chunk"]].append(instance["label"])
        
        for i, l in labels.items():
            assert len(l) == 2
            #exact, partial = get_match_value(l)
            #exact_accumulator += exact
            #partial_accumulator += partial
            value = get_match_value(l)
            if value in [2,4]:
                print(key, i, l)
            match_types_accumulator[get_match_value(l)] += 1

        #exact_scores.append(exact_accumulator/len(labels))
        #partial_scores.append(partial_accumulator/len(labels))

    for k, v in match_types_accumulator.items():
        print(k, v)
    #print(sum(exact_scores)/len(exact_scores))
    #print(sum(partial_scores)/len(partial_scores))


if __name__ == "__main__":
    main()
