import collections
import sqlite3
import argparse
import openreview_db as ordb

parser = argparse.ArgumentParser(
    description='Example for accessing OpenReview data')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class CommentTypes(object):
  ROOT = "root"
  REVIEW = "review"
  METAREVIEW = "probably metareview"
  METARESPONSE = "probably metaresponse"
  ANON_TOP = "anonymous top"
  NAMED_TOP = "named top"
  REBUTTAL = "rebuttal"
  INTER_REVIEWER = "inter-reviewer"
  AUTHOR_TO_REVIEWER = "author-to-reviewer"
  REVIEWER_TO_AUTHOR = "reviewer-to-author"
  AC_INTERJECTION = "ac interjection"
  ANON_INTERJECTION = "anonymous interjection"
  NAMED_INTERJECTION = "named interjection"
  UNOFFICIAL_CONVERSATION = "unofficial conversation"

def is_by_author(comment):
  if "Author" in comment.author:
    if "Authors" not in comment.author:
      dsds
    else:
      return True
  return False

def is_by_reviewer(comment):
  return "AnonReviewer" in comment.author

def is_by_anonymous(comment):
  return comment.author == "(anonymous)"

def is_by_ac(comment):
  return "Area_Chair" in comment.author

def is_official(comment):
  return is_by_ac(comment) or is_by_reviewer(comment) or is_by_author(comment)

def comment_type_trail(child, comment_map, comment_type_map):
  ancestors = []
  curr_ancestor = child
  while not curr_ancestor == "None":
    if curr_ancestor in comment_type_map:
      ancestors.append(comment_type_map[curr_ancestor])
    else:
      ancestors.append(comment_map[curr_ancestor].author)
    curr_ancestor = comment_map[curr_ancestor].parent_id

  return "\t".join(reversed(ancestors))

def check_thread_participants(child, comment_map):
  ancestors = []
  curr_ancestor = child
  while not comment_map[curr_ancestor].parent_id == "None":
    ancestors.append(comment_map[curr_ancestor].author)
    curr_ancestor = comment_map[curr_ancestor].parent_id

  print("\t".join([
    str(len(ancestors)), "comments", str(len(set(ancestors))),
    "participants"]))


def main():

  args = parser.parse_args()

  conn = ordb.create_connection(args.dbfile)
  conn.row_factory = dict_factory
  cur = conn.cursor()
  cur.execute("SELECT * FROM text WHERE split=?", ("train",))
  rows = cur.fetchall()
  text_data = ordb.crunch_text_rows(rows)


  cur.execute("SELECT * FROM structure WHERE split=?", ("train",))
  rows = cur.fetchall()

  structure_map, comment_map = ordb.crunch_structure_rows(rows)


  comment_type_map = {}
  comment_type_accumulator = {}
 

  # Get all root commments
  for forum, structure in structure_map.items():
    for child, parent in structure.items():
      assert child not in comment_type_map
      if parent == "None":
        comment_type_accumulator[child] = CommentTypes.ROOT

  previous_level = dict(comment_type_accumulator)
  comment_type_map.update(comment_type_accumulator)
  comment_type_accumulator = {}

  for forum, structure in structure_map.items():
    for child, parent in structure.items():
      if parent in previous_level:
        comment = comment_map[child]
        if is_by_reviewer(comment):
          comment_type_accumulator[child] = CommentTypes.REVIEW
        elif is_by_author(comment):
          comment_type_accumulator[child] = CommentTypes.METARESPONSE
        elif is_by_ac(comment):
          comment_type_accumulator[child] = CommentTypes.METAREVIEW
        elif is_by_anonymous(comment):
          comment_type_accumulator[child] = CommentTypes.ANON_TOP
        else:
          assert comment.author.startswith("~")
          comment_type_accumulator[child] = CommentTypes.NAMED_TOP

  assert not set(
      previous_level.keys()).intersection(comment_type_accumulator.keys())

  previous_level = dict(comment_type_accumulator)
  comment_type_map.update(comment_type_accumulator)
  comment_type_accumulator = {}

  for forum, structure in structure_map.items():
    for child, parent in structure.items():
      if parent in previous_level:
        comment = comment_map[child]
        parent_comment = comment_map[parent]
        if is_by_reviewer(comment):
          if is_by_reviewer(parent_comment):
            comment_type_accumulator[child] = CommentTypes.INTER_REVIEWER
          elif is_by_author(parent_comment):
            comment_type_accumulator[child] = CommentTypes.REVIEWER_TO_AUTHOR
        elif is_by_author(comment):
          if is_by_reviewer(parent_comment):
            comment_type_accumulator[child] = CommentTypes.REBUTTAL
          elif is_by_author(parent_comment):
           assert False 
        elif is_by_ac(comment):
          assert not is_by_ac(parent_comment)
          comment_type_accumulator[child] = CommentTypes.AC_INTERJECTION
        elif is_by_anonymous(comment):
          if is_official(parent_comment):
            comment_type_accumulator[child] = CommentTypes.ANON_INTERJECTION
          else:
            comment_type_accumulator[child] = CommentTypes.UNOFFICIAL_CONVERSATION
        else:
          assert comment.author.startswith("~")
          if is_official(parent_comment):
            comment_type_accumulator[child] = CommentTypes.NAMED_INTERJECTION
          else:
            comment_type_accumulator[child] = CommentTypes.UNOFFICIAL_CONVERSATION

  assert not set(
      previous_level.keys()).intersection(comment_type_accumulator.keys())
          
  previous_level = dict(comment_type_accumulator)
  comment_type_map.update(comment_type_accumulator)
  comment_type_accumulator = {}

  outstanding_children = []


  all_parents, all_children = set(), set()

  for structure in structure_map.values():
    all_parents.update(structure.values())
    all_children.update(structure.keys())

  for forum, structure in structure_map.items():
    for child, parent in structure.items():
      if child not in comment_type_map:
        outstanding_children.append(child)
        #print(comment_type_trail(child, comment_map, comment_type_map))


  non_parent_children = set(outstanding_children) - set(all_parents)
  for c in non_parent_children:
    check_thread_participants(c, comment_map)
  
if __name__ == "__main__":
  main()



