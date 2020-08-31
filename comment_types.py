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


THREAD_PROFILES = {
    # Thread profiles are for two participants
    "review-rebuttal":  ["root", "reviewer", "author"]
    }

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

def is_by_conference(comment):
  return comment.author == "ICLR.cc/2019/Conference"

def shorten_author(comment):
  if is_by_author(comment):
    return Participants.AUTHOR 
  elif is_by_reviewer(comment):
    return Participants.REVIEWER
  elif is_by_ac(comment):
    return Participants.AC
  elif is_by_anonymous(comment):
    anon_dsds
    return "Anon"
  elif is_by_conference(comment):
    return Participants.CONFERENCE
  else:
    named_dsds
    return "Named"


def is_official(comment):
  return (is_by_ac(comment) or is_by_reviewer(comment)
      or is_by_author(comment) or "Conference" in comment.author)


class Participants(object):
  CONFERENCE = "Conference"
  AUTHOR = "Author"
  AC = "AC"
  REVIEWER = "Reviewer"
  REVIEWER_B = "Reviewer B"
  MULTIPLE = "Multiple"


def shorten_author(author):
  if Participants.AUTHOR in author:
    return Participants.AUTHOR
  elif Participants.REVIEWER in author:
    return Participants.REVIEWER
  elif "Area_Chair" in author:
    return Participants.AC
  elif Participants.CONFERENCE in author:
    return Participants.CONFERENCE
  else:
    dsds


def is_reviewer(author):
  return "AnonReviewer" in author

def shorten_sequence(sequence):
  new_sequence = []
  for author in sequence:
    if is_reviewer(author):
      if Participants.REVIEWER in new_sequence:
        new_sequence.append(Participants.REVIEWER_B)
      else:
        new_sequence.append(Participants.REVIEWER)
    else:
      new_sequence.append(shorten_author(author))
  return new_sequence

  
def characterize_path(terminal_node, comment_map):
  ancestors = []
  curr_ancestor = terminal_node
  while not curr_ancestor == "None":
    ancestors.append(comment_map[curr_ancestor].author)
    curr_ancestor = comment_map[curr_ancestor].parent_id

  sequence = list(reversed(ancestors))
  initiator = sequence[1]


  participants = sequence[:2]
  for next_participant in sequence[2:]:
    if next_participant not in participants:
      participants.append(next_participant)

  if len(participants) > 3:
    final = shorten_sequence(participants[:3]) + [Participants.MULTIPLE]
  else:
    final = shorten_sequence(participants[:4])

  #print(" ".join(participants))
  #print(" ".join(final))
  #print("-" * 80)

  return final




def prune_unofficial(parents, comment_map):
  children = collections.defaultdict(list)
  for child, parent in parents.items():
    children[parent].append(child)

  official_children = collections.defaultdict(list)

  queue = ["None"]
  while queue:
    curr_id = queue.pop(0)
    for child in children[curr_id]:
      if is_official(comment_map[child]):
        queue.append(child)
        official_children[curr_id].append(child)

  official_parents = {}
  for parent, children in official_children.items():
    for child in children:
      official_parents[child] = parent

  return official_parents


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

  all_nodes = set()
  for k, v in structure_map.items():
    all_nodes.update(set(v.keys()))
    all_nodes.update(set(v.values()))
  print("Total number of nodes", len(all_nodes))
  

  official_structure_map = {
      forum_id:prune_unofficial(structure, comment_map)
      for forum_id, structure in structure_map.items()}

  all_nodes = set()
  parent_nodes = set()
  child_nodes = set()
  for k, v in official_structure_map.items():
    child_nodes.update(set(v.keys()))
    parent_nodes.update(set(v.values()))
  print("Total number of official nodes", len(parent_nodes.union(child_nodes)))

  non_parents = child_nodes - parent_nodes

  print("Total number of paths", len(non_parents))

  for terminal_node in non_parents:
    print(" ".join(characterize_path(terminal_node, comment_map)))



 
if __name__ == "__main__":
  main()
