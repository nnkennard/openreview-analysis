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


class Participants(object):
  CONFERENCE = "Conference"
  AUTHOR = "Author"
  AC = "AC"
  REVIEWER = "Reviewer"
  REVIEWER_B = "Reviewer B"
  MULTIPLE = "Multiple"
  ANONYMOUS = "Anonymous"
  NAMED = "Named"


def is_official(comment):
  return shorten_author(comment.author) in [Participants.AUTHOR,
      Participants.REVIEWER, Participants.AC, Participants.CONFERENCE]


def shorten_author(author):
  if Participants.AUTHOR in author:
    return Participants.AUTHOR
  elif Participants.REVIEWER in author:
    return Participants.REVIEWER
  elif "Area_Chair" in author:
    return Participants.AC
  elif Participants.CONFERENCE in author:
    return Participants.CONFERENCE
  elif author == "(anonymous)":
    return Participants.ANONYMOUS
  else:
    assert author.startswith("~")
    return Participants.NAMED


def is_reviewer(author):
  return "AnonReviewer" in author


def authorify_sequence(sequence, comment_map):
  return [shorten_author(comment_map[comment_id].author)
      for comment_id in sequence]
  

def get_path_to_node(node, comment_map):
  ancestor_ids = []
  curr_ancestor_id = node
  while True:
    curr_ancestor = comment_map[curr_ancestor_id]
    ancestor_ids.append(curr_ancestor.comment_id)
    curr_ancestor_id = curr_ancestor.parent_id
    if curr_ancestor_id == "None":
      break

  return list(reversed(ancestor_ids))

  
def characterize_path(terminal_node, comment_map):
  nodes_path = get_path_to_node(terminal_node, comment_map)
  full_authors_path = [comment_map[node].author for node in nodes_path]
  authors_path = authorify_sequence(nodes_path, comment_map)

  reviewer_count = 0
  reviewer_roles = {}
  characteristic_path = []

  for node, short_author in zip(nodes_path, authors_path):
    if short_author in [Participants.CONFERENCE, Participants.AUTHOR,
        Participants.AC]:
      if short_author not in characteristic_path:
        characteristic_path.append(short_author)
    else:
      assert short_author == Participants.REVIEWER
      reviewer_name = comment_map[node].author
      if reviewer_name in reviewer_roles:
        continue
      else:
        reviewer_roles[reviewer_name] = reviewer_count
        characteristic_path.append(Participants.REVIEWER + str(reviewer_count))
        reviewer_count += 1

  if len(set(characteristic_path)) > 4:
    characteristic_path = characteristic_path[:3] + [Participants.MULTIPLE]

  assert len(characteristic_path) <= 4

  return tuple(characteristic_path)


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


def count_author_types(node_list, comment_map):
  nodes_counter = collections.Counter(
      [shorten_author(comment_map[node_id].author)
        for node_id in node_list - set(["None"])])
  for k in sorted(nodes_counter.keys()):
    print(k+" "+str(nodes_counter[k]))
  print(" ")


def count_nodes(structure_map):
  parents = set()
  children = set()
  for k, v in structure_map.items():
    children.update(set(v.keys()))
    parents.update(set(v.values()))

  return parents - set(["None"]), children


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

  print("Author type counts (all)")
  parents, children = count_nodes(structure_map) 
  all_nodes = parents.union(children)
  count_author_types(all_nodes, comment_map)
  print("Total number of nodes", len(all_nodes))
  print("Total number of nonparents", len(children - parents))
     

  official_structure_map = {
      forum_id:prune_unofficial(structure, comment_map)
      for forum_id, structure in structure_map.items()}

  parents, children = count_nodes(official_structure_map) 
  all_nodes = parents.union(children)
  print("Author type counts (official)")
  count_author_types(all_nodes, comment_map)
  print("Total number of official nodes", len(all_nodes))
  print("Total number of nonparents", len(children - parents))
  
  characterized_paths = collections.defaultdict(list)
  for terminal_node in children - parents:
    path_to_node = get_path_to_node(terminal_node, comment_map)
    characterized_paths[characterize_path(terminal_node,
      comment_map)].append(path_to_node)

  print(" ")
  print("Path characteristic counts")
  for char_path, paths in characterized_paths.items():
    print("_".join(char_path) + " " + str(len(paths)))



 
if __name__ == "__main__":
  main()
