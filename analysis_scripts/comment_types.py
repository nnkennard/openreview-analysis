import collections
import json
import sqlite3
import argparse
import openreview_db as ordb

parser = argparse.ArgumentParser(
    description='Example for accessing OpenReview data')
parser.add_argument('-d', '--dbfile', default="db/or.db",
    type=str, help='path to database file')


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
  cur = conn.cursor()
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
  
 
if __name__ == "__main__":
  main()
