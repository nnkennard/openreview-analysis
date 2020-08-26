import collections
import json
import openreview
from tqdm import tqdm

class Conference(object):
  iclr19 = "iclr19"
  iclr20 = "iclr20"
  ALL = [iclr19, iclr20]

INVITATION_MAP = {
    Conference.iclr19:'ICLR.cc/2019/Conference/-/Blind_Submission',
    Conference.iclr20:'ICLR.cc/2020/Conference/-/Blind_Submission',
}


def get_datasets(dataset_file, debug=False):
  with open(dataset_file, 'r') as f:
    examples = json.loads(f.read())

  guest_client = openreview.Client(baseurl='https://api.openreview.net')
  conference = examples["conference"]
  assert conference in Conference.ALL 

  datasets = {}
  for set_split, forum_ids in examples["id_map"].items():
    dataset = Dataset(forum_ids, guest_client, conference, set_split, debug)
    datasets[set_split] = dataset

  return datasets

def get_nonorphans(parents):
  """Remove children whose parents have been deleted for some reason."""

  children = collections.defaultdict(list)
  for child, parent in parents.items():
    children[parent].append(child)

  descendants = sum(children.values(), [])
  ancestors = children.keys()
  nonchildren = set(ancestors) - set(descendants)
  orphans = sorted(list(nonchildren - set([None])))

  while orphans:
    current_orphan = orphans.pop()
    orphans += children[current_orphan]
    del children[current_orphan]

  new_parents = {}
  for parent, child_list in children.items():
    for child in child_list:
      assert child not in new_parents
      new_parents[child] = parent

  return new_parents 


class Dataset(object):
  def __init__(self, forum_ids, client, conference, set_split, debug):
    submissions = openreview.tools.iterget_notes(
          client, invitation=INVITATION_MAP[conference])
    self.forums = [n.forum for n in submissions if n.forum in forum_ids]
    print(len(forum_ids), len(self.forums))
    self.client = client
    self.forum_map, self.node_map = self._get_forum_map()


  def _get_forum_map(self):
    root_map = {}
    note_map = {}
    for forum_id in tqdm(self.forums):
      forum_structure, forum_note_map = self._get_forum_structure(forum_id)
      root_map[forum_id] = forum_structure
      note_map.update(forum_note_map)

    return root_map, note_map


  def _get_forum_structure(self, forum_id):
    notes = self.client.get_notes(forum=forum_id)
    naive_note_map = {note.id:note for note in notes}
    naive_parents = {note.id:note.replyto for note in notes}

    parents = get_nonorphans(naive_parents)
    available_notes = set(parents.keys()).union(
        set(parents.values())) - set([None])
    note_map = {note:naive_note_map[note] for note in available_notes}

    return parents, note_map

