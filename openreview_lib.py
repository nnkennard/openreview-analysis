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


def get_datasets(dataset_file, corenlp_client, debug=False):
  with open(dataset_file, 'r') as f:
    examples = json.loads(f.read())

  guest_client = openreview.Client(baseurl='https://api.openreview.net')
  conference = examples["conference"]
  assert conference in Conference.ALL 

  datasets = {}
  for set_split, forum_ids in examples["id_map"].items():
    dataset = Dataset(forum_ids, guest_client, corenlp_client, conference, set_split, debug)
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


def flatten_signature(note):
  return  "|".join(sorted(note.signatures))


def restructure_forum(forum_structure, note_map):
  notes = set(
      forum_structure.keys()).union(set(
        forum_structure.values())) - set([None])

  equiv_classes = {
      note_id:[note_id] for note_id in notes 
    }

  ordered_children = sorted(forum_structure.keys(), key=lambda x:
      note_map[x].tcdate)

  for child in ordered_children:
    parent = forum_structure[child]
    if parent is None:
      continue
    child_note = note_map[child]
    parent_note = note_map[parent]
    if flatten_signature(child_note) == flatten_signature(parent_note):
      for k, v in equiv_classes.items():
        if parent in v:
          equiv_classes[k]+= list(equiv_classes[child])
          del equiv_classes[child]
          break
   
  return equiv_classes


TOKEN_INDEX = 1  # Index of token field in conll output

def get_tokens_from_tokenized(tokenized):
  lines = tokenized.split("\n")
  sentences = []
  current_sentence = []
  for line in lines:
    if not line.strip():
      if current_sentence:
        sentences.append(current_sentence)
      current_sentence = []
    else:
      current_sentence.append(line.split()[TOKEN_INDEX])
  return sentences 


def get_tokenized_chunks(client, text):
  chunks = text.split("\n\n")
  for chunk in chunks:
    tokens = get_tokens_from_tokenized(client.annotate(chunk))
    print(tokens)


def get_type_and_text(note):
  if note.replyto is None:
    return "root", ""
  else:
    for text_type in ["review", "comment", "withdrawal confirmation",
    "metareview"]:
      if text_type in note.content:
        return text_type, note.content[text_type]
    assert False


  
class Dataset(object):
  def __init__(self, forum_ids, or_client, corenlp_client, conference, set_split, debug):
    submissions = openreview.tools.iterget_notes(
          or_client, invitation=INVITATION_MAP[conference])
    self.forums = [n.forum for n in submissions if n.forum in forum_ids]
    if debug:
      self.forums = self.forums[:50]
    self.or_client = or_client
    self.forum_map, self.note_map = self._get_forum_map()
    for forum_struct in self.forum_map.values():
      new_structure = restructure_forum(forum_struct, self.note_map)
      for supernote, subnotes in new_structure.items():
        for subnote in subnotes:
          text_type, text = get_type_and_text(self.note_map[subnote])
          chunks = get_tokenized_chunks(corenlp_client, text)


  def supernode_transform(self):
    pass


  def _get_forum_map(self):
    root_map = {}
    note_map = {}
    for forum_id in tqdm(self.forums):
      forum_structure, forum_note_map = self._get_forum_structure(forum_id)
      root_map[forum_id] = forum_structure
      note_map.update(forum_note_map)

    return root_map, note_map


  def _get_forum_structure(self, forum_id):
    notes = self.or_client.get_notes(forum=forum_id)
    naive_note_map = {note.id:note for note in notes}
    naive_parents = {note.id:note.replyto for note in notes}

    parents = get_nonorphans(naive_parents)
    available_notes = set(parents.keys()).union(
        set(parents.values())) - set([None])
    note_map = {note:naive_note_map[note] for note in available_notes}

    return parents, note_map

