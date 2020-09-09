import collections
import json
import openreview

from recordtype import recordtype
from tqdm import tqdm

import lib.openreview_db as ordb


class Conference(object):
  iclr19 = "iclr19"
  iclr20 = "iclr20"
  ALL = [iclr19, iclr20]

INVITATION_MAP = {
    Conference.iclr19:'ICLR.cc/2019/Conference/-/Blind_Submission',
    Conference.iclr20:'ICLR.cc/2020/Conference/-/Blind_Submission',
}

CommentRow = recordtype("CommentRow",
  """forum_id split parent_supernote comment_supernote
  timestamp author author_type or_text_type
  comment_type original_id chunk_idx sentence_idx
  token_idx token""", default=None)

def get_datasets(dataset_file, corenlp_client, conn, debug=False):
  """Given a dataset file, enter it into a sqlite3 database. 

    Args:
      dataset_file: json file produced by make_train_test_split.py
      corenlp_client: stanford-corenlp client with at least ssplit, tokenize
      conn: connection to a sqlite3 database
      debug: set in order to truncate to 50 examples

  """
  with open(dataset_file, 'r') as f:
    examples = json.loads(f.read())

  guest_client = openreview.Client(baseurl='https://api.openreview.net')
  conference = examples["conference"]
  assert conference in Conference.ALL 

  datasets = {}
  for set_split, forum_ids in examples["id_map"].items():
    build_dataset(forum_ids, guest_client, corenlp_client, conn,
                      conference, set_split, debug)


    

def get_nonorphans(parents):
  """Remove children whose parents have been deleted for some reason.
  
    Args:
      parents: A map from the id of each child comment to the id of its parent

    Returns:
      A new map that only include non-deleted comments

    This addresses the problem of orphan subtrees caused by deleted comments.
  """

  # TODO(nnk): Why does this work??

  children = collections.defaultdict(list)
  for child, parent in parents.items():
    children[parent].append(child)

  descendants = sum(children.values(), [])
  ancestors = children.keys()
  nonchildren = set(ancestors) - set(descendants)
  orphans = sorted(list(nonchildren - set([None])))

  while orphans:
    # Add orphan's children as their parent's children instead
    current_orphan = orphans.pop()
    orphans += children[current_orphan]
    del children[current_orphan]

  new_parents = {}
  # Create a new map in the same format but only with non-orphan children
  for parent, child_list in children.items():
    for child in child_list:
      assert child not in new_parents
      new_parents[child] = parent

  return new_parents 


def flatten_signature(note):
  """Map signature field to a deterministic string.
     Tbh it looks like most signatures are actually only 1 author long..
  """
  assert len(note.signatures) == 1
  return  "|".join(sorted(sig.split("/")[-1] for sig in note.signatures))


def restructure_forum(forum_structure, note_map):
  """Merge parent-child comment pairs that are intended to be continuations.
    Args:
      forum_structure: The structure of one forum in {parent:child} format
      note_map: A map from the note ids to the note in openreview.Note format

    Returns:
      equiv_classes: map from top comment to a list of all its continuation
      comments
      equiv_map: map from each comment to the top parent of the continuation it
      is a part of (map to themselves if they are not a continuation)
  """

  notes = set(
      forum_structure.keys()).union(set(
        forum_structure.values())) - set([None])

  # Each equivalence class is named with the top comment, and contains all
  # supposed continuations (direct descendants where the authors of all the
  # descendants are the same as the top comment)
  equiv_classes = {
      note_id:[note_id] for note_id in notes 
    }

  # Order by creation date
  ordered_children = sorted(forum_structure.keys(), key=lambda x:
      note_map[x].tcdate)


  for child in ordered_children:
    parent = forum_structure[child]
    if parent is None:
      continue
    child_note = note_map[child]
    parent_note = note_map[parent]
    # Check authors
    if flatten_signature(child_note) == flatten_signature(parent_note):
      for k, v in equiv_classes.items():
        if parent in v:
          # Merge these two equivalence classes
          equiv_classes[k]+= list(equiv_classes[child])
          del equiv_classes[child]
          break
   
  # Maps each merged comment's id to the top parent id of its chain
  equiv_map = {None:"None"}
  for supernote, subnotes in equiv_classes.items():
    for subnote in subnotes:
      equiv_map[subnote] = supernote


  return equiv_classes, equiv_map


TOKEN_INDEX = 1  # Index of token field in conll output


def get_tokens_from_tokenized(tokenized):
  """Extract token sequences from CoreNLP output.
    Args:
      tokenized: The conll-formatted output from a CoreNLP server with ssplit
      and tokenize
    Returns:
      sentences: A list of sentences in which each sentence is a list of tokens.
  """
  lines = tokenized.split("\n")
  sentences = []
  current_sentence = []
  for line in lines:
    if not line.strip(): # Blank links indicate the end of a sentence
      if current_sentence:
        sentences.append(current_sentence)
      current_sentence = []
    else:
      current_sentence.append(line.split()[TOKEN_INDEX])
  return sentences 


def get_tokenized_chunks(corenlp_client, text):
  """Runs tokenization using a CoreNLP client.
    Args:
      corenlp_client: a corenlp client with at least ssplit and tokenize
      text: raw text
    Returns:
      A list of chunks in which a chunk is a list of sentences and a sentence is
      a list of tokens.
  """
  chunks = text.split("\n\n")
  return [get_tokens_from_tokenized(corenlp_client.annotate(chunk))
      for chunk in chunks]


def get_info(note_id, note_map):
  """Gets relevant note metadata.
    Args:
      note_id: the note id from the openreview.Note object
      note_map: a map from note ids to relevant openreview.Note objects
    Returns:
      The creation date and the authors of the note.
   """
  note = note_map[note_id]
  if note.replyto is None:
    return "root", "", note.tcdate, flatten_signature(note)
  else:
    for text_type in ["review", "comment", "withdrawal confirmation",
    "metareview"]:
      if text_type in note.content:
        return text_type, note.content[text_type], note.tcdate, flatten_signature(note)
    assert False

def get_forum_map(forums, or_client):
  """Retrieve notes and structure for all forums in this Dataset.

  Args:
    forums: list of forum ids to retrieve
    or_client: openreview client

  Returns:
    root_map: map from forum id (root of comment tree) to forum structure
    note_map: map from note id to openreview.Note object
  """
  root_map = {}
  note_map = {}
  for forum_id in tqdm(forums):
    forum_structure, forum_note_map = get_forum_structure(forum_id,
        or_client)
    root_map[forum_id] = forum_structure
    note_map.update(forum_note_map)

  return root_map, note_map


def get_forum_structure(forum_id, or_client):
  """Retrieves structure and notes of a forum.

  Args:
    forum_id: id of forum to retrieve
    or_client: openreview client

  Returns:
    parents: forum structure in {child_id:parent_id} format
    note_map: map from note ids to openreview.Note objects
  """
  notes = or_client.get_notes(forum=forum_id)
  naive_note_map = {note.id:note for note in notes} # includes orphans
  naive_parents = {note.id:note.replyto for note in notes}

  parents = get_nonorphans(naive_parents)
  available_notes = set(parents.keys()).union(
      set(parents.values())) - set([None])
  note_map = {note:naive_note_map[note] for note in available_notes}

  return parents, note_map


def build_dataset(forum_ids, or_client, corenlp_client, conn,
             conference, set_split, debug):
  """Initializes and dumps to sqlite3 database.

  Args:
    forum_ids: list of forum ids (top comment ids) in the dataset
    or_client: openreview client
    corenlp_client: stanford-corenlp client for tokenization
    conn: sqlite3 connection
    conference: conference name
    set_split: train/dev/test
    debug: if True, truncate example list
  """
  submissions = openreview.tools.iterget_notes(
        or_client, invitation=INVITATION_MAP[conference])
  forums = [n.forum for n in submissions if n.forum in forum_ids]
  if debug:
    forums = forums[:10]

  forum_map, note_map = get_forum_map(forums, or_client)
  for forum_id, forum_struct in tqdm(forum_map.items()):
    equiv_classes, equiv_map = restructure_forum(forum_struct, note_map)

    forum_rows = []

    # Adding tokenized text of each comment to text table
    for supernote, subnotes in equiv_classes.items():
      text_type, _, timestamp, supernote_author = get_info(supernote, note_map)
      supernote_as_dict = CommentRow(
          forum_id, set_split, equiv_map[forum_struct[supernote]], supernote, 
          timestamp, supernote_author, "~AUTHOR_TYPE", text_type, 
          "~COMMENT_TYPE")._asdict()
      chunk_offset = 0
      for subnote in subnotes:
        text_type, text, _, subnote_author = get_info(subnote, note_map)
        assert subnote_author == supernote_author
        text = "Hahaha here is a sentence for now. And another one; I hope this works."
        chunks = get_tokenized_chunks(corenlp_client, text)
        
        for chunk_idx, chunk in enumerate(chunks):
          for sentence_idx, sentence in enumerate(chunk):
            for token_idx, token in enumerate(sentence):
              new_row = CommentRow(supernote_as_dict)
              new_row.chunk_idx = chunk_idx + chunk_offset
              new_row.sentence_idx = sentence_idx
              new_row.token_idx = token_idx
              new_row.token = token
              forum_rows.append(new_row)

        chunk_offset += len(chunks)

      for row in forum_rows:
        print(row)



