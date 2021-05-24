import collections
import json
import math 
import sys


WINDOW = 5
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
  matches = []
  for k1, v1 in hashes_1.items():
    for k2, v2 in hashes_2.items():
      if v1 == v2:
        matches.append((k1, k2))
  return sorted(matches)

def find_lcs(review_tokens, rebuttal_tokens, review_offset, rebuttal_offset):
  review_idx = review_offset
  rebuttal_idx = rebuttal_offset
  while review_tokens[review_idx] == rebuttal_tokens[rebuttal_idx]:
    review_idx += 1
    rebuttal_idx += 1
    if review_idx == len(review_tokens) or rebuttal_idx == len(rebuttal_tokens):
      break
  return review_tokens[review_offset:review_idx]


Location = collections.namedtuple("Location",
    "supernote chunk_idx token_idx".split())


Match = collections.namedtuple("Match",
    "review_location rebuttal_location lcs".split())


def find_matches(rebuttal, review):
  (review_id, review_chunks), = review.items()
  (rebuttal_id, rebuttal_chunks), = rebuttal.items()

  matches = []
  for i, review_chunk in enumerate(review_chunks):
    for j, rebuttal_chunk in enumerate(rebuttal_chunks):
      review_chunk_tokens  = sum(review_chunk, [])
      rebuttal_chunk_tokens  = sum(rebuttal_chunk, [])
      match_offsets = karp_rabin(review_chunk_tokens, rebuttal_chunk_tokens)
      if match_offsets:
        for (review_offset, rebuttal_offset) in match_offsets:
          lcs = find_lcs(review_chunk_tokens, rebuttal_chunk_tokens,
            review_offset, rebuttal_offset)
          if matches and " ".join(lcs) in matches[-1].lcs:
            continue
          else:
            matches.append(Match(Location(review_id, i, review_offset),
              Location(rebuttal_id, j, rebuttal_offset), " ".join(lcs)))
  return matches




