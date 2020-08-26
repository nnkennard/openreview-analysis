import corenlp
import sys
import openreview_lib as orl

ANNOTATORS = "ssplit tokenize".split()

def chunk_text(text):
  return text.split("\n\n")

def main():

  dataset_file = sys.argv[1]

  # get all forums

  with corenlp.CoreNLPClient(annotators=ANNOTATORS, output_format='conll') as corenlp_client:
    datasets = orl.get_datasets(dataset_file, corenlp_client, debug=True)

  
  # patch together long comments

  # chunk

  # insert into tables

  pass


if __name__ == "__main__":
  main()
