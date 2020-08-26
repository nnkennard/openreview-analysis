import sys
import openreview_lib as orl

def main():

  dataset_file = sys.argv[1]

  # get all forums

  datasets = orl.get_datasets(dataset_file, debug=True)

  print(datasets)

  # split forums into train/dev/test

  # patch together long comments

  # chunk

  # insert into tables

  pass


if __name__ == "__main__":
  main()
