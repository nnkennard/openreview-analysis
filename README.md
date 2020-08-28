# openreview-analysis

First, create a virtualenv (or alternative)

```
virtualenv --python `which python3` ora_ve
source ora_ve/bin/activate
python -m pip install -r requirements.txt
```


Create a file with train-test splits of the conference you are interested in (`iclr19` or `iclr20`, add a new one to openreview_lib.py if necessary)

```
mkdir splits
python make_train_test_split.py -o splits/ -c iclr19
```

Create a directory for database files and create a new database
```
mkdir db
python create_db.py --dbfile db/or.db
```

Populate the tables with conference data
```
python build_or_db.py --dbfile db/or.db --dataset splits/iclr19_split.json
```
