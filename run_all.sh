#mkdir splits
#python make_train_test_split.py -o splits/ -c iclr19

rm -rf db/
mkdir db
python create_db.py --dbfile db/or.db
python build_or_db.py --dbfile db/or.db --inputfile splits/iclr19_split.json
python example.py --dbfile db/or.db
