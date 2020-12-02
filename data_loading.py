from os import listdir
from os.path import isfile, join

from settings import DB_FILE, DATA_DIR
from etl.load_data_to_db import load_file_to_db
from etl.db_operations import create_connection


conn = create_connection(dbfile=DB_FILE)

with conn:
    for item in listdir(DATA_DIR):
        item_location = join(DATA_DIR, item)
        if isfile(item_location):
            load_file_to_db(conn, item_location)
