from os import listdir
from os.path import isfile, join

from settings import DB_FILE, DATA_DIR
from etl.load_data_to_db import load_file_to_db
from etl.db_operations import create_connection

from logger.app_logger import logger as app_logger
from logger.read_logger import logger as read_logger
from logger.insert_logger import logger as insert_logger

conn = create_connection(dbfile=DB_FILE)

with conn:
    data_file_list = sorted(listdir(DATA_DIR))
    for item in data_file_list:
        item_location = join(DATA_DIR, item)
        if isfile(item_location):
            load_file_to_db(conn, item_location, app_logger, read_logger, insert_logger)
