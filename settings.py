import os

# SETTINGS
## ROOT
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

## Data
DATA_DIR_NAME = "data"
DATA_DIR = os.path.join(ROOT_DIR, DATA_DIR_NAME)

## DB
DB_DIR_NAME = "db"
DB_FILE_NAME = "prices.db"
DB_DIR = os.path.join(ROOT_DIR, DB_DIR_NAME)
DB_FILE = os.path.join(ROOT_DIR, DB_DIR_NAME, DB_FILE_NAME)

TABLE_NAME = "hourly_prices"

## Logs
LOG_DIR_NAME = "logs"
APP_LOG_FILE_NAME = "app.log"
READ_LOG_FILE_NAME = "read_data.log"
INSERT_LOG_FILE_NAME = "insert_data.log"

LOG_DIR = os.path.join(ROOT_DIR, LOG_DIR_NAME)

APP_LOG_FILE = os.path.join(ROOT_DIR, LOG_DIR_NAME, APP_LOG_FILE_NAME)
READ_LOG_FILE = os.path.join(ROOT_DIR, LOG_DIR_NAME, READ_LOG_FILE_NAME)
INSERT_LOG_FILE = os.path.join(ROOT_DIR, LOG_DIR_NAME, INSERT_LOG_FILE_NAME)





