import logging
from settings import INSERT_LOG_FILE 

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# log file
logfile = INSERT_LOG_FILE

# set handlers
file_handler = logging.FileHandler(logfile, mode="w")
file_handler.setLevel(logging.INFO)

# formatters
file_format = logging.Formatter("*** %(asctime)s - %(filename)s: ***\n\t%(levelname)s: %(message)s")

# set formatters on handlers
file_handler.setFormatter(file_format)

# add handlers to logger
logger.addHandler(file_handler)






