import logging
from settings import APP_LOG_FILE

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# log file
logfile = APP_LOG_FILE

# set handlers
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler(logfile, mode="a")


# formatters
stream_format = \
    logging.Formatter("*** %(name)s: %(filename)s: ***\n  %(levelname)s: %(message)s")

file_format = logging.Formatter("*** %(asctime)s - %(filename)s: ***\n  %(levelname)s: %(message)s")

# set formatters on handlers
stream_handler.setFormatter(stream_format)
file_handler.setFormatter(file_format)

# add handlers to logger
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


