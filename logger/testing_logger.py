import logging

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# set handlers
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)


# formatters
stream_format = \
    logging.Formatter("*** %(name)s: %(filename)s: ***\n%(levelname)s: %(message)s")

# set formatters on handlers
stream_handler.setFormatter(stream_format)

# add handlers to logger
logger.addHandler(stream_handler)


