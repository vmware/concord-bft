import logging


def get_logger(name=None):
    # create the logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', '%d-%m-%Y %H:%M:%S')

    # Create console handler and set level to debug
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    # add handler to logger
    stream_handler.setFormatter(formatter)

    # add handler to logger
    logger.addHandler(stream_handler)

    return logger
