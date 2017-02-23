import logging


def init_logger(level):
    levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    logger = logging.getLogger("spark-stat-analyzer")
    logger_level = levels.get(level.upper(), logging.WARNING)
    logger.setLevel(logger_level)
    steam_handler = logging.StreamHandler()
    steam_handler.setLevel(logger_level)
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)5s] [%(process)5s] [%(name)10s] %(message)s')
    steam_handler.setFormatter(formatter)
    logger.addHandler(steam_handler)


def get_logger():
    return logging.getLogger("spark-stat-analyzer")