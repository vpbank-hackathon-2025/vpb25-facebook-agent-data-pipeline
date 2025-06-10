import logging
import os
import pytz
import traceback
from datetime import datetime

from settings.config import settings


def exception_logging(exctype, value, tb):
    """
    Log exception by using the root logger.
    """
    write_val = {'exception_type': exctype.__name__,
                 'message': str(value) + " Traceback: " + str(traceback.format_tb(tb, 10))}
    logger.error(str(write_val))

class CustomFormatter(logging.Formatter):

    green = "\x1b[0;32m"
    grey = "\x1b[38;5;248m"
    yellow = "\x1b[38;5;229m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    blue = "\x1b[38;5;31m"
    white = "\x1b[38;5;255m"
    reset = "\x1b[38;5;15m"
    
    base_format = (f"{grey}%(asctime)s | %(threadName)s | {{level_color}}%(levelname)-3s{grey} | {blue}%(module)s:%(lineno)d{grey} - {white}%(message)s")
    
    FORMATS = {
        logging.INFO: base_format.format(level_color=green),
        logging.WARNING: base_format.format(level_color=yellow),
        logging.ERROR: base_format.format(level_color=red),
        logging.CRITICAL: base_format.format(level_color=bold_red),
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def custom_logger(app_name="APP"):
    logger_r = logging.getLogger(name=app_name)
    tz = pytz.timezone("Asia/Ho_Chi_Minh")  # Set the timezone to Ho_Chi_Minh

    logging.Formatter.converter = lambda *args: datetime.now(tz).timetuple()

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(CustomFormatter())
    
    # File handler
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    log_file_path = os.path.join(logs_dir, "logs.log")
    fh = logging.FileHandler(log_file_path)
    fh.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(threadName)s | %(levelname)-3s | %(module)s:%(lineno)d - %(message)s'
    )
    fh.setFormatter(file_formatter)

    logger_r.setLevel(logging.INFO)
    logger_r.addHandler(ch)
    logger_r.addHandler(fh)
    
    logger_r.info(f"Log file created at: {log_file_path}")

    return logger_r


logger = custom_logger(app_name=settings.app_name)

logger.info('Logger initiated')
