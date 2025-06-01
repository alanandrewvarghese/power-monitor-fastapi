import logging
import os
from logging.handlers import RotatingFileHandler

# Ensure log directory exists
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, 'app.log')

# Logging configuration
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')

    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Rotating file handler
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=3)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

# Call this in your main.py before app startup
# logger = setup_logging()
