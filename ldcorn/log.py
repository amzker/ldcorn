import logging
import sys

class ColorFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[94m',      # Blue
        'INFO': '\033[92m',       # Green
        'WARNING': '\033[93m',    # Yellow
        'ERROR': '\033[91m',      # Red
        'CRITICAL': '\033[1;91m', # Bold Red
    }
    RESET = '\033[0m'
    
    def format(self, record):
        level_name = f"{record.levelname}:"
        prefix = f"{self.COLORS.get(record.levelname, self.RESET)}{level_name:<10}\033[0m [ldcorn]"
        log_fmt = f"{prefix} %(message)s"
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

logger = logging.getLogger("ldcorn")
logger.setLevel(logging.INFO)

if not logger.handlers:
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(ColorFormatter())
    logger.addHandler(ch)
