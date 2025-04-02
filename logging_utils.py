import logging
from bidi.algorithm import get_display
import unicodedata

# ANSI escape codes for bold and colored formatting
BOLD_YELLOW = '\033[1;33m'
BOLD_GREEN = '\033[1;32m'
BOLD_RED = '\033[1;31m'
RESET = '\033[0m'

# Configure logging
def setup_logging(log_file='application.log'):
    logging.basicConfig(
        filename=log_file,
        filemode='w',
        level=logging.INFO,
        format='%(message)s',  # Only log the message itself
        encoding='utf-8'
    )
    return logging.getLogger()

logger = setup_logging()
def log_and_print(message, level="info", ansi_format=None, is_hebrew=False, indent=0):
    """
    Log a message and print it with optional ANSI formatting and indentation.
    If the message contains Hebrew, apply RTL normalization for console output only.
    """
    # Normalize Hebrew text for console, but keep original for log
    if is_hebrew:
        console_message = normalize_hebrew(message)
        log_message = message  # Original logical order for logging
    else:
        console_message = message
        log_message = message

    # Apply ANSI formatting to the console output
    if ansi_format:
        console_message = f"{ansi_format}{console_message}{RESET}"

    # Apply indentation
    console_message = f"{' ' * indent}{console_message}"

    # Print to the console
    print(console_message)

    # Log to the file without ANSI formatting or indentation
    if level.lower() == "info":
        logger.info(log_message)
    elif level.lower() == "warning":
        logger.warning(log_message)
    elif level.lower() == "error":
        logger.error(log_message)
    elif level.lower() == "debug":
        logger.debug(log_message)


def normalize_hebrew(text):
    """Normalize and format Hebrew text for proper RTL display."""
    if not text:
        return text
    return get_display(unicodedata.normalize("NFKC", text.strip()))
