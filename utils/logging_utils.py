import logging
from bidi.algorithm import get_display
from dateutil.parser import parse
import unicodedata
import os
import re

from dotenv import load_dotenv
load_dotenv()

DEBUG = os.getenv("DEBUG", "false").lower() == "true"

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


def filter_post_cutoff_docs(doc_ids, source_df, cutoff_datetime, source_label=""):
    filtered = []

    for moj in doc_ids:
        row = source_df[source_df["mojId"] == moj]
        if not row.empty:
            raw_date = row.iloc[0].get("statusDate")
            if raw_date:
                try:
                    parsed = parse(raw_date).replace(tzinfo=None)
                    if parsed <= cutoff_datetime:
                        filtered.append(moj)
                    else:
                        log_and_print(f"✅ Skipping [{source_label}] {moj}, post-CUTOFF: {parsed}", "debug")
                except Exception as e:
                    log_and_print(f"⚠️ Failed to parse statusDate for mojId {moj}: {e}", "warning")
                    filtered.append(moj)
            else:
                filtered.append(moj)
        else:
            filtered.append(moj)

    return filtered



def normalize_whitespace(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'\s+', ' ', text.strip())


def log_and_print(message, level="info", ansi_format=None, is_hebrew=False, indent=0):
    """
    Log a message and print it with optional ANSI formatting and indentation.
    Skips logging/printing unless DEBUG is enabled.
    """
    if not DEBUG:
        return

    # Normalize Hebrew text for console, but keep original for log
    if is_hebrew:
        console_message = normalize_hebrew(message)
        log_message = message
    else:
        console_message = message
        log_message = message

    # Apply ANSI formatting
    if ansi_format:
        console_message = f"{ansi_format}{console_message}{RESET}"

    # Indent
    console_message = f"{' ' * indent}{console_message}"

    # Console print
    print(console_message)

    # File log (no ANSI or indent)
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
