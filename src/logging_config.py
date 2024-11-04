import logging

# ANSI escape codes for light blue color
LIGHT_BLUE = "\033[94m"
RESET_COLOR = "\033[0m"


class LightBlueFormatter(logging.Formatter):
    def format(self, record):
        # Wrap the message with the light blue color code
        record.msg = f"{LIGHT_BLUE}{record.msg}{RESET_COLOR}"
        return super().format(record)


def setup_logging():
    """Sets up the logging configuration to output to the console only."""
    # Configure the logging
    logging.basicConfig(
        level=logging.INFO,  # Set the default logging level
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Log message format
        handlers=[
            logging.StreamHandler()  # Log to console
        ]
    )

    # Set the formatter for the root logger
    for handler in logging.getLogger().handlers:
        handler.setFormatter(LightBlueFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
