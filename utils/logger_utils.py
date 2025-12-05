from loguru import logger

class PrefixedLogger:
    """Wrapper around loguru logger that adds a prefix to all messages"""
    
    def __init__(self, prefix: str, width: int = 12):
        # Pad prefix to fixed width (left-aligned by default)
        # Use < for left-align, > for right-align, ^ for center
        self.prefix = f"{prefix}"
        self.prefix = self.prefix.ljust(width if width > len(self.prefix) else len(self.prefix)) + " |"
    
    def _format_message(self, message: str) -> str:
        return f"{self.prefix} {message}"
    
    def debug(self, message: str, *args, **kwargs):
        logger.debug(self._format_message(message), *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        logger.info(self._format_message(message), *args, **kwargs)
    
    def success(self, message: str, *args, **kwargs):
        logger.success(self._format_message(message), *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        logger.warning(self._format_message(message), *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        logger.error(self._format_message(message), *args, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        logger.critical(self._format_message(message), *args, **kwargs)


def get_logger(prefix: str, width: int = 12) -> PrefixedLogger:
    """Get a logger with automatic prefix for all messages
    
    Args:
        prefix: The prefix text to display
        width: Fixed width for padding (default: 20)
    """
    return PrefixedLogger(prefix, width)
