import logging
import logging.handlers
import os
import sys
from typing import Optional, Dict, Any

# Default log format
DEFAULT_LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Map string log levels to logging constants
LOG_LEVEL_MAP = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Global flag to track if root logger has been configured
_root_logger_configured = False

def configure_logging(
    name: Optional[str] = None, 
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_file: Optional[str] = None,
    max_bytes: int = 10485760,  # 10MB
    backup_count: int = 5,
    stream: Optional[Any] = sys.stdout,
    additional_handlers: Optional[list] = None
) -> logging.Logger:
    """
    Configure logging with consistent formatting across all modules
    
    Args:
        name: Logger name (defaults to the calling module's name)
        log_level: Override for the default log level
        log_format: Custom log format (defaults to timestamp, logger name, level, message)
        log_file: Optional file path to enable file logging
        max_bytes: Maximum size in bytes before rotating log file
        backup_count: Number of backup log files to keep
        stream: Stream to use for console output (sys.stdout by default)
        additional_handlers: List of additional logging handlers to add
        
    Returns:
        Configured logger instance
    """
    global _root_logger_configured
    
    # Get log level from environment variable or parameter
    log_level_str = log_level or os.getenv('LOG_LEVEL', 'INFO')
    level = LOG_LEVEL_MAP.get(log_level_str.upper(), logging.INFO)
    
    # Get log format from parameter or use default
    format_str = log_format or DEFAULT_LOG_FORMAT
    formatter = logging.Formatter(format_str)
    
    # Configure root logger only once
    if not _root_logger_configured:
        # Remove any existing handlers to avoid duplicates
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Create and configure console handler
        console_handler = logging.StreamHandler(stream)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
        
        # Set the root logger level
        root_logger.setLevel(level)
        
        # Mark root logger as configured
        _root_logger_configured = True
    
    # Get or create logger for the specified name
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Add file handler if log_file is specified
    if log_file:
        # Create directory for log file if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            
        # Use rotating file handler to prevent logs from growing too large
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Add any additional handlers
    if additional_handlers:
        for handler in additional_handlers:
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    
    return logger

def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Get a logger that has already been configured or create a new one
    with default settings if not yet configured.
    
    Args:
        name: Logger name (defaults to the calling module's name)
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If root logger hasn't been configured yet, configure it
    global _root_logger_configured
    if not _root_logger_configured:
        configure_logging(name)
        
    return logger