import logging

# Configure root logger
app_logger = logging.getLogger('')
app_logger.setLevel(logging.DEBUG)

# Configure log file for all output
app_log_handler = logging.FileHandler('Log/app_log.log')
app_log_handler.setLevel(logging.DEBUG)
app_log_formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(filename)s:%(lineno)d - %(message)s')
app_log_handler.setFormatter(app_log_formatter)

# Configure console logging
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s | %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

app_logger.addHandler(app_log_handler)
app_logger.addHandler(console_handler)

app_logger.debug("Logging configured")