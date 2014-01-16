import logging

# Silence requests logger
requests_log = logging.getLogger("requests")
requests_log.setLevel(logging.WARNING)

log = logging.getLogger('s3tup')
log.addHandler(logging.NullHandler())