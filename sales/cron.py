import logging
# logger = logging.getLogger(__name__)
logger = logging.getLogger('django')

def hello_world():
    print("Hello World")
    logger.debug("Hello World")