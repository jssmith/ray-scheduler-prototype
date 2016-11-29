import logging

class TimestampedLogger():
    def __init__(self, name, system_time):
        self._logger = logging.getLogger(name)
        self._system_time = system_time

    def debug(self, message):
        self._logger.debug(message, extra={'timestamp':self._system_time.get_time()})

    # TODO add other logging levels

def setup_logging():
    logging_format = '%(timestamp).6f %(name)s %(message)s'
    logging.basicConfig(format=logging_format)
    logging.getLogger().setLevel(logging.DEBUG)
