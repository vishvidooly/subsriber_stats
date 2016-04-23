import os
import logging
import logging.handlers

is_debug = os.environ.get('DATA_DEBUG', '0')

if is_debug == '0':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    _facility = logging.handlers.SysLogHandler.LOG_LOCAL1
    _address = '/dev/log'
    _handler = logging.handlers.SysLogHandler(address=_address,
                                              facility=_facility)
    _formatter = logging.Formatter('%(message)s')
    _handler.setFormatter(_formatter)
    logger.addHandler(_handler)
else:
    class abc(object):
        def info(self, msg1, msg2, msg3):
            pass

    logger = abc()
