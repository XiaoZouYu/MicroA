from __future__ import absolute_import

import logging
import sys
import time
from contextlib import contextmanager

logger: logging.Logger = logging.getLogger("mia")
"""`mia`全局`logger`

Usage::

    >>> logger.debug('Some log message here')

"""
default_handler = logging.StreamHandler(sys.stdout)
default_handler.setFormatter(logging.Formatter('[%(asctime)s %(name)s] %(levelname)s: %(message)s'))
logger.addHandler(default_handler)
logger.setLevel("DEBUG")


def timing_logger(precision=3, level=logging.DEBUG):
    """ 一个计时记录器

    Usage::

        >>> import logging
        >>> log_time = timing_logger(2, logging.INFO)
        >>> with log_time("hello %s", "world"):
        ...     time.sleep(1)

    INFO:foobar:hello world in 1.00s
    :param precision: 精确度
    :param level: 日志级别
    :return: contextmanager
    """

    @contextmanager
    def timing(msg: str = '', *args):
        """ 使用上下文的方式来记录消耗的时间、以及相关的信息`msg`, `*args`

        :param msg: 消息头
        :param args: 额外信息
        :return:
        """
        start_time = time.time()

        try:
            yield
        finally:
            message = "{} in %0.{}fs".format(msg, precision)
            args += (time.time() - start_time,)
            logger.log(level, message, *args)

    return timing
