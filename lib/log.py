import os
import sys
import logging
import logging.config
import colorlog
import socket
import threading
import time
import re
import math

sys.path.insert(0, "%s/.." % os.path.split(os.path.realpath(__file__))[0])

from lib.config import fluentdconf
from fluent import sender

# configs from env
columndebug = os.getenv("COL_DEBUG", "*")
iscolor = os.getenv("IS_COLOR", False)
isremote = os.getenv("SEND_REMOTE", False)
isdevelopmentenv = os.getenv("PYTHON_ENV", False)

logconfig = isdevelopmentenv and fluentdconf["test"] or fluentdconf["pro"]

# add trace level
TRACE = 15
logging.addLevelName(TRACE, "TRACE")

logging.config.dictConfig({
    'version': 1,
    'formatters': {
        'colored': {
            '()': 'colorlog.ColoredFormatter',
            'format': '%(log_color)s%(levelname)s%(reset)s %(message)s',
            'log_colors': {
                'DEBUG': 'bg_black',
                'INFO': 'green',
                'TRACE': 'blue',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bg_bold_yellow'
            }
        },
        'nocolor': {
            '()': "logging.Formatter",
            'format': '%(levelname)s %(message)s'
        }
    },
    'handlers': {
        'colored': {
            'class': 'logging.StreamHandler',
            'formatter': 'colored',
        },
        'nocolor': {
            'class': 'logging.StreamHandler',
            'formatter': 'nocolor'
        }
    },
    'loggers': {
        'colored': {
            'handlers': ['colored'],
        },
        'nocolor': {
            'handlers': ['nocolor']
        }
    }
})


def msgformat(logfunc):
    '''decorator to construct the log msg'''

    def __msgformatter(self, msg, *args, **kwargs):
        logfunc(self, "%s %s %s", repr(msg), args and repr(
            args) or "", kwargs and repr(kwargs) or "")

    return __msgformatter


class SessionAdapter(logging.LoggerAdapter):
    '''Adapter to add session info to'''

    def __init__(self, logger, extra):
        self.logger = logger
        self.tag = re.sub(r"\s+", "_", str(extra["tag"]))
        self.session = re.sub(r"\s+", "_", str(extra["session"]))
        self.scope = re.sub(r"\s+", "_", str(extra["scope"]))
        self.instance = re.sub(r"\s+", "_", str(extra["instance"]))

    def process(self, msg, kwargs):
        return '%s %s %s:%s:%s' % (self.tag, msg, self.session, self.instance, self.scope), kwargs


class Log():
    def __init__(self, tag=None, instance=None, scope=None, session=None):
        self.tag = tag or "log"

        # set up fluent sender
        self.sender = sender.FluentSender(
            logconfig["logger_name"], host=logconfig["host"], port=logconfig["port"])

        # setup color log
        self.logger = SessionAdapter(logging.getLogger(iscolor and "colored" or "nocolor"),
        {
            "tag": self.tag,
            "instance": instance or "%s.%d" % (socket.gethostname(), os.getpid()),
            "scope": scope or "g",
            "session": session or math.floor(time.time() * 1000)
        })
        self.logger.setLevel(logging.DEBUG)

    @msgformat
    def trace(self, msg, *args, **kwargs):
        self.logger.log(TRACE, msg, *args, **kwargs)

    @msgformat
    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    @msgformat
    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    @msgformat
    def warn(self, msg, *args, **kwargs):
        self.logger.warn(msg, *args, **kwargs)

    @msgformat
    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    @msgformat
    def fatal(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)

    def remote(self, obj):
        self.sender.emit(self.tag, obj)


if __name__ == "__main__":
    log = Log(tag="stdout")
    log.fatal(dict(a=12, b="12fdafadf"), "heheheh")
    log.remote(dict(k="vdf", k2="sdfr"))
