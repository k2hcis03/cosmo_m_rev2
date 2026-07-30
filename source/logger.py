import logging
import os
import atexit
import multiprocessing
from logging.handlers import RotatingFileHandler, QueueHandler, QueueListener
import datetime

log_dir = f'/home/pi/Projects/cosmo-m/log/{datetime.datetime.now().strftime("%y%m%d_%H%M%S")}'
log_fname = 'debug.log'

if not os.path.exists(log_dir):
    os.mkdir(log_dir)

path = os.path.join(log_dir, log_fname)
# encoding을 지정하지 않으면 실행 환경의 로케일에 따라 기록 인코딩이 달라져 한글 로그가 깨질 수 있다.
_file_handler = RotatingFileHandler(path, mode='a', maxBytes=1048576, backupCount=5, encoding='utf-8')
_formatter = logging.Formatter('[%(levelname)s] :: %(asctime)s :: %(module)s ::%(name)s ::%(message)s\n')
_file_handler.setFormatter(_formatter)

_log_queue = multiprocessing.Queue(-1)

_listener = QueueListener(_log_queue, _file_handler, respect_handler_level=True)
_listener.start()

logger = logging.getLogger('VINE')
logger.setLevel(logging.DEBUG)
logger.addHandler(QueueHandler(_log_queue))

atexit.register(_listener.stop)
