import logging
import os
from logging.handlers import RotatingFileHandler
import datetime

log_dir = f'/home/pi/Projects/cosmo-m/log/{datetime.datetime.now().strftime("%y%m%d_%H%M%S")}'
log_fname = 'debug.log'

if not os.path.exists(log_dir):
    os.mkdir(log_dir)

logger = logging.getLogger('VINE')
logger.setLevel(logging.DEBUG)

path = os.path.join(log_dir, log_fname)
rot_file_hander = RotatingFileHandler(path, mode='a', maxBytes=1048576, backupCount=5)
formatter = logging.Formatter('[%(levelname)s] :: %(asctime)s :: %(module)s ::%(name)s ::%(message)s\n')
rot_file_hander.setFormatter(formatter)
logger.addHandler(rot_file_hander)
# logger.debug('202002')
