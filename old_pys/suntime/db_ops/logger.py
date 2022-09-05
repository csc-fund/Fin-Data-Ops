import logging
from datetime import datetime
from .configs import LOGGER_ROOT
import os

logger = logging.getLogger("logger")

today = datetime.today().strftime("%Y%m%d")

if not os.path.exists(LOGGER_ROOT):
    os.makedirs(LOGGER_ROOT)

file_pth = os.path.join(LOGGER_ROOT, today)

info_handler = logging.FileHandler(filename="{}_info.log".format(file_pth),encoding="utf-8")
warning_handler = logging.FileHandler(filename="{}_warning.log".format(file_pth),encoding="utf-8")
stream_handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
info_handler.setFormatter(formatter)
warning_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)


logger.setLevel(logging.DEBUG)
info_handler.setLevel(logging.INFO)
warning_handler.setLevel(logging.WARNING)
stream_handler.setLevel(logging.INFO)


logger.addHandler(info_handler)
logger.addHandler(warning_handler)
logger.addHandler(stream_handler)
