from importlib.metadata import version
import logging

__version__ = version("mawpy")
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(thread)d %(levelname)s %(module)s.%(funcName)s(): %(message)s"
)
logger = logging.getLogger(__name__)
