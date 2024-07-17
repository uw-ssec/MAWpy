from .version import __version__
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(thread)d %(levelname)s %(module)s.%(funcName)s(): %(message)s"
)
logger = logging.getLogger(__name__)

__all__ = ["__version__"]
