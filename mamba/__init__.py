"""
Mamba
-----------------------------------------

Released under the the BSD License
"""

__version__ = "0.1.0"

#---------------------------------------------------------------------------#
# Block unhandled logging
#---------------------------------------------------------------------------#
import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass

h = NullHandler()
logging.getLogger("mamba").addHandler(h)

