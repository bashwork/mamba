'''
This service can be run with the following::

    twistd -ny mamba.tac
'''
from twisted.application import service, internet
from twisted.python.log import ILogObserver, FileLogObserver
from twisted.python.logfile import DailyLogFile

from mamba.server import MambaServerFactory
from mamba.config import Options

def BuildService():
    '''
    A helper method to build the service
    '''
    opts = Options.ConfigFile()
    factory = MambaServerFactory(options=opts)
    return internet.TCPServer(8080, factory)

application = service.Application("Mamba Server")
service = BuildService()
logfile = DailyLogFile("mamba.log", "/tmp")
application.setComponent(ILogObserver, FileLogObserver(logfile).emit)
service.setServiceParent(application)
