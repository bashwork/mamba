'''
Implementation of a Twisted Mamba Server
------------------------------------------

Mamba is driven from the twisted asynchronous event/networking
library. This allows mamba to be run using minimal resources
while at the same time solving the c10k problem.

Example run::

    from mamba.server import StartServer
    StartServer() # this will not return
'''
import time
from twisted.internet import reactor
from twisted.internet.protocol import ServerFactory
from twisted.protocols.basic import LineReceiver

from mamba.handler import Handler
from mamba.statistics import Statistics
from mamba.config import Options

#---------------------------------------------------------------------------#
# Logging
#---------------------------------------------------------------------------#
import logging
_logger = logging.getLogger("mamba.server")

#---------------------------------------------------------------------------#
# Server
#---------------------------------------------------------------------------#
class MambaProtocol(LineReceiver):
    '''
    Implementation of an async mamba client handler using
    the Twisted protocol.
    '''

    def connectionMade(self):
        ''' Callback for when a client connects
       
        Note, since the protocol factory cannot be accessed from the
        protocol __init__, the client connection made is essentially our
        __init__ method.     
        '''
        _logger.debug("Client Connected [%s]" % self.transport.getHost())
        self.factory.statistics.connections += 1
        self.factory.statistics.total_connections += 1
        self.handler = self.factory.getHandler()
        self.callbacks = {'send':self._send, 'exit':self._shutdown}

    def connectionLost(self, reason):
        ''' Callback for when a client disconnects

        :param reason: The client's reason for disconnecting
        '''
        _logger.debug("Client Disconnected")
        self.factory.statistics.connections -= 1

    def lineRecieved(self, data):
        ''' Callback when we receive any data

        :param data: The data sent by the client
        '''
        logger.debug("RX: %s", data)
        self.factory.statistics.bytes_read += len(data)
        self.handler.process(data, self.callbacks)

    #--------------------------------------------------------------------------#
    # Private Functions
    #--------------------------------------------------------------------------#

    def _send(self, data):
        ''' Send and log a message to the network
        :param data: The mamba response message
        '''
        _logger.debug('TX: %s' % data)
        self.factory.statistics.bytes_written += len(data)
        return self.transport.write(data)

    def _shutdown(self):
        ''' Helper method to shutdown the server

        :return: void
        '''
        try:
            reactor.stop()
        except: logging.error("Silencing reactor shutdown")

class MambaServerFactory(ServerFactory):
    '''
    Builder class for a mamba server that also holds the queue
    collections that are maintained across connections.
    '''

    protocol = MambaProtocol

    def __init__(self, options):
        ''' Overloaded initializer for the server factory

        :param options: The server options to apply
        '''
        self.path = options['path']
        self.timeout = options['timeout']

    def startFactory(self):
        '''
        Callback for when the server is started up

        :return: void
        '''
        _logger.debug('Mamba Stared on %s:%s' % (self.host, self.port))
        self.database = QueueCollection()
        self.statistics = Statistics()
        self.statistics.start_time = time.time()

    def stopFactory(self):
        '''
        Callback for when the server is shut down

        :return: void
        '''
        self.database.close()

    def getHandler(self):
        '''
        Helper method to encapsulate creating a message handler

        :return: A mamba message handler
        '''
        if not self.handler:
            self.handler = Handler(self.database)
        return self.handler

#---------------------------------------------------------------------------# 
# Helper Functions
#---------------------------------------------------------------------------# 
def StartServer(options={}):
    ''' Helper method to start the Mamba Async TCP server

    :param options: The server options to apply
    '''
    opts = Options.Config()
    opts.update(options) # source any user supplied options
    # daemonize, trap signals (twisted plugin?)
    reactor.listenTCP(options.get('port', opts['port']),
        MambaServerFactory(options=opts))
    reactor.run()

#---------------------------------------------------------------------------# 
# Exported symbols
#---------------------------------------------------------------------------# 
__all__ = [ "StartServer" ]
