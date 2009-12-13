'''
Implementation of a Twisted Mamba Server
------------------------------------------

Example run::

    StartServer()
'''
import time
from twisted.internet import reactor
from twisted.internet.protocol import ServerFactory
from twisted.protocols.basic import LineReceiver

from mamba.handler import Handler
from mamba.defaults import Defaults
from mamba.statistics import Statistics

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
        self.handler.process(data, self.send)

#---------------------------------------------------------------------------#
# Extra Helper Functions
#---------------------------------------------------------------------------#

    def send(self, data):
        ''' Send and log a message to the network
        :param data: The mamba response message
        '''
        _logger.debug('TX: %s' % data)
        self.factory.statistics.bytes_written += len(data)
        return self.transport.write(data)

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
        self.path = options.get('path', Defaults.Path)
        self.timeout = options.get('timeout', Defaults.Timeout)

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
# Starting Factories
#---------------------------------------------------------------------------# 
def StartServer(options={}):
    ''' Helper method to start the Mamba Async TCP server

    :param options: The server options to apply
    '''
    # read and parse configuration options from etc
    # daemonize, trap signals (twisted plugin?)
    reactor.listenTCP(options.get('port', Defaults.Port),
        MambaServerFactory(options=options))
    reactor.run()

#---------------------------------------------------------------------------# 
# Exported symbols
#---------------------------------------------------------------------------# 
__all__ = [ "StartServer" ]
