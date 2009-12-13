'''
Constants For Mamba Server/Client
----------------------------------

This is the single location for storing default
values for the servers and clients.
'''
class Defaults(object):
    '''
    '''
    Host    = "127.0.0.1"
    Port    = 22122
    Timeout = 60
    Path    = "/tmp/starling"
    Pidfile = "/var/run/mamba.pid"

#---------------------------------------------------------------------------# 
# Exported Identifiers
#---------------------------------------------------------------------------# 
__all__ = [ "Defaults" ]
