'''
Mamaba Client
------------------------------------------------------------

As mamba, like starling, speaks the "memcache" protocol, any
memcache client should be sufficient for interacting with the
server. 

However, we also define a wrapper around the standard
memcache wrappers (cmemcache if it is available and fall-back
to the pure python version) to simplify client code.
'''
import time
try:
    import cmemcache as memcache
except ImportError:
    import memcache
from mamba.errors import MambaException

#---------------------------------------------------------------------------#
# Class definitions
#---------------------------------------------------------------------------#
class Client(memcache.Client):
    '''
    A simple wrapper around a memcache client to simplify the
    client code needed to be written.
    '''
    __wait_time = 0.25

    def get(self, *args, **kwargs):
        '''
        Retrieves a key from the memcache.
        
        @return: The value or None if key doesn't exist
        '''
        while True:
            response = super(Client, self).get(*args, **kwargs)
            if response:
                return response
            time.sleep(self.__wait_time)
    
    def set(self, *args, **kwargs):
        '''
        Unconditionally sets a key to a given value in mamba.

        @return: Nonzero on success.
        @rtype: int
        '''
        retries = 0
        while retries < 3:
            result = super(Client, self).set(*args, **kwargs)
            if not result:
                retries += 1
                time.sleep(self.__wait_time)
            else: break;
        else: raise MambaException("Cannot set value on server")

    def sizeof(self, queue):
        '''
        Retrieve the number of items in the given queue, or all if the
        queue name is :all

        @return: The number of items in the queue
        @rtype: int
        '''
        result = self.get_stats()
        return sum(s['size'] for s in result)

    def flush(self, queue):
        '''
        Return an iterator around get to flush a queue

        @return: The next item in the queue
        '''
        for _ in sizeof(queue):
            yield get(queue)

#---------------------------------------------------------------------------# 
# Exported symbols
#---------------------------------------------------------------------------# 
__all__ = [ "Client" ]
