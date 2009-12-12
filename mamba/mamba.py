import time
try:
    import cmemcache as memcache
except ImportError:
    import memcache

class mamba(memcache.Client):
    '''
    '''
    __wait_time = 0.25

    def get(self, *args, **kwargs):
        '''
        Retrieves a key from the memcache.
        
        @return: The value or None if key doesn't exist
        '''
        while True:
            response = super(mamba, self).get(*args, **kwargs)
            if response:
                return response
            time.sleep(WAIT_TIME)
    
    def set(self, *args, **kwargs):
        '''
        Unconditionally sets a key to a given value in mamba.

        @return: Nonzero on success.
        @rtype: int
        '''
        retries = 0
        while retries < 3:
            result = super(mamba, self).set(*args, **kwargs)
            if not result:
                retries += 1
                time.sleep(self.__wait_time)
            else: break;
        else:
            raise PeafowlError("Can't set value")

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
