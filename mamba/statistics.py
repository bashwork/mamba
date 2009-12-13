'''
Mamba Statistics
------------------------------------------------------------

In order to allow users to know the status of the currently
running mamba servers, we provide the following statistics:

* server total uptime
* server time
* server version
* server user processor usage
* server system processor usage
* current numer of queues
* total number of queues that existed
* the current server memory usage
* current number of connections to the server
* total connections ever on the server
* number of get commands
* number of set commands
* queue get hits
* queue get misses
* network bytes read by server
* network bytes written by server
* the current server memory limit

We also provide per queue statistics as well. For each queue
we provide the following statistics:

* number of items in the queue
* number of items ever in the queue
* current size of queue persistence log
* numer of total expired items
'''

class Statistics(dict):
    '''
    A helper object to give us property access to the statistics
    for the mamba server.
    '''

    def __setattr__(self, key, value):
        ''' Overload to give use property access to dictionary

        :param key: The key to set a value at
        :param value: The value to set at key
        :return: void
        '''
        self.__setitem__(key, value)

    def __getattr__(self, key):
        ''' Overload to give use property access to dictionary

        :param key: The key to retrieve the value at
        :return: The value at the given key
        '''
        return self.get(key, 0)
