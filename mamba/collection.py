'''
Queue Collection
------------------------------------------------------------

We wrap all the queues in a collection so that we can control
the safe creation, statistics, and logging.
'''
import os, thread, logging
from mamba.persistent import PersistentQueue
from mamba.errors import QueueCollectionException
from mamba.statistics import Statistics

class QueueCollection(object):
    '''
    Represents a collection of message queues for the
    mamba system
    '''
    def __init__(self, path):
        '''
        Initialize a new collection of queues persisted 
        at the given path.

        :param path: The path to store the queue persistence logs
        '''
        self.path = path
        self.queues = {}
        self.queue_locks = {}
        self.shutdown_lock = thread.allocate_lock()
        self.statistics = Statistics()
        self._setup_path(path)

    def put(self, key, data):
        '''
        Puts a new value in the queue at key

        :param key: The key to put the next value at
        :param data: The data to put at the specified key
        :return: True if the put succeeded, False otherwise
        '''
        queue = self.get_queues(key)
        if queue:
            self.statistics.current_bytes += len(data)
            self.statistics.total_items += 1
            queue.put(data)
        return queue is not None
    
    def get(self, key):
        '''
        Retrieves the data at key if it exists

        :param key: The key to put the next value at
        :return: The data at key or None if the queue does not exist
        '''
        queue = self.get_queues(key)
        result = None
        if not queue or not queue.qsize():
            self.statistics.get_misses += 1
        else:
            self.statistics.get_hits += 1
            result = queue.get()
            self.statistics.current_bytes -= len(result)
        return result
    
    def get_queues(self, key = None):
        '''
        Returns the specified queue or all active queues.

        :param key: The queue to retrieve
        :return: The requested queues
        '''
        # if we are closed, we will always be closed
        if self.shutdown_lock.locked():
            return None
        # if no queue specified, return all, otherwise return queue
        if not key: return self.queues
        if self.queues.has_key(key): return self.queues[key]
        
        # otherwise, we need to start the safe creation process
        if not self.queue_locks.has_key(key):
            self.queue_locks[key] = thread.allocate_lock()
        
        if self.queue_locks[key].locked():
            return None
        else:
            try:
                self.queue_locks[key].acquire()
                if not self.queues.has_key(key):
                    logging.debug("Creating new queue %s" % key)
                    self.queues[key] = PersistentQueue(self.path, key)
                    self.statistics.current_bytes += self.queues[key].initial_bytes
            finally:
                self.queue_locks[key].release()
                del self.queue_locks[key] # do we need this?
        return self.queues[key]

    def delete(self, key):
        '''
        Delete the queue at key if it exists

        :param key: The key to delete
        :return: True if successful, False otherwise
        '''
        queue = self.queues.get(key, None)
        if queue:
            queue.purge
            del self.queues[key]
        return queue is not None
        
    def get_statistic(self, name = None):
        '''
        Returns the requested statistic from the current queue
        Valid statistics are:

            ``get_misses``    Total number of get requests with empty responses
            ``get_hits``      Total number of get requests that returned data
            ``current_bytes`` Current size in bytes of items in the queues
            ``current_size``  Current number of items across all queues
            ``total_items``   Total number of items stored in queues.

        :param name: The statistic to retrieve, or none for all
        :return: the requested statistic
        '''
        if not name:
            return self.statistics
        elif name == 'current_size':
            return sum(q.qsize() for q in self.queues)
        else: return self.statistics[name]
    
    def close(self):
        '''
        Safely closes all queues

        :return: void
        '''
        # we don't release this until restart
        logging.debug("Closing all the queues")
        self.shutdown_lock.acquire()
        for name, queue in self.queues:
            queue.close()
            del self.queues[name]

    # ---------------------------------------------------- #
    # Private Methods
    # ---------------------------------------------------- #
    def _setup_path(self, path):
        '''
        Helper to check and create the persistence log directory

        :param path: The path to create the directory at
        :return: void
        '''
        if not os.path.isdir(path) and not os.access(path, os.W_OK):
            try:
                logging.info("Creating queue directory : '%s'" % path)
                os.makedirs(path)
            except OSError:
                raise QueueCollectionException("Queue path '%s' is inacessible" % path) 
        
