'''
'''
import os, time
from struct import pack, unpack
from Queue import Queue
from mamba.errors import TransactionLogException

#---------------------------------------------------------------------------#
# Logging
#---------------------------------------------------------------------------#
import logging
_logger = logging.getLogger("mamba.queue")

#---------------------------------------------------------------------------#
# Local helpers
#---------------------------------------------------------------------------#
def file_iterator(fd):
    ''' Simply helper to make code cleaner '''
    while True:
        value = fd.read(1)
        if not value: break
        yield value

#---------------------------------------------------------------------------#
# Class definitions
#---------------------------------------------------------------------------#
class PersistentQueue(Queue):
    '''
    PersistentQueue is a subclass of Python synchronized class. It adds a 
    transactional log to the in-memory Queue, which enables quickly rebuilding
    the Queue in the event of a sever outage.
    '''
    __max_size = 16 * (1024**2) # 16 MB

    __trx_cmd_push = "\x00"
    __trx_cmd_pop  = "\x01"
    __trx_push     = "\x00%s%s"
    __trx_pop      = "\x01"

    def __init__(self, persistence_path, queue_name):
        '''
        Create a new PersistentQueue at +persistence_path+/+queue_name+.
        If a queue log exists at that path, the Queue will be loaded from
        disk before being available for use.

        :param persistence_path: The path to the persistence directory
        :param queue_name: The name of the queue
        '''
        self.path = persistence_path
        self.name = queue_name
        self.log_path = os.path.join(self.path, self.name)
        self.total_items = 0
        Queue.__init__(self, 0)
        self.initial_bytes = self._replay_transactions()

    def put(self, value, log=True):
        '''
        Pushes ``value`` to the queue. By default, ``put`` will write to the
        transactional log. Set ``log`` to ``False`` to override this behaviour.

        :param value: The value to queue up
        :param log: Set to True to log to the transaction log, False otherwise
        :return: void
        '''
        if log:
            self._log_exists_or_throw(log)
            size = pack("I", len(value))
            self._transaction(self.__trx_push % (size, value))
        self.total_items += 1
        Queue.put(self, value)

    def get(self, log = True):
        '''
        Retrieve the next element off the queue

        :param log: Set to True to log to the transaction log, False otherwise
        :return: The next item off of the queue
        '''
        self._log_exists_or_throw(log)
        value = Queue.get(self, log)
        if log: self._transaction(self.__trx_pop)
        return value

    def close(self):
        '''
        Finish all writes to this queue's transaction log file
        and close it.

        :return: void
        '''
        # TODO find a way to do this without another lock?
        _logger.debug("Closing the queue %s" % self.name)
        temp = self.transactions
        self.transactions = None
        temp.close()

    def purge(self):
        '''
        Purge the entire transaction log for this queue

        :return: void
        '''
        _logger.debug("Purging the entire transaction for %s" % self.name)
        self.close()
        if os.path.exists(self.log_path):
            os.remove(self.log_path)

    # --------------------------------------------------------------- #
    # Private Methods
    # --------------------------------------------------------------- #
    def _open_log(self):
        '''
        Helper method to open a new log file and initialize values

        :return: void
        '''
        fd = os.open(self.log_path, os.O_RDWR|os.O_CREAT)
        self.transactions = os.fdopen(fd, "rb+")
        self.log_size = os.path.getsize(self.log_path)

    def _rotate_log(self):
        '''
        Helper method to quickly rename a file to
        `filename.timestamp`

        :return: void
        '''
        # guard with a reader writer lock?
        _logger.debug("Rotating log for queue %s" % self.name)
        self.transactions.close()
        os.rename(self.log_path, "%s.%s" % (self.log_path, time.time()))
        self._open_log()

    def _log_exists_or_throw(self, test=True):
        '''
        Helper to make the log checking look cleaner

        :return: void
        '''
        if test and not self.transactions:
            _logger.error("Transaction log not available for queue %s" % self.name)
            raise TransactionLogException("Transaction log not available")

    def _read_command(self, fd):
        '''
        Helper method to read a single command back out of the
        transactions log

        :param fd: The file descriptor to read from
        :return: A tuple of the read command and size
        '''
        raw_size = fd.read(4)
        if not raw_size:
            return (0, None)
        size = unpack("I", raw_size)[0] 
        data = fd.read(size)
        return (size, data)

    def _replay_transactions(self):
        '''
        Helper to make the log checking look cleaner

        :return: void
        '''
        self._open_log()
        bytes_read = 0

        _logger.debug("Reading back transaction log for queue %s" % self.name)
        for cmd in file_iterator(self.transactions):
            if cmd == self.__trx_cmd_push:
                (size, data) = self._read_command(self.transactions)
                if not data: continue
                self.put(data, False)
                bytes_read -= size
            elif cmd == self.__trx_cmd_pop:
                bytes_read -= len(self.get(False))
            else:
                _logger.warning("Invalid command(%s) in transaction log" % cmd)
        _logger.debug("Finished reading back transaction log for queue %s" % self.name)
        return bytes_read

    def _transaction(self, data):
        '''
        Helper method to write some data to the transaction
        log file.

        :param data: The data to append to the log
        :return: void
        '''
        # guard with a reader writer lock?
        self._log_exists_or_throw()
        self.transactions.write(data)
        self.transactions.flush()
        self.log_size += len(data)
        if self.log_size > self.__max_size and self.qsize() == 0:
            self._rotate_log()

#---------------------------------------------------------------------------# 
# Exported Identifiers
#---------------------------------------------------------------------------# 
__all__ = [ "PersistentQueue" ]
