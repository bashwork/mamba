'''
Mamba Protocol Handler
-----------------------------------------------------------

'''
import re, os, time, logging
from struct import pack, unpack
from resource import getrusage, RUSAGE_SELF
import mamba

class Handler(object):
    '''
    Class to abstact away all the handling for the mamba
    server protocol. Since we are using the twisted line
    reader protocol, we shouldn't have to do any network
    work here, simply the logic for the protocol.  To
    abstract it even more, the response command is passed
    in as a continuation.
    '''

    def __init__(self, database, statistics):
        '''
        Initializes a new instance of the mamba handler class

        :param database: The queue collection to work against
        :param statistics: The statistics handle for the server
        '''
        self.database = database
        self.statistics = statistics
        self.exiprations = {}
        self.buffer = ''

    def process(self, command, callbacks):
        '''
        Main handler processing function for new client request

        :param command: The command to issue against the queue
        :param callbacks: The continuations to process the command result
        :return: void
        '''
        # check if we have a set command pending

        # otherwise process the request as an new command
        for proc, regex in Messages.get_commands():
            match = re.match(regex, command)
            if match:
                self.__dict__(proc)(callbacks, match)
                break
        else:
            logging.debug("Received unknown command")
            callbacks['send'](Messages.unknown_command)

    # ---------------------------------------------------- #
    # Private Methods
    # ---------------------------------------------------- #

    def _shutdown(self, callbacks, match):
        '''
        Wrapper around the client shutdown operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        logging.debug("Received a SHUTDOWN command")
        callbacks['exit']()
        # server is going down, no response needed

    def _quit(self, callbacks, match):
        '''
        Wrapper around the client quit operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        logging.debug("Received a QUIT command")
        self.statistics.clean_exits += 1
        # client is exiting, no response needed

    def _delete(self, callbacks, match):
        '''
        Wrapper around the queue delete operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        logging.debug("Received a DELETE command")
        self.statistics.delete_requests += 1

        key = match.group(1)
        self.database.delete(key)
        callbacks['send'](Messages.delete_response)

    def _set(self, callbacks, match):
        '''
        Wrapper around the queue set operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        logging.debug("Received a SET command")
        self.statistics.set_requests += 1

        key, flags, expire, length = (
            match.group(1), match.group(2), match.group(3), match.group(4))

        # state machine...ugh
        data = self.handle.read(length)
        data_end = self.handle.read(2)
        # state machine...ugh

        self.statistics.bytes_read += (length + 2)
        if data_end == '\r\n' and len(data) == length:
            compressed = pack(Messages.data_pack_format % (length, flags, expire, data))
            if self.database.put(key, compressed):
                callbacks['send'](Messages.set_response_success)
            else: callbacks['send'](Messages.set_response_failure)
        else: callbacks['send'](Messages.set_client_data_error)

    def _get_next_message(self, key):
        '''
        Helper method to abstract away from getting the next valid
        non expired message out of a queue

        :param key: The key to retrieve the next message from
        :return: The next message, or None if none exist
        '''
        now = time.time()
        result = None
        for message in self.database.get(key):
            flag, expire, result = unpack(
                Messages.data_pack_format % (len(response) - 8), message)
            if expire == 0 or expire >= now:
                break
            self.exiprations[key] = 1 + self.expirations.get(key, 0)
            flag, expire, result = (None, None, None) # reset results
        return result
    
    def _get(self, callbacks, match):
        '''
        Wrapper around the queue get operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        logging.debug("Received a GET command")
        self.statistics.get_requests += 1

        key = match.group(1)
        message = self._get_next_message(key)
        if message:
            callbacks['send'](Messages.get_response % (key, flags, len(data), data))
        else: callbacks['send'](Messages.get_response_empty)
    
    def _statistics(self, callbacks, match):
        '''
        Wrapper around the server statistics retrieval operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        logging.debug("Received a STATS command")
        callbacks['send'](Messages.stats_response % (
            os.getpid(),                              # server pid
            time.time() - self.statistics.start_time, # total uptime
            time.time(),                              # current time
            mamba.__version__,                        # server version
            getrusage(RUSAGE_SELF)[0],                # user processor time
            getrusage(RUSAGE_SELF)[1],                # system processor time
            self.database.get_stats('current_size'),
            self.database.get_stats('total_items'),
            self.database.get_stats('current_bytes'),
            self.statistics.connections,
            self.statistics.total_connections,
            self.statistics.get_requests,
            self.statistics.set_requests,
            self.database.stats['get_hits'],
            self.database.stats['get_misses'],
            self.statistics.bytes_read,
            self.statistics.bytes_written,
            0,
            self._get_queue_statistics()
        ))
        
    def _get_queue_statistics(self):
        '''
        Wrapper around the queue statistics retrieval operation

        :return: The combined statistics for each queue
        '''
        # TODO clean this up
        response = ''
        for name in self.database.get_queues():
            queue = self.database.get_queues(name)
            expire_count = self.expirations.get(name, 0)
            response += Messages.queue_stats_response % ({
                'name':   name,               # queue name
                'size':   queue.qsize(),      # number of queues
                'total':  queue.total_items,  # total number of items in all queues
                'logs':   queue.log_size,     # current queue log size
                'expire': expire_count})      # current expiration statistics
        return response

# -------------------------------------------------------- #
# Thar' be dragons here
# -------------------------------------------------------- #
class Messages(object):
    '''
    The static protocol messages and regular expressions for the
    starling protocol.
    '''

    # mamba common constants
    data_pack_format      = "!II%ss"
    unknown_command       = "CLIENT_ERROR bad command line format\r\n"
   
    # mamba get constants
    get_command           = r'^get (.{1,250})\r\n$'
    get_response          = "VALUE %s %s %s\r\n%s\r\nEND\r\n"
    get_response_empty    = "END\r\n"
   
    # mamba set constants
    set_command           = r'^set (.{1,250}) ([0-9]+) ([0-9]+) ([0-9]+)\r\n$'
    set_response_success  = "STORED\r\n"
    set_response_failure  = "NOT STORED\r\n"
    set_client_data_error = "CLIENT_ERROR bad data chunk\r\nERROR\r\n"
   
    # mamba delete constants
    delete_command        = r'^delete (.{1,250}) ([0-9]+)\r\n$'
    delete_response       = "END\r\n"

    # mamba stop commands
    shutdown_command      = r'^shutdown\r\n$'
    quit_command          = r'^quit\r\n$'
   
    # mamba statistics constants
    stats_command         = r'^stats\r\n$'
    stats_response        = """STAT pid %d\r
STAT uptime %d\r
STAT time %d\r
STAT version %s\r
STAT rusage_user %0.6f\r
STAT rusage_system %0.6f\r
STAT curr_items %d\r
STAT total_items %d\r
STAT bytes %d\r
STAT curr_connections %d\r
STAT total_connections %d\r
STAT cmd_get %d\r
STAT cmd_set %d\r
STAT get_hits %d\r
STAT get_misses %d\r
STAT bytes_read %d\r
STAT bytes_written %d\r
STAT limit_maxbytes %d\r
%s\nEND\r\n"""

    # mamba queue statistics constants
    queue_stats_response = """
STAT queue_%(name)s_items %(size)d\r
STAT queue_%(name)s_total_items %(total)d\r
STAT queue_%(name)s_logsize %(logs)d\r
STAT queue_%(name)s_expired_items %(expire)d\r"""

    # helper to clean up processing code
    _commands = {
        '_delete':     delete_command,
        '_get':        get_command,
        '_set':        set_command,
        '_quit':       quit_command,
        '_shutdown':   shutdown_command,
        '_statistics': stats_command,
    }

    @staticmethod
    def get_commands():
        '''
        Helper method to return an iterator over the mamba commands
        as well as the callbacks processors used to evaluate the
        command results

        :return: The iterator for the command types
        '''
        for pair in Methods._commands.iteritems():
            yield pair

