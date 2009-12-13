'''
Mamba Protocol Handler
-----------------------------------------------------------

'''
import re, os, time, logging
from struct import pack, unpack
from resource import getrusage, RUSAGE_SELF
from mamba import __version__

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

    def process(self, command, callback):
        '''
        Main handler processing function for new client request

        :param command: The command to issue against the queue
        :param callback: The continuation to send the result to
        :return: void
        '''
        # TODO clean this up
        m = re.match(Messages.set_command, command)
        if m:
            self._set(callback, m.group(1), m.group(2), m.group(3), m.group(4))
            return
        m = re.match(Messages.get_command, command)
        if m:
            self._get(callback, m.group(1))
            return
        m = re.match(Messages.stats_command, command)
        if m:
            self._get_statistics(callback)
            return
        m = re.match(Messages.delete_command, command)
        if m:
            self._delete(callback, m.group(1))
            return
        m = re.match(Messages.quit_command, command)
        if m:
            self._quit(callback)
            return
        m = re.match(Messages.shutdown_command, command)
        if m:
            self._shutdown(callback)
            return
        logging.debug("Received unknown command")
        callback(Messages.unknown_command)

    # ---------------------------------------------------- #
    # Private Methods
    # ---------------------------------------------------- #

    def _shutdown(self, callback):
        '''
        Wrapper around the client shutdown operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        logging.debug("Received a SHUTDOWN command")
        # server is going down, no response needed
        # TODO stop the server

    def _quit(self, callback):
        '''
        Wrapper around the client quit operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        logging.debug("Received a QUIT command")
        self.statistics.clean_exits += 1
        # client is exiting, no response needed

    def _delete(self, callback, key):
        '''
        Wrapper around the queue set operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        logging.debug("Received a DELETE command")
        self.statistics.delete_requests += 1
        self.database.delete(key)
        callback(Message.delete_response)

    def _set(self, callback, key, flags, expiry, length):
        '''
        Wrapper around the queue set operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        logging.debug("Received a SET command")
        self.statistics.set_requests += 1

        # TODO check this
        length = int(length)
        data = self.file.read(length)
        data_end = self.file.read(2)
        self.stats['bytes_read'] += (length + 2)
        if data_end == '\r\n' and len(data) == length:
            internal_data = pack(DATA_PACK_FMT % (length), int(flags), int(expiry), data)
            if self.queue_collection.put(key, internal_data):
                logging.debug("SET command is a success")
                response = Message.set_response_success
            else:
                logging.warning("SET command failed")
                response = Message.set_response_failure
        else:
            logging.error("SET command failed hard")
            response = Message.set_client_data_error
        callback(SET_CLIENT_DATA_ERROR)
    
    def _get(self, callback, key):
        '''
        Wrapper around the queue get operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        logging.debug("Received a GET command")
        self.statistics.get_requests += 1

        # TODO check this
        now = time.time()
        data = None
        response = self.queue_collection.take(key)
        while response:
            flags, expiry, data = unpack(DATA_PACK_FMT % (len(response) - 8), response)
            if expiry == 0 or expiry >= now:
                break
            if self.expiry_stats.has_key(key):
                self.expiry_stats[key] += 1
            else:
                self.expiry_stats[key] = 1
            flags, expiry, data = None, None, None
            response = self.queue_collection.take(key)

        if data:
            response = Message.get_response % (key, flags, len(data), data)
        else: response = Message.get_response_empty
        callback(response)
    
    def _get_statistics(self, callback):
        '''
        Wrapper around the server statistics retrieval operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        logging.debug("Received a STATS command")
        callback(Message.stats_response % (
            os.getpid(),                              # server pid
            time.time() - self.statistics.start_time, # total uptime
            time.time(),                              # current time
            __version__,                              # server version
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
        
    def _get_queue_statistics(self, callback):
        '''
        Wrapper around the queue statistics retrieval operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        # TODO this can be done better, fix when we do queue-coll
        response = ''
        for name in self.database.get_queues():
            queue = self.database.get_queues(name)
            if self.expiry_stats.has_key(name):
                expiry_stats = self.expiry_stats[name]
            else:
                expiry_stats = 0
            response += Message.queue_stats_response % ({
                'name':   name,               # queue name
                'size':   queue.qsize(),      # number of queues
                'total':  queue.total_items,  # total number of items in all queues
                'logs':   queue.log_size,     # current queue log size
                'expire': expiry_stats})      # current expiration statistics
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
    get_response_nil      = "END\r\n"
   
    # mamba set constants
    set_command           = r'^set (.{1,250}) ([0-9]+) ([0-9]+) ([0-9]+)\r\n$'
    set_response_success  = "STORED\r\n"
    set_response_failure  = "NOT STORED\r\n"
    set_client_data_error = "CLIENT_ERROR bad data chunk\r\nERROR\r\n"
   
    # mamba delete constants
    delete_command           = r'\Adelete (.{1,250}) ([0-9]+)\r\n$'
    delete_response          = "END\r\n"

    # mamba stop commands
    shutdown_command      = r'\Ashutdown\r\n$'
    quit_command          = r'\Aquit\r\n$'
   
    # mamba statistics constants
    stats_command         = r'stats\r\n$'
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
