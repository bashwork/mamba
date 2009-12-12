'''
Mamba Protocol Handler
-----------------------------------------------------------

'''
import re, os, time, logging
from struct import pack, unpack
from resource import getrusage, RUSAGE_SELF
from mamba import __version__

class Handler(object):

    def __init__(self, database, stats):
        '''
        Initializes a new instance of the mamba handler class

        :param database: The queue collection to work against
        :param stats: The statistics handle for the server
        '''
        self.database = database
        self.stats = stats

    def process(self, command, callback):
        '''
        Main handler processing function for new client request

        :param command: The command to issue against the queue
        :param callback: The continuation to send the result to
        :return: void
        '''
        m = re.match(Messages.set_command, command)
        if m:
            logging.debug("Received a SET command")
            self.stats['set_requests'] += 1
            self.set(callback, m.group(1), m.group(2), m.group(3), m.group(4))
            return
        m = re.match(Messages.get_command, command)
        if m:
            logging.debug("Received a GET command")
            self.stats['get_requests'] += 1
            self.get(callback, m.group(1))
            return
        m = re.match(Messages.stats_command, command)
        if m:
            logging.debug("Received a STATS command")
            self.get_stats(callback)
            return
        logging.debug("Received unknow command")
        callback(Messages.unknown_command)

    def _set(self, callback, key, flags, expiry, length):
        '''
        Wrapper around the queue set operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        # TODO check this
        length = int(length)
        data = self.file.read(length)
        data_end = self.file.read(2)
        self.stats['bytes_read'] += (length + 2)
        if data_end == '\r\n' and len(data) == length:
            internal_data = pack(DATA_PACK_FMT % (length), int(flags), int(expiry), data)
            if self.queue_collection.put(key, internal_data):
                logging.debug("SET command is a success")
                self._respond(SET_RESPONSE_SUCCESS)
            else:
                logging.warning("SET command failed")
                self._respond(SET_RESPONSE_FAILURE)
        else:
            logging.error("SET command failed hard")
            self._respond(SET_CLIENT_DATA_ERROR)
    
    def _get(self, callback, key):
        '''
        Wrapper around the queue get operation

        :param callback: The continuation to send the result to
        :return: void
        '''
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
            logging.debug("GET command respond with value")
            self._respond(GET_RESPONSE, key, flags, len(data), data)
        else:
            logging.debug("GET command response was empty")
            self._respond(GET_RESPONSE_EMPTY)
    
    def _get_stats(self, callback):
        '''
        Wrapper around the server statistics retrieval operation

        :param callback: The continuation to send the result to
        :return: void
        '''
        callback(Message.stats_response % (
            os.getpid(),                            # server pid
            time.time() - self.stats['start_time'], # total uptime
            time.time(),                            # current time
            __version__,                            # server version
            getrusage(RUSAGE_SELF)[0],              # user processor time
            getrusage(RUSAGE_SELF)[1],              # system processor time
            self.database.get_stats('current_size'),
            self.database.get_stats('total_items'),
            self.database.get_stats('current_bytes'),
            self.stats['connections'],
            self.stats['total_connections'],
            self.stats['get_requests'],
            self.stats['set_requests'],
            self.database.stats['get_hits'],
            self.database.stats['get_misses'],
            self.stats['bytes_read'],
            self.stats['bytes_written'],
            0,
            self._get_queue_stats()
        ))
        
    def _get_queue_stats(self, callback):
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
            response += Message.queue_stats_response % (
                    name,               # queue name
                    queue.qsize(),      # number of queues
                    name,               # queue name
                    queue.total_items,  # total number of items in all queues
                    name,               # queue name
                    queue.log_size,     # current queue log size
                    name,               # queue name
                    expiry_stats)       # current expiration statistics
        return response

# -------------------------------------------------------- #
# Thar' be dragons here
# -------------------------------------------------------- #
class Messages(object):
    '''
    The static protocol messages and regular expressions for the
    starling protocol.
    '''
    data_pack_format      = "!II%ss"
    unknown_command       = "CLIENT_ERROR bad command line format\r\n"
    
    get_command           = r'^get (.{1,250})\r\n$'
    get_response          = "VALUE %s %s %s\r\n%s\r\nEND\r\n"
    get_response_nil      = "END\r\n"
    
    set_command           = r'^set (.{1,250}) ([0-9]+) ([0-9]+) ([0-9]+)\r\n$'
    set_response_success  = "STORED\r\n"
    set_response_failure  = "NOT STORED\r\n"
    set_client_data_error = "CLIENT_ERROR bad data chunk\r\nERROR\r\n"
    
    del_command           = r'\Adelete (.{1,250}) ([0-9]+)\r\n$'
    del_response          = "END\r\n"
    
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

    queue_stats_response = """
STAT queue_%s_items %d\r
STAT queue_%s_total_items %d\r
STAT queue_%s_logsize %d\r
STAT queue_%s_expired_items %d\r"""
