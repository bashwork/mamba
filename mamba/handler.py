'''
Mamba Protocol Handler
-----------------------------------------------------------

'''
import re, os, time
from struct import pack, unpack
from resource import getrusage, RUSAGE_SELF
import mamba

#---------------------------------------------------------------------------#
# Logging
#---------------------------------------------------------------------------#
import logging
_logger = logging.getLogger("mamba.server")

#---------------------------------------------------------------------------#
# Class definitions
#---------------------------------------------------------------------------#
class Handler(object):
    '''
    Class to abstact away all the handling for the mamba
    server protocol. Since we are using the twisted line
    reader protocol, we shouldn't have to do any network
    work here, simply the logic for the protocol.  To
    abstract it even more, the response command is passed
    in as a continuation.
    '''

    # mamba command regex collection
    _commands = {
        lambda s,d,c: s._get(d,c):        re.compile(r'^get (.{1,250})$'),
        lambda s,d,c: s._set(d,c):        re.compile(r'^set (.{1,250}) ([0-9]+) ([0-9]+) ([0-9]+)$'),
        lambda s,d,c: s._delete(d,c):     re.compile(r'^delete (.{1,250}) ([0-9]+)$'),
        lambda s,d,c: s._statistics(d,c): re.compile(r'^stats$'),
        lambda s,d,c: s._quit(d,c):       re.compile(r'^quit$'),
        lambda s,d,c: s._shutdown(d,c):   re.compile(r'^shutdown$'),
    }

    def __init__(self, database, statistics):
        '''
        Initializes a new instance of the mamba handler class

        :param database: The queue collection to work against
        :param statistics: The statistics handle for the server
        '''
        self.database = database
        self.statistics = statistics
        self.exiprations = {}
        self.state = None
        self.database.put('galen', 'galen')

    def process(self, command, callbacks):
        '''
        Main handler processing function for new client request

        :param command: The command to issue against the queue
        :param callbacks: The continuations to process the command result
        :return: void
        '''
        # if we have a set command pending
        if self.state: self._set_data(callbacks, command)
        else:
        # otherwise process the request as an new command
            #for proc, regex in Messages.get_commands():
            for proc, regex in self._commands.iteritems():
                match = re.match(regex, command)
                if match:
                    #self.__dict__[proc](callbacks, match)
                    proc(self, callbacks, match)
                    break
            else:
                _logger.debug("Received unknown command")
                callbacks['send'](Messages.unknown_response)

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
        _logger.debug("Received a SHUTDOWN command")
        callbacks['send'](Messages.quit_response)
        callbacks['exit']()

    def _quit(self, callbacks, match):
        '''
        Wrapper around the client quit operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        _logger.debug("Received a QUIT command")
        self.statistics.clean_exits += 1
        callbacks['send'](Messages.quit_response)

    def _delete(self, callbacks, match):
        '''
        Wrapper around the queue delete operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        _logger.debug("Received a DELETE command")
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
        _logger.debug("Received a SET command")
        self.statistics.set_requests += 1

        key, flags, expire, length = (
            match.group(1), match.group(2), match.group(3), match.group(4))
        self.state = {
            'key': key, 'flags':int(flags),
            'expire':int(expire), 'length':int(length) }
        self.buffer = '' # start state machine

    def _set_data(self, callbacks, data):
        '''
        Wrapper around the actually setting the resulting data

        :param callbacks: The continuations to send the results to
        :param data: The next chunk of data to process
        :return: void
        '''
        self.buffer += data # what if we get overlapped commands ?
        if len(self.buffer) == self.state['length']:
            _logger.debug("Finishing SET command")
            compressed = pack(Messages.data_pack_format % (
                self.state['length'], self.state['flags'],
                self.state['expire'], self.buffer))
            if self.database.put(key, compressed):
                callbacks['send'](Messages.set_response_success)
            else: callbacks['send'](Messages.set_response_failure)
            self.buffer, self.state = ('', None) # reset

    def _get_next_message(self, key):
        '''
        Helper method to abstract away from getting the next valid
        non expired message out of a queue

        :param key: The key to retrieve the next message from
        :return: The next message, or None if none exist
        '''
        now = time.time()
        flag, result = None, None
        for message in iter(lambda: self.database.get(key), None):
            import pdb; pdb.set_trace()
            flag, expire, result = unpack(
                Messages.data_pack_format % (len(message) - 8), message)
            if expire == 0 or expire >= now:
                break
            self.exiprations[key] = 1 + self.expirations.get(key, 0)
            flag, expire, result = (None, None, None) # reset results
        return (flag, result)
    
    def _get(self, callbacks, match):
        '''
        Wrapper around the queue get operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        _logger.debug("Received a GET command")
        self.statistics.get_requests += 1

        key = match.group(1)
        (flag, data) = self._get_next_message(key)
        if data:
            callbacks['send'](Messages.get_response % (key, flags, len(data), data))
        else: callbacks['send'](Messages.get_response_empty)
    
    def _statistics(self, callbacks, match):
        '''
        Wrapper around the server statistics retrieval operation

        :param callbacks: The continuations to send the results to
        :param match: The parameters found for the command
        :return: void
        '''
        _logger.debug("Received a STATS command")
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
   
    # mamba get constants
    get_response          = "VALUE %s %s %s\r\n%s\r\nEND\r\n"
    get_response_empty    = "END\r\n"
   
    # mamba set constants
    set_response_success  = "STORED\r\n"
    set_response_failure  = "NOT STORED\r\n"
    set_client_data_error = "CLIENT_ERROR bad data chunk\r\nERROR\r\n"
   
    # mamba other constants
    delete_response       = "END\r\n"
    quit_response         = "END\r\n"
    unknown_response      = "CLIENT_ERROR bad command line format\r\n"
   
    # mamba statistics constants
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

    # mamba command regex collection
    _commands = {
        'get':        re.compile(r'^get (.{1,250})$'),
        'set':        re.compile(r'^set (.{1,250}) ([0-9]+) ([0-9]+) ([0-9]+)$'),
        'delete':     re.compile(r'^delete (.{1,250}) ([0-9]+)$'),
        'statistics': re.compile(r'^stats$'),
        'quit':       re.compile(r'^quit$'),
        'shutdown':   re.compile(r'^shutdown$'),
    }

    @staticmethod
    def get_commands():
        '''
        Helper method to return an iterator over the mamba commands
        as well as the callbacks processors used to evaluate the
        command results

        :return: The iterator for the command types
        '''
        for pair in Messages._commands.iteritems():
            yield pair

