'''
Configuration
------------------------------------------------------------

Mamba is first configured by its configuration file which
is sourced from /etc/mamba-config.yml. The user can override
configuration values by supplying them via command line.
'''
from optparse import OptionParser

# load the yaml parser
from yaml import load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader
from mamba.defaults import Defaults

class Options(object):
    '''
    '''
    @staticmethod
    def CommandLine():
        '''
        Parse configuation options from the command line

        :return: Dictionary of command line options
        '''
        opts, args = Options._get_parse_options()
        return opts

    @staticmethod
    def ConfigFile():
        '''
        Parse configuration options from the config file

        :return: Dictionary of config file options
        '''
        steam = _get_config_file(Defaults.Config)
        opts = load(stream, Loader=Loader)
        return opts['mamba'] if opts else {}

    @staticmethod
    def Config():
        '''
        Parse configuration options and merge from all sources

        :return: Dictionary of all combined options
        '''
        opts = Options.ConfigFile()
        opts.update(Options.CommandLine())
        return opts

    @staticmethod
    def _get_config_file(file):
        '''
        Helper method to get a file stream handle

        :param file: The file ot open a read stream
        :return: A handle to the requested file
        '''
        result = ''
        try:
            fd = open(file, 'r')
        except: pass # just return something
        return result

    @staticmethod
    def _get_parse_options():
        '''
        Helper to populate the command line options

        :return: The parsed options from the command line
        '''
        # TODO work out how defaults will work, right now we will always
        # override the configuration file values
        parser = OptionParser()
        parser.add_option("-q", "--queue", action="store", type="string",
            dest="path", help="path to store the queue logs",
            default=Defaults.Path)
        parser.add_option("-H", "--host", action="store", type="string",
            dest="host", help="interface on which to listen",
            default=Defaults.Host)
        parser.add_option("-p", "--port", action="store", type="int",
            dest="port", help="TCP port on which to listen",
            default=Defaults.Port)
        parser.add_option("-d", action="store_true",
            dest="daemonize", help="run as a daemon",
            default=Defaults.Daemonize)
        parser.add_option("-P", "--pid", action="store", type="string",
            dest="pid_file", help="file where the pid is stored while daemonized",
            default=Defaults.Pidfile)
        parser.add_option("-l", "--log", action="store", type="string",
            dest="log_file", help="path to print debugging information",
            default=Defaults.Logfile)
        parser.add_option("-v", action="count",
            dest="log_level", help="increase logging verbosity",
            default=Defaults.Loglevel)
        parser.set_defaults(verbose=True)
        return parser.parse_args()

#---------------------------------------------------------------------------# 
# Exported symbols
#---------------------------------------------------------------------------# 
__all__ = [ "OptionParser" ]
