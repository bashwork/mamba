'''
Configuration
------------------------------------------------------------

Mamba is first configured by its configuration file which
is sourced from /etc/mamba-config.yml. The user can override
configuration values by supplying them via command line.
'''
import yaml
from optparse import OptionParser
from mamba.defaults import Defaults

class Options(object):
    '''
    Helper class to abstract away reading options
    from command line and configuration files.
    '''

    @staticmethod
    def CommandLine():
        '''
        Parse configuation options from the command line

        :return: Dictionary of command line options
        '''
        opts, args = Options._get_parse_options()
        return opts.__dict__

    @staticmethod
    def ConfigFile(file=Defaults.Config):
        '''
        Parse configuration options from the config file

        :param file: The config file to parse or default
        :return: Dictionary of config file options
        '''
        try:
            with open(file) as stream:
                options = yaml.load(stream)
        except: options = None
        return options['mamba'] if options else {}

    @staticmethod
    def Config(file=Defaults.Config):
        '''
        Parse configuration options and merge from all sources

        :param file: The config file to parse or default
        :return: Dictionary of all combined options
        '''
        opts = Options.ConfigFile(file)
        opts.update(Options.CommandLine())
        return opts

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
        parser.add_option("-t", "--time", action="store", type="int",
            dest="timeout", help="The default timeout for entries",
            default=Defaults.Timeout)
        return parser.parse_args()

#---------------------------------------------------------------------------# 
# Exported symbols
#---------------------------------------------------------------------------# 
__all__ = [ "OptionParser" ]
