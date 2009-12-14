import sys, unittest
from mamba.config import Options

class SimpleOptionsTest(unittest.TestCase):
    '''
    The unit test for the mamba.confg module
    '''

    def setUp(self):
        ''' Initializes the test environment '''
        self.file = 'extra/etc/sample-config.yml'
        self.commandline = '''mamba.py
            --host=127.0.0.1
            --port=22122
            --pid=/var/run/mamba.pid
            --queue=/tmp/mamba/spool
            --log=/var/log/mamba.log
            -v -d
        '''.split()

    def tearDown(self):
        ''' Cleans up the test environment '''
        pass

    def testConfigFile(self):
        '''
        Test that the configuration is loaded correctly
        from file.
        '''
        actual   = Options.ConfigFile(self.file)
        expected = {
            'host':      '127.0.0.1',
            'port':      22122,
            'pid_file':  '/var/run/mamba.pid',
            'path':      '/tmp/mamba/spool',
            'timeout':   0,
            'log_file':  '/var/log/mamba.log',
            'log_level': 1,
            'daemonize': True,
        }
        self.assertEqual(expected, actual)

    def testCommandLine(self):
        '''
        Test that the configuration is loaded correctly
        from the command line.
        '''
        old_argv, sys.argv = (sys.argv, self.commandline)
        actual = Options.CommandLine()
        expected = {
            'host':      '127.0.0.1',
            'port':      22122,
            'pid_file':  '/var/run/mamba.pid',
            'path':      '/tmp/mamba/spool',
            'log_file':  '/var/log/mamba.log',
            'log_level': 1,
            'daemonize': True,
        }
        sys.argv = old_argv
        self.assertEqual(expected, actual)

    def testConfig(self):
        '''
        Test that the configuration is loaded correctly
        from all sources.
        '''
        old_argv, sys.argv = (sys.argv, self.commandline)
        actual = Options.Config(self.file)
        expected = {
            'host':      '127.0.0.1',
            'port':      22122,
            'pid_file':  '/var/run/mamba.pid',
            'path':      '/tmp/mamba/spool',
            'timeout':   0,
            'log_file':  '/var/log/mamba.log',
            'log_level': 1,
            'daemonize': True,
        }
        sys.argv = old_argv
        self.assertEqual(expected, actual)

#---------------------------------------------------------------------------#
# Main
#---------------------------------------------------------------------------#
if __name__ == "__main__":
    unittest.main()
