import sys, unittest
from mamba.defaults import Defaults

class SimpleDefaultsTest(unittest.TestCase):
    '''
    The unit test for the mamba.defaults module
    '''

    def setUp(self):
        ''' Initializes the test environment '''
        pass

    def tearDown(self):
        ''' Cleans up the test environment '''
        pass

    def testSystemDefaults(self):
        '''
        Test that the detaults are just that
        '''
        self.assertEqual(Defaults.Host, "127.0.0.1")
        self.assertEqual(Defaults.Port, 22122)
        self.assertEqual(Defaults.Timeout, 60)
        self.assertEqual(Defaults.Daemonize, False)
        self.assertEqual(Defaults.Config, "/etc/mamba.conf")
        self.assertEqual(Defaults.Path, "/tmp/starling")
        self.assertEqual(Defaults.Pidfile, "/var/run/mamba.pid")
        self.assertEqual(Defaults.Logfile, "/var/log/mamba.log")
        self.assertEqual(Defaults.Loglevel, 0)

#---------------------------------------------------------------------------#
# Main
#---------------------------------------------------------------------------#
if __name__ == "__main__":
    unittest.main()
