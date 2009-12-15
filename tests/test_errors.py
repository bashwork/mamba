import sys, unittest
from mamba.errors import *

class SimpleErrorsTest(unittest.TestCase):
    '''
    The unit tests for the mamba.errors module
    '''

    def setUp(self):
        ''' Initializes the test environment '''
        pass

    def tearDown(self):
        ''' Cleans up the test environment '''
        pass

    def testMambaException(self):
        '''
        Test that we can throw and catch our exception
        '''
        expected = MambaException
        try:
            raise expected("result")
        except Exception, ex:
            self.assertEqual(type(ex), type(expected()))

    def testTransactionLogException(self):
        '''
        Test that we can throw and catch our exception
        '''
        expected = TransactionLogException
        try:
            raise expected("result")
        except Exception, ex:
            self.assertEqual(type(ex), type(expected()))

    def testQueueCollectionException(self):
        '''
        Test that we can throw and catch our exception
        '''
        expected = QueueCollectionException
        try:
            raise expected("result")
        except Exception, ex:
            self.assertEqual(type(ex), type(expected()))

#---------------------------------------------------------------------------#
# Main
#---------------------------------------------------------------------------#
if __name__ == "__main__":
    unittest.main()
