import sys, unittest
from mamba.attr import AttributeDict

class SimpleAttributeDictTest(unittest.TestCase):
    '''
    The unit tests for the mamba.attr module
    '''

    def setUp(self):
        ''' Initializes the test environment '''
        pass

    def tearDown(self):
        ''' Cleans up the test environment '''
        pass

    def testDictToProperty(self):
        '''
        Test that we can go from index to attr
        '''
        handle = AttributeDict()
        handle['name1'] = 1
        handle['name2'] += 1
        self.assertEqual(handle.name1, 1)
        self.assertEqual(handle.name2, 1)
        del handle['name1']
        del handle['name2']
        self.assertEqual(handle.keys(), [])

    def testPropertyToDict(self):
        '''
        Test that we can go from attr to index
        '''
        handle = AttributeDict()
        handle.name1 = 1
        handle.name2 += 1
        self.assertEqual(handle['name1'], 1)
        self.assertEqual(handle['name1'], 1)
        del handle.name1
        del handle.name2
        self.assertEqual(handle.keys(), [])

#---------------------------------------------------------------------------#
# Main
#---------------------------------------------------------------------------#
if __name__ == "__main__":
    unittest.main()
