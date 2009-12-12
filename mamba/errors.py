'''
Mamba Exceptions
-------------------------------------------------

The following are the exceptions used throughout the
mamba queue library.
'''

class MambaException(Exception):
    '''
    Base exception for all exceptions in the mamba message
    queue library.
    '''
    pass

class TransactionLogException(MambaException):
    '''
    Exception that is thrown when there is undefined behaviour
    with the persisent queue transaction log.
    '''
    pass

class QueueCollectionException(MamabaException):
    '''
    Exception that is thrown when there is undefined behaviour
    with the queue collection
    '''
    pass

