
class AttributeDict(dict):
    '''
    A dictionary overloaded to provide attribute access
    '''

    def __init__(self, init={}):
        '''
        Initializes a new dictionary instance

        :param init: The default values for the dict
        '''
        dict.__init__(self, init)

    def __setitem__(self, key, value):
        ''' Overload of item setter for the dict

        :param key: The key to set a value at
        :param value: The value to set at key
        :return: The new value being set
        '''
        return super(AttributeDict, self).__setitem__(key, value)

    def __getitem__(self, key):
        ''' Overload of item getter for the dict

        :param key: The key to retrieve the value at
        :return: The value at the given key
        '''
        return super(AttributeDict, self).get(key, 0)

    def __delitem__(self, key):
        ''' Overload of item delete for the dict

        :param key: The key to retrieve the value at
        :return: The value at the given key
        '''
        super(AttributeDict, self).__delitem__(key)

    # our attribute accessors
    __getattr__ = __getitem__
    __setattr__ = __setitem__
    __delattr__ = __delitem__

