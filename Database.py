"""
Andres Eduardo Columna
MSU CSE 480 Spring '19

Database, Table, Row classes
"""

from collections import OrderedDict
from sys import maxsize


class Database(dict):
    """ A dictionary of Table objects """

    def __init__(self):
        self.locks = dict()  # mapping connection-name-string to string of lock-type held!

    def __getitem__(self, tableName):
        """ Get table from table name """
        if tableName in self:
            return super().__getitem__(tableName)
        raise AssertionError('Table name does not exist')

    def __setitem__(self, tableName, table):
        """ Map table_name to table"""
        if tableName in self:
            raise AssertionError('Table {} already exists'.format(tableName))
        super().__setitem__(tableName, table)

    def removeLock(self, connectionName):
        """ removes locks assigned to a connection from database """
        if connectionName in self.locks:
            self.locks.pop(connectionName)

    def requestLock(self, connectionName, requestedLockType):
        """
        Checks if lockType can be granted. Raises error if it can't be.
        If succesful, returns string of the kind of lock that has been granted to the connection.
        Also sets the lock in the locks Database.locks dictionary
        """
        # if database has no locks, grant them since there's no constraints.
        if len(self.locks) == 0:
            self.locks[connectionName] = requestedLockType
            return requestedLockType

        # connection already has a lock to the current database
        elif connectionName in self.locks:

            currentLock = self.locks[connectionName]

            if currentLock == requestedLockType or currentLock == "EXCLUSIVE":  # already has the lock it's requesting
                return currentLock

            elif requestedLockType == 'SHARED' and currentLock in ['RESERVED', 'EXCLUSIVE']:
                # shared is a lower precedence lock
                return currentLock

            elif requestedLockType == 'EXCLUSIVE':

                if len(self.locks) == 1 and currentLock == 'RESERVED':
                    self.locks[connectionName] = requestedLockType
                    return 'EXCLUSIVE'
                else:
                    raise AssertionError('Cant grant exclusive lock because other locks already exist')

            elif requestedLockType == 'RESERVED':
                # make sure no other reserved or exclusive locks exist
                if 'RESERVED' in self.locks.values() or 'EXCLUSIVE' in self.locks.values():
                    raise AssertionError('Database already has exclusive and reserved locks')

                else:
                    self.locks[connectionName] = 'RESERVED'
                    return 'RESERVED'
            elif requestedLockType == 'SHARED':
                if 'EXCLUSIVE' in self.locks.values():
                    raise AssertionError('DB already has exclusive lock')

                else:
                    self.locks[connectionName] = 'SHARED'
                    return 'SHARED'

        # Connection has no locks
        elif connectionName not in self.locks:
            if requestedLockType == "SHARED":
                if 'EXCLUSIVE' in self.locks.values():
                    raise AssertionError('Database already has exclusive lock')
                else:
                    self.locks[connectionName] = requestedLockType
                    return 'SHARED'
            elif requestedLockType == 'RESERVED':
                if 'RESERVED' in self.locks.values():
                    raise AssertionError('RESERVED lock already exists')
                elif 'EXCLUSIVE' in self.locks.values():
                    raise AssertionError('Theres ALREADY AN EXCLUSIVE lock GTFO')
                else:
                    self.locks[connectionName] = requestedLockType
                    return True
            elif requestedLockType == 'EXCLUSIVE':
                if len(self.locks) != 0:
                    raise AssertionError('Cant grant exclusive locks because database still has active locks')
                else:
                    self.locks[connectionName] = requestedLockType
                    return True

        raise AssertionError('Should not have reached the end!')


class Table(list):
    """ A List of Row objects """

    def __init__(self, name: str, schema: OrderedDict = None) -> None:
        self.schema = schema
        self.name = name
        self.keyToPosDict = dict()

        # i.e. name goes first, age goes second, height third.
        for i, key in enumerate(schema.keys()):
            self.keyToPosDict[key] = i


class Row(list):
    """ A SQL row represented as a list (not tuple)"""

    def __init__(self, list=None):
        if list:  # if parameter passed in, that means initialize it with those values
            for i in list:
                self.append(i)
        self.deleteFlag = False  # if the flag is on, we should delete that row.

    def append(self, item):
        super().append(self.cleanString(item))

    def getVal(self, key, keyToPosDict):
        ''' Get value from column_name. Schema is stored in the Table object, so we need that too. '''
        return self[keyToPosDict[key]]

    def cleanRow(self, schema: OrderedDict) -> None:
        """ Remove all Nonetypes from row and replace with maximum value of the corresponding type.
        Needed for sorting reasons as None cannot be compared """

        self.cleanedRow = []
        types = list(schema.values())

        for i, el in enumerate(self):
            if el is not None:
                self.cleanedRow.append(el)
                continue

            objType = types[i]

            x = None

            if isinstance(objType, int):
                x = maxsize  # sys.maxsize is largest int

            elif isinstance(objType, float):
                x = float("inf")  # largest float

            elif isinstance(objType, str):
                x = "~"  # highest value char

            self.cleanedRow.append(x)

    @staticmethod
    def cleanString(string):
        """remove ` character. The way unescaped single quotes in sql queries are dealt with
         is by turning them into `. Upon appending to a row, we should revert and fix it.
         """

        if not isinstance(string, str):
            return string
        if string == "NULL" or string == "None":
            return None

        return string.replace("`", "'")
