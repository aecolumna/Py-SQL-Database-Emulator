"""
Andres Eduardo Columna
MSU CSE 480 Spring '19

Contains Database connector classes meant to emulate SQLITE3 python module
"""

from collections import deque, OrderedDict
from copy import deepcopy
from Database import Table, Database, Row
from HelperFunctions import orderValue, where, addTableName, distinct, leftJoinParse
from Tokenize import tokenize


class Connection(object):
    _ALL_DATABASES = {}  # Global Dictionary of all databases available

    def __init__(self, filename="test.db", timeout=0.1, isolation_level=None):

        self.filename = filename
        
        if filename in Connection._ALL_DATABASES:
            self.db = Connection._ALL_DATABASES[filename]
        else:
            self.db = Database()  # create database
            Connection._ALL_DATABASES[filename] = self.db  # make link between Database and Connection object

        self.timeout = timeout
        self.isolation_level = isolation_level

        import random
        import string

        # need a string to map connection to locks held in database object.
        self.name = ''.join(
            random.SystemRandom().choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for i in
            range(10))

        self.commitMode = 'AUTOCOMMIT'  # default commit mode
        self.lock = None  # default lock

    def getDatabase(self):
        return self.db

    def close(self):
        """ Delete this as this is just an expedient to make the tests work seamlessly """
        for db in Connection._ALL_DATABASES:
            del db



    def checkout(self):
        """
        makes global db dict point at db2. db is now a deep copy. Make global dict point db to make changes permanent
        """
        self.db = deepcopy(Connection._ALL_DATABASES[self.filename]) # make copy of global database

    @staticmethod
    def encode(message):
        """
            Deals with unescaped single quotes by changing them to ` symbol
            and avoids delimiting single quotes from being incorrectly escaped.
            The overlaoded Row.append method unescapes it after the message has been
            tokenized.
          """
        if message.find("''") != -1:  # if no double quotes found
            message = message.replace(" ('", " (^")
            message = message.replace(", '", ", ^")

            message = message.replace("''", "`")  # apostrophes part of string will be `

            message = message.replace(" (^", " ('")
            message = message.replace(", ^", ", '")
        return message

    def createTable(self, tokens: deque) -> list:
        """ conn.execute("CREATE TABLE myTable(name TEXT)")
            CREATE TABLE IF NOT EXISTS students
         """

        assert (tokens.popleft() == "CREATE")
        assert (tokens.popleft() == "TABLE")

        ifNotExists = False  # Does sql statement have CREATE TABLE 'IF NOT EXISTS'?

        if tokens[0] == "IF" and tokens[1] == "NOT" and tokens[2] == "EXISTS":
            ifNotExists = True
            tokens.popleft()
            tokens.popleft()
            tokens.popleft()

        tableName = tokens.popleft()

        # short circuit if table already exists and IF NOT EXISTS
        if ifNotExists is True and tableName in self.db:
            return

        assert (tokens.popleft() == "(")

        typesDict = {"TEXT": str, "INTEGER": int, "REAL": float, "NULL": None}

        # maps variable title to the sqlite type's equivalent in python i.e. {name --> string, age --> integer}
        # order is important to determine the supposed type of a value while it's being inserted as a row
        schema = OrderedDict()

        while True:
            token = tokens.popleft()
            if token in (')', ';'):
                break

            elif token == ',':
                continue

            else:
                tokenType = typesDict[tokens.popleft()]
                token = tableName + "." + token
                schema[token] = tokenType

        self.db[tableName] = Table(tableName, schema)

        return []  # SQL CREATE returns an empty list

    def dropTable(self, tokens: deque) -> list:
        """DROP TABLE students;"""
        assert (tokens.popleft() == "DROP")
        assert (tokens.popleft() == "TABLE")

        ifExists = False  # Does sql statement have CREATE TABLE 'IF NOT EXISTS' students

        if tokens[0] == "IF" and tokens[1] == "EXISTS":
            ifExists = True
            tokens.popleft()
            tokens.popleft()

        tableName = tokens.popleft()

        if tableName in self.db:
            self.db.pop(tableName)
        elif ifExists is True:
            return
        else:
            raise AssertionError("Table Name '{}' does not exist!".format(tableName))

    def insertHelper(self, tokens):
        """
        The second type of insert, where the order of insertions doesn't match schema e.g.,
        INSERT INTO students (name, grade) VALUES ('Josh', 3.7), ('Tyler', 2.5), ('Hangchen', 3.9);
        """
        _, _, tableName, *tokens = tokens
        tokens = deque(tokens)
        table = self.db[tableName]

        token = tokens.popleft()  # (
        fields = []
        while token != ")":
            if token.isalnum():
                fields.append(token)
            token = tokens.popleft()

        # Standardize all Values to tableName.attribute
        for i, field in enumerate(fields):
            fields[i] = addTableName(tableName, field)

        # denotes at what index should an element be placed in the final row given the insertion order specified.
        fieldIndices = [table.keyToPosDict[field] for field in fields]

        token = tokens.popleft()
        assert (token == "VALUES")

        while token != ";":
            token = tokens.popleft()  # (
            rowList = len(table.schema) * [None]
            i = 0  # for field indices
            while token != ")":
                if token != "," and token != "(":
                    rowList[fieldIndices[i]] = token
                    i += 1
                token = tokens.popleft()
            r = Row(rowList)
            table.append(r)
            token = tokens.popleft()
        return []

    def insert(self, tokens):
        """
            conn.execute("INSERT INTO students VALUES (3, 'hi', 4.5);")
            conn.execute("INSERT INTO students VALUES (7842, 'string with spaces', 3.0);")
            conn.execute("INSERT INTO students VALUES (7, 'look a null', NULL);")
            INSERT INTO students (name, grade) VALUES ('Josh', 3.7), ('Tyler', 2.5), ('Hangchen', 3.9);
            INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');
         """
        from copy import deepcopy
        tokensCopy = deepcopy(tokens)

        assert (tokens.popleft() == "INSERT")
        assert (tokens.popleft() == "INTO")

        tableName = tokens.popleft()

        table = self.db[tableName]

        token = tokens.popleft()

        # Take the route where the insert syntax is different
        # INSERT INTO students (name, grade) VALUES ('Josh', 3.7), ('Tyler', 2.5), ('Hangchen', 3.9);
        if token == "(":
            return self.insertHelper(tokensCopy)

        assert (token == "VALUES")

        assert (tokens.popleft() == "(")

        # Populate Row
        while token != ";":
            row = Row()
            while True:
                token = tokens.popleft()

                if token == ')' or token == ';':
                    break

                elif token == ',' or token == '(':
                    continue

                else:
                    row.append(token)
            # append Row to table
            table.append(row)
            token = tokens.popleft()

        return []

    def select(self, tokens):
        """
        SELECT qty, symbol FROM stocks ORDER BY price, qty;
        "SELECT * FROM table WHERE two > 50 ORDER BY three, two, one;"
        SELECT DISTINCT column_name FROM ... ORDER BY column_name
        """

        assert (tokens.popleft() == "SELECT")

        fields = []  # the fields the select statement wants in return
        descending = False  # Flag whether ordering will be ascending or descending

        distinctFlag = False  # Whether we will only include unique values in our select

        if tokens[0] == "DISTINCT":
            distinctFlag = True
            tokens.popleft()

        while True:
            token = tokens.popleft()

            if token == ",":
                continue
            elif token == "FROM":
                break

            fields.append(token)

        tableName = tokens.popleft()

        # check2 out the latest version of the database!
        table = self.db[tableName]  # retrieve reference to table from Database dict

        # standardize column names e.g. col1 --> table.col1
        for i, f in enumerate(fields):
            fields[i] = addTableName(tableName, f)

        # Expand Asterisks e.g. * --> table.col1, table.col2, table.col3
        i = 0
        while i < len(fields):
            if fields[i] == tableName + ".*":
                fields.pop(i)
                fields[i:i] = table.schema.keys()
                i += len(table.schema.keys())
                continue
            i += 1

        token = tokens.popleft()

        if token == "WHERE":
            columnName = tokens.popleft()
            columnName = addTableName(tableName, columnName)

            symbol = tokens.popleft()
            nextToken = tokens.popleft()

            if nextToken in ("NOT", "="):
                symbol = symbol + nextToken
                constant = tokens.popleft()
            else:
                constant = nextToken

            table = where(table, columnName, symbol, constant)
            token = tokens.popleft()

        if token == "ORDER":
            tokens.popleft()

            orderFields = []

            while True:
                token = tokens.popleft()

                if token == ";":
                    break
                if token == ",":
                    continue
                elif token == "DESC":
                    descending = True
                    break
                token = addTableName(tableName, token)

                orderFields.append(token)
            # Map columnName to its place in final ordering
            for i in range(len(orderFields)):
                orderFields[i] = table.keyToPosDict[orderFields[i]]

            for row in table:
                row.cleanRow(table.schema)

            # function orderValue doing a lot of heavy lifting
            table.sort(key=orderValue(orderFields), reverse=descending)

        fieldsToKeep = [table.keyToPosDict[field] for field in fields]

        if distinctFlag is True:
            table = distinct(table, fields[0])

        filteredSortedList = []

        # Convert lists to tuples as per instructions
        for row in table:
            filteredSortedList.append(tuple(row[i] for i in fieldsToKeep))

        return filteredSortedList

    def update(self, tokens):
        """UPDATE table_name SET col1 = value1, col2 = value2;
           UPDATE student SET grades=4.0 WHERE name = 'Josh';
        """
        assert (tokens.popleft() == "UPDATE")
        tableName = tokens.popleft()
        table = self.db[tableName]
        assert (tokens.popleft() == "SET")

        token = tokens.popleft()

        updateDict = {}

        while token != ";" and token != "WHERE":

            colName = addTableName(tableName, token)
            assert (tokens.popleft() == "=")
            val = tokens.popleft()
            updateDict[colName] = val

            token = tokens.popleft()
            if token == ",":
                token = tokens.popleft()

        if token == "WHERE":
            columnName = addTableName(tableName, tokens.popleft())

            symbol = tokens.popleft()
            nextToken = tokens.popleft()

            if nextToken in ("NOT", "="):
                symbol = symbol + nextToken
                constant = tokens.popleft()
            else:
                constant = nextToken

            table = where(table, columnName, symbol, constant)
            token = tokens.popleft()

        for row in table:
            for key, val in updateDict.items():
                row[table.keyToPosDict[key]] = val

    def delete(self, tokens):
        """
        DELETE FROM students;
        DELETE FROM students WHERE id > 4;
        """
        assert (tokens.popleft() == "DELETE")
        assert (tokens.popleft() == "FROM")

        tableName = tokens.popleft()

        table = self.db[tableName]
        token = tokens.popleft()

        # DELETE FROM table; case
        if token == ";":
            table.clear()
            return []

        assert (token == "WHERE")

        columnName = tokens.popleft()
        columnName = addTableName(tableName, columnName)

        symbol = tokens.popleft()
        nextToken = tokens.popleft()

        if nextToken in ("NOT", "="):
            symbol = symbol + nextToken
            constant = tokens.popleft()
        else:
            constant = nextToken

        # toggle flags of rows to delete
        deleteTable = where(table, columnName, symbol, constant)  # newTable contains all rows to delete
        for row in deleteTable:
            row.deleteFlag = True

        rowsToKeep = []

        # collect all rows to keep
        for row in table:
            if row.deleteFlag is False:
                rowsToKeep.append(row)
        # delete all rows
        table.clear()

        # append all rows to keep to table
        table.extend(rowsToKeep)

        return []

    def begin(self, tokens):

        # check2 if begin has already been called
        if self.commitMode in ['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE']:
            raise AssertionError('cannot begin because it already began!')

        # checkout database from global
        self.checkout()
        commitMode = tokens[1]  # might be equal to transaction, which indicates deferred

        if tokens[0] == "BEGIN" and (tokens[1] == "TRANSACTION" or tokens[1] == 'DEFERRED'):
            self.commitMode = "DEFERRED"

        else:
            assert (commitMode in ['DEFERRED', 'IMMEDIATE', 'EXCLUSIVE', 'AUTOCOMMIT'])

        if commitMode == "IMMEDIATE":  # that lock is handed immediately!
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, "RESERVED")
            self.commitMode = commitMode
        elif commitMode == "EXCLUSIVE":
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, "EXCLUSIVE")
            self.commitMode = commitMode

    def publishChanges(self):
        " Syncs the global database with the connection database, making changes permanent"

        # merge the locks from the global database with the locks from the local database that we will be committing to.
        for name, lock in Connection._ALL_DATABASES[self.filename].locks.items():
            if name not in self.db:
                self.db.locks[name] = lock

        self.db.removeLock(self.name)  # remove lock after commitment
        Connection._ALL_DATABASES[self.filename].removeLock(self.name)  # remove them from both just in case
        self.lock = None
        Connection._ALL_DATABASES[self.filename] = self.db # publish

    def commit(self, tokens):
        if self.commitMode == "AUTOCOMMIT":
            raise AssertionError('cannot commit because you never started. youre in Autocommit mode')

        elif self.commitMode == 'EXCLUSIVE':
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, 'EXCLUSIVE')  # just making sure

        elif self.commitMode == 'IMMEDIATE':
            # request an exclusive lock since you only have a reserved
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, "EXCLUSIVE")

        elif self.commitMode == 'DEFERRED' and self.lock == 'RESERVED':
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, 'EXCLUSIVE')

        self.publishChanges()
        self.commitMode = "AUTOCOMMIT"
        return []

    def rollback(self, tokens):
        # ensure you're not in autocommit mode
        assert self.commitMode != 'AUTOCOMMIT', 'cannot rollback in autocommit mode'

        Connection._ALL_DATABASES[self.filename].removeLock(self.name)
        self.commitMode = 'AUTOCOMMIT'
        self.lock = None
        self.db = Connection._ALL_DATABASES[self.filename] # reset database copy
        return []

    def execute(self, message):
        """
        :param message: sql statement string
        :return: list of tuples of rows. Or blank, if it's a sql statement that doesn't return anything
        """

        from collections import deque

        message = self.encode(message)

        tokens = tokenize(message)

        tokens = deque(tokens)

        ### check2 if the proper locks are held

        if self.commitMode == 'AUTOCOMMIT':
            # checkout the latest version of the database
            self.checkout()

        returnObject = None

        if tokens[0] == "BEGIN":
            self.begin(tokens)

        elif tokens[0] == "COMMIT":
            self.commit(tokens)

        elif tokens[0] == "CREATE":
            returnObject = self.createTable(tokens)

        elif tokens[0] == "DROP":
            returnObject = self.dropTable(tokens)

        elif tokens[0] == "INSERT":
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, 'RESERVED')
            returnObject = self.insert(tokens)

        elif tokens[0] == "UPDATE":
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, 'RESERVED')
            returnObject = self.update(tokens)

        elif tokens[0] == "DELETE":
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, 'RESERVED')
            returnObject = self.delete(tokens)

        elif message.find("LEFT OUTER JOIN") != -1:
            returnObject = self.leftOuterJoin(tokens)

        elif tokens[0] == "SELECT":
            self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, 'SHARED')
            returnObject = self.select(tokens)

        elif tokens[0] == "ROLLBACK":
            returnObject = self.rollback(tokens)

        # if in autocommit mode, then commit by updating the global database
        if self.commitMode == "AUTOCOMMIT":
            if self.lock in ['RESERVED', 'EXCLUSIVE']:
                self.lock = Connection._ALL_DATABASES[self.filename].requestLock(self.name, 'EXCLUSIVE')

            self.publishChanges()

        return returnObject if returnObject is not None else []

    def executeHelper(self, message):
        """
        ad hoc for the left outer join bit the uses it which requires the commit deepcopying to be turned off!
        """

        from collections import deque

        message = self.encode(message)

        tokens = tokenize(message)

        tokens = deque(tokens)

        if tokens[0] == "CREATE":
            self.createTable(tokens)

        elif tokens[0] == "INSERT":
            self.insert(tokens)

        elif tokens[0] == "UPDATE":
            return self.update(tokens)

        elif tokens[0] == "DELETE":
            return self.delete(tokens)

        elif message.find("LEFT OUTER JOIN") != -1:
            return self.leftOuterJoin(tokens)

        elif tokens[0] == "SELECT":
            return self.select(tokens)

        return []  # returns empty list is select statement was made, otherwise returns list of tuples. convert rows from lists to tuples

    def leftOuterJoin(self, tokens):
        """
        "SELECT students.name, students.grade, classes.course, classes.instructor
        FROM students LEFT OUTER JOIN classes
        ON students.class = classes.course
        WHERE students.grade > 60
        ORDER BY classes.instructor, students.name, students.grade;"
        """

        whereArr = []
        if "WHERE" in tokens:
            tokens = list(tokens)
            whereIndex = tokens.index("WHERE")
            orderIndex = tokens.index("ORDER")
            whereArr = tokens[whereIndex: orderIndex]
            del tokens[whereIndex: orderIndex]
            tokens = deque(tokens)

        # PARSING STRING FIRST
        fields, leftTableName, rightTableName, leftJoinCol, rightJoinCol, orderByColumns = leftJoinParse(tokens)

        # Make deep copies of left and right tables
        leftTable: Table = deepcopy(self.db[leftTableName])
        rightTable: Table = deepcopy(self.db[rightTableName])

        newTableName = "joinedTable"
        newSchema = OrderedDict()

        for key, value in leftTable.schema.items():
            newKeyName = newTableName + "." + key
            newSchema[newKeyName] = value

        for key, value in rightTable.schema.items():
            newKeyName = newTableName + "." + key
            newSchema[newKeyName] = value

        newTable = Table(newTableName, newSchema)
        self.db[newTableName] = newTable

        lkeyToPosDict, rkeyToPosDict = leftTable.keyToPosDict, rightTable.keyToPosDict

        rightNones = len(rightTable[0]) * [None]

        # populate new table
        for lrow in leftTable:
            row = Row()

            if lrow.getVal(leftJoinCol, lkeyToPosDict) is None:
                row.extend(lrow + rightNones)
                newTable.append(row)
                continue
            for rrow in rightTable:
                if lrow.getVal(leftJoinCol, lkeyToPosDict) == rrow.getVal(rightJoinCol, rkeyToPosDict):
                    row.extend(lrow + rrow)
                    newTable.append(row)  # needs fixing, maybe?
                    break  # there might be multiple tables with same matching id
            else:
                row.extend(lrow + rightNones)
                newTable.append(row)
                continue

        for i, field in enumerate(fields):
            fields[i] = addTableName(newTableName, field)

        for i, field in enumerate(orderByColumns):
            orderByColumns[i] = addTableName(newTableName, field)

        fieldsString = ", ".join(fields)
        orderString = ", ".join(orderByColumns)
        whereString = ""
        if whereArr:
            whereArr[1] = addTableName(newTableName, whereArr[1])
            whereArr = [str(i) for i in whereArr]  # maybe switch none to NULL
            whereString = " ".join(whereArr)

        query = " SELECT {} FROM {} {} ORDER BY {};".format(fieldsString, newTableName, whereString, orderString)

        result = self.executeHelper(query)

        # delete the table we created
        self.db.pop(newTableName)
        return result


def connect(filename="file.txt", timeout=0.1, isolation_level=None):
    """ returns connection object which contains reference to database"""
    connection = Connection(filename, timeout, isolation_level)  # create connection object
    return connection
