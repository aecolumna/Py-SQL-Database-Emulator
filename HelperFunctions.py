import operator
from Database import Table, Database, Row


def orderValue(ranking):
    """ Helper function intended to be used in sorted(arr, key=FUNCTION) for sorting by different multiple parameters
    :param ranking: rank of indices by which to value a row in sorting
    :return: a function that returns the relevant row elements from a Row object
    """

    def rankingFunction(row):
        a = []
        for index in ranking:
            a.append(row.cleanedRow[index])
        return a

    return rankingFunction


functions = [operator.gt, operator.lt, operator.eq, operator.ne, operator.is_not, operator.is_]
# ISNOT deliberately has no space. Explanation is in project.py/select/ if WHERE
symbols = [">", "<", "=", "!=", "ISNOT", "IS"]
operatorDictionary = dict(zip(symbols, functions))


def distinct(table: Table, column):
    """ returns a filtered table where all rows are distinct """
    newTable = Table(table.name, table.schema)
    columnIndex = table.keyToPosDict[column]
    valueSet = set()
    for row in table:
        val = row[columnIndex]
        if val in valueSet:
            continue
        valueSet.add(val)
        newTable.append(row)

    return newTable



def where(table: Table, column, symbol, constant):
    """
    tuple = (column_name, operator, constant)
    SLECT * IS NOT NULL
    SELECT * WHERE id > 4;
     >, <,=, !=, IS NOT, IS.

      returns a filtered table
     """
    constant = None if constant == "NULL" else constant
    newTable = Table(table.name, table.schema)
    func = operatorDictionary[symbol]
    columnIndex = table.keyToPosDict[column]

    # for row in table: # suspicious
    #   row.cleanRow(table.schema) # suspicious

    for row in table:
        if row[columnIndex] is None and constant is not None:
            continue
        if func(row[columnIndex], constant):
            newTable.append(row)

    return newTable


def addTableName(tableName, colName):
    if colName.find(tableName) != 0:
        return tableName + "." + colName
    return colName


def leftJoinParse(tokens):
    """
    "SELECT students.name, students.grade, classes.course, classes.instructor
    FROM students LEFT OUTER JOIN classes
    ON students.class = classes.course
    WHERE students.grade > 60
    ORDER BY classes.instructor, students.name, students.grade;"
    """
    token = tokens.popleft()
    assert (token == "SELECT")

    fields = []

    token = tokens.popleft()
    # put fields into fields array
    while token != "FROM":

        if token != ',' and token != "FROM":
            fields.append(token)

        token = tokens.popleft()

    assert (token == "FROM")

    leftTableName = tokens.popleft()

    assert (tokens.popleft() == "LEFT")
    assert (tokens.popleft() == "OUTER")
    assert (tokens.popleft() == "JOIN")

    rightTableName = tokens.popleft()

    assert (tokens.popleft() == "ON")

    leftJoinCol = tokens.popleft()
    assert (tokens.popleft() == "=")
    rightJoinCol = tokens.popleft()

    assert (tokens.popleft() == "ORDER")
    assert (tokens.popleft() == "BY")

    orderByColumns = []

    token = tokens.popleft()

    while token != ";":

        if token != ',':
            orderByColumns.append(token)

        token = tokens.popleft()

    t = (fields, leftTableName, rightTableName, leftJoinCol, rightJoinCol, orderByColumns)
    return t
