"""
Andres Eduardo Columna
MSU CSE 480 Spring '19

Contains Tokenize classes for tokenizing SQL queries
"""

import string


def collect_characters(query, allowed_characters):
    """
    @query: string
    @allowed_characters: a container type

    return characters from string that are in allowed character. Stop looking once an invalid character is read"""
    letters = []
    allowed_characters = set(allowed_characters)
    for letter in query:
        if letter not in allowed_characters:
            break
        else:
            letters.append(letter)

    return "".join(letters)


def remove_whitespace(query, tokens):
    return query.lstrip()


def remove_word(query, tokens):
    word = collect_characters(query, string.ascii_letters + "_.*" + string.digits)
    if word == "NULL" or word == "Null" or word == "None" or word is None:
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    assert (query[0] == "'")

    query = query[1:]

    index = query.find("'")

    if query and query[0] == "'":
        s = ''
        tokens.append(s)
        return query[2:]

    s = query[:index]
    tokens.append(s)
    return query[len(s) + 1:]


def remove_number(query, tokens):
    index = 0
    strSet = string.digits + "."
    for x, i in enumerate(query):
        if i in strSet:
            continue
        else:
            index = x
            break
    s = query[:index]

    if "." in s:  # a decimal number
        tokens.append(float(s))
    else:
        tokens.append(int(s))

    query = query[index:]
    return query


def tokenize(query: str) -> object:
    tokens = []

    while query:

        old_query = query

        if query[0] in string.whitespace:
            query = query.lstrip()
            continue

        if query[0] in string.ascii_letters + "_":
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*><=!":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query) and query:
            raise AssertionError("TOKENIZE MESSED UP!")

    return tokens
