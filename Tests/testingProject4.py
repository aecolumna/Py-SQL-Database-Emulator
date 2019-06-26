import project
from pprint import pprint
import Tests.testing

def raisesException(conn, statement):
    x = "no"
    try:
        conn.execute(statement)
    except AssertionError as w:
        print("Successfully threw error")
        x = "yes"
    finally:
        if x != "yes":
            raise AssertionError("Exception was not thrown!")


import project
from pprint import pprint


def check(sql_statement, expected):
    print("SQL: " + sql_statement)
    result = conn.execute(sql_statement)
    result_list = list(result)

    print("expected:")
    pprint(expected)
    print("student: ")
    pprint(result_list)
    assert expected == result_list


conn = project.connect("test.db")
conn.execute("CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
conn.execute("CREATE TABLE owners (name TEXT, age INTEGER, id INTEGER);")
conn.execute(
    "INSERT INTO pets VALUES ('RaceTrack', 'Ferret', 3), ('Ghost', 'Ferret', 2), ('Zoe', 'Dog', 7), ('Ebony', 'Dog', 17);")
conn.execute("INSERT INTO pets (species, name) VALUES ('Rat', 'Ginny'), ('Dog', 'Balto'), ('Dog', 'Clifford');")

conn.execute("UPDATE pets SET age = 15 WHERE name = 'RaceTrack';")

check(
    "SELECT species, *, pets.name FROM pets WHERE age > 3 ORDER BY pets.name;",
    [('Dog', 'Ebony', 'Dog', 17, 'Ebony'),
     ('Ferret', 'RaceTrack', 'Ferret', 15, 'RaceTrack'),
     ('Dog', 'Zoe', 'Dog', 7, 'Zoe')]
)

conn.execute("INSERT INTO owners VALUES ('Josh', 29, 10), ('Emily', 27, 2), ('Zach', 25, 4), ('Doug', 34, 5);")
conn.execute("DELETE FROM owners WHERE name = 'Doug';")
check(
    "SELECT owners.* FROM owners ORDER BY id;",
    [('Emily', 27, 2), ('Zach', 25, 4), ('Josh', 29, 10)]
)

conn.execute("CREATE TABLE ownership (name TEXT, id INTEGER);")
conn.execute("INSERT INTO ownership VALUES ('RaceTrack', 10), ('Ginny', 2), ('Ghost', 2), ('Zoe', 4);")

check(
    "SELECT pets.name, pets.age, ownership.id FROM pets LEFT OUTER JOIN ownership ON pets.name = ownership.name WHERE pets.age IS NULL ORDER BY pets.name;",
    [('Balto', None, None), ('Clifford', None, None), ('Ginny', None, 2)]

    )

import project
from pprint import pprint


def check(conn, sql_statement, expected):
    print("SQL: " + sql_statement)
    result = conn.execute(sql_statement)
    result_list = list(result)

    print("expected:")
    pprint(expected)
    print("student: ")
    pprint(result_list)
    assert expected == result_list


conn_1 = project.connect("test.db", timeout=0.1, isolation_level=None)
conn_2 = project.connect("test.db", timeout=0.1, isolation_level=None)
conn_3 = project.connect("test.db", timeout=0.1, isolation_level=None)
conn_4 = project.connect("test.db", timeout=0.1, isolation_level=None)
conn_5 = project.connect("test.db", timeout=0.1, isolation_level=None)

conn_1.execute("CREATE TABLE students (name TEXT, id INTEGER);")
conn_2.execute("CREATE TABLE grades (grade INTEGER, name TEXT, student_id INTEGER);")

conn_3.execute("INSERT INTO students (id, name) VALUES (42, 'Josh'), (7, 'Cam');")
conn_2.execute("INSERT INTO grades VALUES (99, 'CSE480', 42), (80, 'CSE450', 42), (70, 'CSE480', 9);")

conn_2.execute("BEGIN DEFERRED TRANSACTION;")
conn_1.execute("BEGIN IMMEDIATE TRANSACTION;")
conn_1.execute("INSERT INTO grades VALUES (10, 'CSE231', 1);")
check(conn_2,
      "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
      [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
      )
check(conn_1,
      "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
      [(10, 'CSE231', None),
       (80, 'CSE450', 'Josh'),
       (70, 'CSE480', None),
       (99, 'CSE480', 'Josh')]
      )
conn_2.execute("COMMIT TRANSACTION;")
check(conn_2,
      "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
      [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
      )


conn_1.execute("ROLLBACK TRANSACTION;")
check(conn_1,
      "SELECT grades.grade, grades.name, students.name FROM grades LEFT OUTER JOIN students ON grades.student_id = students.id ORDER BY grades.name, grades.grade;",
      [(80, 'CSE450', 'Josh'), (70, 'CSE480', None), (99, 'CSE480', 'Josh')]
      )
conn_1.execute("DROP TABLE IF EXISTS other;")
conn_3.execute("INSERT INTO students VALUES ('Zach', 732);")
check(conn_4, "SELECT name FROM students WHERE name > 'A' ORDER BY name;",
      [('Cam',), ('Josh',), ('Zach',)]
      )


print('\nsuccess!')