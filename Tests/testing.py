
import project

testNumber = 0

def check2(sql_statement, expected):
    global testNumber
    print(" \u2705", end="")
    print("{}.SQL: {}".format(testNumber, sql_statement))

    result = conn.execute(sql_statement)
    result_list = list(result)

    # print("expected:", end=": ")
    # pprint(expected)
    # print("student: ", end=": ")
    # pprint(result_list)
    assert expected == result_list

    testNumber += 1

#print("Regression Tests\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table_1 (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
conn.execute("INSERT INTO table_1 VALUES (33, 'hi', 4.5);")
conn.execute("CREATE TABLE table2 (col_1 INTEGER, other INTEGER);")
conn.execute("INSERT INTO table2 VALUES (15, 782);")
conn.execute("INSERT INTO table2 VALUES (615, 7582);")
check2(
    "SELECT * FROM table_1 ORDER BY _col2, col_1;",
    [(33, 'hi', 4.5)]
)
check2("SELECT * FROM table2 ORDER BY other, col_1;",
       [(15, 782), (615, 7582)]
       )
conn.execute("INSERT INTO table_1 VALUES (3, 'hi', 4.5);")
conn.execute("INSERT INTO table2 VALUES (165, 7282);")
conn.execute("INSERT INTO table_1 VALUES (54, 'string with spaces', 3.0);")
conn.execute("INSERT INTO table_1 VALUES (75842, 'string with spaces', 3.0);")
conn.execute("INSERT INTO table_1 VALUES (623, 'string with spaces', 3.0);")

check2("SELECT * FROM table_1 ORDER BY _col2, col_1;",
       [(3, 'hi', 4.5), (33, 'hi', 4.5), (54, 'string with spaces', 3.0), (623, 'string with spaces', 3.0),
       (75842, 'string with spaces', 3.0)])
check2("SELECT * FROM table2 ORDER BY other, col_1;",
       [(15, 782), (165, 7282), (615, 7582)])

conn.execute("DROP TABLE table_1;")
conn.execute("DROP TABLE table2;")

#print("\nEscaping String Tests\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table_1 (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
conn.execute("INSERT INTO table_1 VALUES (33, 'hi', 4.5);")
conn.execute("INSERT INTO table_1 VALUES (36, 'don''t', 7);")
conn.execute("INSERT INTO table_1 VALUES (36, 'hi ''josh''', 7);")

check2(
    "SELECT * FROM table_1 ORDER BY _col2, col_1;",
    [(36, "don't", 7), (33, 'hi', 4.5), (36, "hi 'josh'", 7)]
)

conn.execute("DROP TABLE table_1;")

#print("\nQualified Names\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table_1 (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
conn.execute("INSERT INTO table_1 VALUES (33, 'hi', 4.5);")
conn.execute("INSERT INTO table_1 VALUES (36, 'don''t', 7);")
conn.execute("INSERT INTO table_1 VALUES (36, 'hi ''josh''', 7);")

check2(
    "SELECT col_1, col_3_, table_1._col2 FROM table_1 ORDER BY table_1._col2, _col2, col_1;",
    [(36, 7, "don't"), (33, 4.5, 'hi'), (36, 7, "hi 'josh'")]
)

conn.execute("DROP TABLE table_1;")

#print("\nSelect Star\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table_1 (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
conn.execute("INSERT INTO table_1 VALUES (33, 'hi', 4.5);")
conn.execute("INSERT INTO table_1 VALUES (36, 'don''t', 7);")
conn.execute("INSERT INTO table_1 VALUES (36, 'hi ''josh''', 7);")

check2(
    "SELECT col_1, *, col_3_, table_1._col2, * FROM table_1 ORDER BY table_1._col2, _col2, col_1;",
    [(36, 36, "don't", 7, 7, "don't", 36, "don't", 7),
     (33, 33, 'hi', 4.5, 4.5, 'hi', 33, 'hi', 4.5),
     (36, 36, "hi 'josh'", 7, 7, "hi 'josh'", 36, "hi 'josh'", 7)]
)

conn.execute("DROP TABLE table_1;")

#print("\ninserting into Normal Order\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness');")
conn.execute("INSERT INTO table (one, two, three) VALUES (11.4, 437, 'sadness');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(3.4, 43, 'happiness'), (11.4, 437, 'sadness')]
)

conn.execute("DROP TABLE table;")
#print("\nInserting into different order\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness');")
conn.execute("INSERT INTO table (one, three, two) VALUES (11.4, 'sadness', 84);")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(3.4, 43, 'happiness'), (11.4, 84, 'sadness')]
)

conn.execute("DROP TABLE table;")
#print("\ninserting not all columns\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness');")
conn.execute("INSERT INTO table (one, three) VALUES (11.4, 'sadness');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(3.4, 43, 'happiness'), (11.4, None, 'sadness')]
)

conn.execute("DROP TABLE table;")
#print("\ninserting into mulitple columns\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness');")
conn.execute("INSERT INTO table (one, three) VALUES (11.4, 'sadness'), (84.7, 'fear'), (94.7, 'weird');")
conn.execute("INSERT INTO table (two, three) VALUES (13, 'warmth'), (34, 'coldness');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(None, 34, 'coldness'),
     (84.7, None, 'fear'),
     (3.4, 43, 'happiness'),
     (11.4, None, 'sadness'),
     (None, 13, 'warmth'),
     (94.7, None, 'weird')]
)

conn.execute("DROP TABLE table;")
#print("\nselecting with where\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (43.24, 25, 'life'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE two > 50 ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'), (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE two = 42 ORDER BY three, two, one;",
    [(5345.6, 42, 'sadness'), (5.6, 42, 'thirst')])

check2(
    "SELECT * FROM table WHERE three IS NOT NULL ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (43.24, 25, 'life'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE two != 42 ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (43.24, 25, 'life'),
     (323.4, 433, 'warmth')]
)
conn.execute("DROP TABLE table;")
#print("\nwhere with NULL\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
conn.execute("INSERT INTO table VALUES (NULL, NULL, 'other'), (5.6, NULL, 'hunger'), (NULL, 235, 'want');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (5.6, None, 'hunger'),
     (43.24, 25, 'life'),
     (None, None, 'other'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE two > 50 ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'), (None, 235, 'want'), (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE two = 42 ORDER BY three, two, one;",
    [(5345.6, 42, 'sadness'), (5.6, 42, 'thirst')])

check2(
    "SELECT * FROM table WHERE two IS NOT NULL ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (43.24, 25, 'life'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

conn.execute("DROP TABLE table;")
#print("\nwhere with null two\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
conn.execute("INSERT INTO table VALUES (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
conn.execute("INSERT INTO table VALUES (NULL, NULL, 'other'), (5.6, NULL, 'hunger'), (NULL, 235, 'want');")

check2(
    "SELECT * FROM table WHERE one != 5.6 ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (43.24, 25, 'life'),
     (5345.6, 42, 'sadness')]
)

conn.execute("DROP TABLE table;")
#print("\nwhere clause qualified\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
conn.execute("INSERT INTO table VALUES (NULL, NULL, 'other'), (5.6, NULL, 'hunger'), (NULL, 235, 'want');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (5.6, None, 'hunger'),
     (43.24, 25, 'life'),
     (None, None, 'other'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE table.two > 50 ORDER BY three;",
    [(4.4, 235, 'Skyrim'), (None, 235, 'want'), (323.4, 433, 'warmth')]
)

check2(
    "SELECT table.* FROM table WHERE two = 42 ORDER BY three, two, one;",
    [(5345.6, 42, 'sadness'), (5.6, 42, 'thirst')])

check2(
    "SELECT * FROM table WHERE two IS NOT NULL ORDER BY table.three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (43.24, 25, 'life'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

conn.execute("DROP TABLE table;")
#print("\nupdate\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, notes TEXT);")
conn.execute(
    "INSERT INTO students VALUES ('Josh', 562, 'Likes Python'), ('Dennis', 45, 'Likes Networks'), ('Jie', 455, 'Likes Driving');")
conn.execute(
    "INSERT INTO students VALUES ('Cam', 524, 'Likes Anime'), ('Zizhen', 4532, 'Likes Reading'), ('Emily', 245, 'Likes Environmentalism');")

check2(
    "SELECT * FROM students ORDER BY name;",
    [('Cam', 524, 'Likes Anime'),
     ('Dennis', 45, 'Likes Networks'),
     ('Emily', 245, 'Likes Environmentalism'),
     ('Jie', 455, 'Likes Driving'),
     ('Josh', 562, 'Likes Python'),
     ('Zizhen', 4532, 'Likes Reading')]
)

conn.execute("UPDATE students SET grade = 100, notes = 'Likes Databases';")

check2(
    "SELECT * FROM students ORDER BY name;",
    [('Cam', 100, 'Likes Databases'),
     ('Dennis', 100, 'Likes Databases'),
     ('Emily', 100, 'Likes Databases'),
     ('Jie', 100, 'Likes Databases'),
     ('Josh', 100, 'Likes Databases'),
     ('Zizhen', 100, 'Likes Databases')]
)

conn.execute("DROP TABLE students;")


#print("\nupdate with where\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, notes TEXT);")
conn.execute(
    "INSERT INTO students VALUES ('Josh', 562, 'Likes Python'), ('Dennis', 45, 'Likes Networks'), ('Jie', 455, 'Likes Driving');")
conn.execute(
    "INSERT INTO students VALUES ('Cam', 524, 'Likes Anime'), ('Zizhen', 4532, 'Likes Reading'), ('Emily', 245, 'Likes Environmentalism');")

check2(
    "SELECT * FROM students ORDER BY name;",
    [('Cam', 524, 'Likes Anime'),
     ('Dennis', 45, 'Likes Networks'),
     ('Emily', 245, 'Likes Environmentalism'),
     ('Jie', 455, 'Likes Driving'),
     ('Josh', 562, 'Likes Python'),
     ('Zizhen', 4532, 'Likes Reading')]
)

conn.execute("UPDATE students SET notes = 'High Grade' WHERE grade > 100;")

check2(
    "SELECT * FROM students ORDER BY name;",
    [('Cam', 524, 'High Grade'),
     ('Dennis', 45, 'Likes Networks'),
     ('Emily', 245, 'High Grade'),
     ('Jie', 455, 'High Grade'),
     ('Josh', 562, 'High Grade'),
     ('Zizhen', 4532, 'High Grade')]
)

conn.execute("DROP TABLE students;")
#print("\ndistinct\n")
conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, notes TEXT);")
conn.execute(
    "INSERT INTO students VALUES ('Josh', 99, 'Likes Python'), ('Dennis', 99, 'Likes Networks'), ('Jie', 52, 'Likes Driving');")
conn.execute(
    "INSERT INTO students VALUES ('Cam', 56, 'Likes Anime'), ('Zizhen', 56, 'Likes Reading'), ('Emily', 74, 'Likes Environmentalism');")

check2(
    "SELECT * FROM students ORDER BY name;",
    [('Cam', 56, 'Likes Anime'),
     ('Dennis', 99, 'Likes Networks'),
     ('Emily', 74, 'Likes Environmentalism'),
     ('Jie', 52, 'Likes Driving'),
     ('Josh', 99, 'Likes Python'),
     ('Zizhen', 56, 'Likes Reading')]
)

check2(
    "SELECT DISTINCT grade FROM students ORDER BY grade;",
    [(52,), (56,), (74,), (99,)]
)

check2(
    "SELECT DISTINCT grade FROM students WHERE name < 'Emily' ORDER BY grade;",
    [(56,), (99,)]
)

conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
conn.execute("INSERT INTO table VALUES (NULL, NULL, 'other'), (5.6, NULL, 'hunger'), (NULL, 235, 'want');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (5.6, None, 'hunger'),
     (43.24, 25, 'life'),
     (None, None, 'other'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE two > 50 ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'), (None, 235, 'want'), (323.4, 433, 'warmth')]
)

check2(
    "SELECT * FROM table WHERE two = 42 ORDER BY three, two, one;",
    [(5345.6, 42, 'sadness'), (5.6, 42, 'thirst')])

check2(
    "SELECT * FROM table WHERE two IS NOT NULL ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (43.24, 25, 'life'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

conn.execute("DROP TABLE students;")
conn.execute("DROP TABLE table;")

conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
conn.execute("INSERT INTO table VALUES (NULL, NULL, 'other'), (5.6, NULL, 'hunger'), (NULL, 235, 'want');")

check2(
    "SELECT * FROM table ORDER BY three, two, one;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (5.6, None, 'hunger'),
     (43.24, 25, 'life'),
     (None, None, 'other'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

conn.execute("DELETE FROM table;")

check2(
    "SELECT * FROM table ORDER BY three;",
    []
)

conn.execute("DROP TABLE table;")

conn = project.connect("test.db")
conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
conn.execute("INSERT INTO table VALUES (NULL, NULL, 'other'), (5.6, NULL, 'hunger'), (NULL, 235, 'want');")

check2(
    "SELECT * FROM table ORDER BY three;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (5.6, None, 'hunger'),
     (43.24, 25, 'life'),
     (None, None, 'other'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (None, 235, 'want'),
     (323.4, 433, 'warmth')]
)

conn.execute("DELETE FROM table WHERE one IS NULL;")

check2(
    "SELECT * FROM table ORDER BY three;",
    [(4.4, 235, 'Skyrim'),
     (3.4, 43, 'happiness'),
     (5.6, None, 'hunger'),
     (43.24, 25, 'life'),
     (5345.6, 42, 'sadness'),
     (5.6, 42, 'thirst'),
     (323.4, 433, 'warmth')]
)

conn.execute("DELETE FROM table WHERE two < 50;")

check2(
    "SELECT * FROM table ORDER BY three;",
    [(4.4, 235, 'Skyrim'), (5.6, None, 'hunger'), (323.4, 433, 'warmth')]

)

conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")

conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
conn.execute("INSERT INTO students VALUES ('Cam', 56, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")

conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")

conn.execute("DROP TABLE students;")
conn.execute("DROP TABLE classes;")


conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")

conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
conn.execute("INSERT INTO students VALUES ('Cam', 56, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")

conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")

check2(
  "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course ORDER BY classes.instructor, students.name, students.grade;",
[('Jie', 52, 'CSE491', 'Dr. Josh'),
 ('Zizhen', 56, 'CSE491', 'Dr. Josh'),
 ('Cam', 56, 'CSE480', 'Dr. Nahum'),
 ('Dennis', 99, 'CSE480', 'Dr. Nahum'),
 ('Josh', 99, 'CSE480', 'Dr. Nahum'),
 ('Emily', 74, 'CSE431', 'Dr. Ofria')]
  )

conn.execute("DROP TABLE students;")
conn.execute("DROP TABLE classes;")

conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")

conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
conn.execute("INSERT INTO students VALUES ('Cam', 56, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")

conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")


check2(
  "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course WHERE students.grade > 60 ORDER BY classes.instructor, students.name, students.grade;",
[('Dennis', 99, 'CSE480', 'Dr. Nahum'),
 ('Josh', 99, 'CSE480', 'Dr. Nahum'),
 ('Emily', 74, 'CSE431', 'Dr. Ofria')]
  )

conn.execute("DROP TABLE students;")
conn.execute("DROP TABLE classes;")

conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")

conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
conn.execute("INSERT INTO students VALUES ('Cam', 56, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")
conn.execute("INSERT INTO students VALUES ('James', 96, 'CSE335'), ('Carol', 87, NULL), ('Jackie', 45, 'CSE323');")


conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")
conn.execute("INSERT INTO classes VALUES ('CSE331', 'Dr. Owens'), (NULL, 'Chair');")


check2(
  "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course ORDER BY students.name;",
[('Cam', 56, 'CSE480', 'Dr. Nahum'),
 ('Carol', 87, None, None),
 ('Dennis', 99, 'CSE480', 'Dr. Nahum'),
 ('Emily', 74, 'CSE431', 'Dr. Ofria'),
 ('Jackie', 45, None, None),
 ('James', 96, None, None),
 ('Jie', 52, 'CSE491', 'Dr. Josh'),
 ('Josh', 99, 'CSE480', 'Dr. Nahum'),
 ('Zizhen', 56, 'CSE491', 'Dr. Josh')]
  )

conn.execute("DROP TABLE students;")
conn.execute("DROP TABLE classes;")

conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")

conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
conn.execute("INSERT INTO students VALUES ('Cam', 66, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")
conn.execute("INSERT INTO students VALUES ('Jake', 36, 'CSE480'), ('John', 36, 'CSE491'), ('Jen', 34, 'CSE431');")

conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")

check2(
    "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course WHERE students.name > 'Emily' ORDER BY classes.instructor, students.name, students.grade;",
    [('Jie', 52, 'CSE491', 'Dr. Josh'),
     ('John', 36, 'CSE491', 'Dr. Josh'),
     ('Zizhen', 56, 'CSE491', 'Dr. Josh'),
     ('Jake', 36, 'CSE480', 'Dr. Nahum'),
     ('Josh', 99, 'CSE480', 'Dr. Nahum'),
     ('Jen', 34, 'CSE431', 'Dr. Ofria')]
)

check2(
    "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course WHERE students.name < 'Emily' ORDER BY classes.instructor, students.name, students.grade;",
    [('Cam', 66, 'CSE480', 'Dr. Nahum'), ('Dennis', 99, 'CSE480', 'Dr. Nahum')]
)

check2(
    "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course WHERE students.name = 'Emily' ORDER BY classes.instructor, students.name, students.grade;",
    [('Emily', 74, 'CSE431', 'Dr. Ofria')]

)

conn.execute("DROP TABLE students;")
conn.execute("DROP TABLE classes;")

conn = project.connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")

conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
conn.execute("INSERT INTO students VALUES ('Cam', 66, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")
conn.execute("INSERT INTO students VALUES ('Jake', 36, 'CSE480'), ('John', 36, 'CSE491'), ('Jen', 34, 'CSE431');")

conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")

check2(
    "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course WHERE students.name > 'Emily' ORDER BY classes.instructor, students.name, students.grade;",
    [('Jie', 52, 'CSE491', 'Dr. Josh'),
     ('John', 36, 'CSE491', 'Dr. Josh'),
     ('Zizhen', 56, 'CSE491', 'Dr. Josh'),
     ('Jake', 36, 'CSE480', 'Dr. Nahum'),
     ('Josh', 99, 'CSE480', 'Dr. Nahum'),
     ('Jen', 34, 'CSE431', 'Dr. Ofria')]
)

check2(
    "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course WHERE students.name < 'Emily' ORDER BY classes.instructor, students.name, students.grade;",
    [('Cam', 66, 'CSE480', 'Dr. Nahum'), ('Dennis', 99, 'CSE480', 'Dr. Nahum')]
)

check2(
    "SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course WHERE students.name = 'Emily' ORDER BY classes.instructor, students.name, students.grade;",
    [('Emily', 74, 'CSE431', 'Dr. Ofria')]

)

conn.execute("DROP TABLE students;")
conn.execute("DROP TABLE classes;")

conn = project.connect("test.db")
conn.execute("CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
conn.execute("CREATE TABLE owners (name TEXT, age INTEGER, id INTEGER);")
conn.execute("INSERT INTO pets VALUES ('RaceTrack', 'Ferret', 3), ('Ghost', 'Ferret', 2), ('Zoe', 'Dog', 7), ('Ebony', 'Dog', 17);")
conn.execute("INSERT INTO pets (species, name) VALUES ('Rat', 'Ginny'), ('Dog', 'Balto'), ('Dog', 'Clifford');")

conn.execute("UPDATE pets SET age = 15 WHERE name = 'RaceTrack';")


check2(
  "SELECT species, *, pets.name FROM pets WHERE age > 3 ORDER BY pets.name;",
[('Dog', 'Ebony', 'Dog', 17, 'Ebony'),
 ('Ferret', 'RaceTrack', 'Ferret', 15, 'RaceTrack'),
 ('Dog', 'Zoe', 'Dog', 7, 'Zoe')]
 )

conn.execute("INSERT INTO owners VALUES ('Josh', 29, 10), ('Emily', 27, 2), ('Zach', 25, 4), ('Doug', 34, 5);")
conn.execute("DELETE FROM owners WHERE name = 'Doug';")
check2(
  "SELECT owners.* FROM owners ORDER BY id;",
[('Emily', 27, 2), ('Zach', 25, 4), ('Josh', 29, 10)]
 )

conn.execute("CREATE TABLE ownership (name TEXT, id INTEGER);")
conn.execute("INSERT INTO ownership VALUES ('RaceTrack', 10), ('Ginny', 2), ('Ghost', 2), ('Zoe', 4);")


result = conn.execute("SELECT * FROM pets ORDER BY name;")
for i in result:
    print(i)
print("---------------")
result = conn.execute("SELECT * FROM ownership;")
for i in result:
    print(i)
print("---------------")
#
result = conn.execute("SELECT pets.name, pets.age, ownership.id FROM pets LEFT OUTER JOIN ownership ON pets.name = ownership.name WHERE pets.age IS NULL ORDER BY pets.name;")
for i in result:
    print(i)

# conn.execute("CREATE TABLE pets (name TEXT, species TEXT, age INTEGER);")
# conn.execute("CREATE TABLE owners (name TEXT, age INTEGER, id INTEGER);")
# check2("SELECT pets.name, pets.age, ownership.id FROM pets LEFT OUTER JOIN ownership ON pets.name = ownership.name WHERE pets.age IS NULL ORDER BY pets.name;",
#   [('Balto', None, None), ('Clifford', None, None), ('Ginny', None, 2)]

conn.execute("DROP TABLE pets;")
conn.execute("DROP TABLE ownership;")
conn.execute("DROP TABLE owners;")


print("Success!")