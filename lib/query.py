# Sample Data for Full Load
SOURCE_CSV_EMPLOYEE_SAMPLE_DATA = """101,Smith,Bob,2014-06-04,New York
102,Smith,Bob,2015-10-08,Los Angeles
103,Smith,Bob,2017-03-13,Dallas
104,Smith,Bob,2017-03-13,Dallas"""

SOURCE_CSV_DEPARTMENT_SAMPLE_DATA = """201,HR
202,IT
203,Finance"""

SOURCE_CSV_PROJECT_SAMPLE_DATA = """301,Project1,Description1
302,Project2,Description2
303,Project3,Description3"""

# Sample Data for CDC
CDC_FILE_SAMPLE_DATA_1 = """INSERT,employee,hr,101,Smith,Bob,2014-06-04,New York
UPDATE,employee,hr,101,Smith,Bob,2015-10-08,Los Angeles
UPDATE,employee,hr,101,Smith,Bob,2017-03-13,Dallas
DELETE,employee,hr,101,Smith,Bob,2017-03-13,Dallas"""

CDC_FILE_SAMPLE_DATA_2 = """INSERT,department,hr,204,Software
INSERT,employee,hr,101,Smith,Bob,2015-10-08,Los Angeles
INSERT,project,hr,101,Project1,Description1
DELETE,project,hr,101,Project1,Description1
DELETE,department,hr,301,Software
UPDATE,employee,hr,101,Smith,Bob,2017-03-13,Dallas
DELETE,employee,hr,101,Smith,Bob,2017-03-13,Dallas"""
