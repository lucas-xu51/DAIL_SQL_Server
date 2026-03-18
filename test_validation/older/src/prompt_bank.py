column_meaning_prompt = """def convert_schema_to_comprehensive_description(db_id, table_name, column_name, column_type, column_description = None, value_description = None):
    # step1: The interpretation of a column name is contingent upon its relational association with the table name. Thus, the first generated sentence should explain the column meaning within the context of table_name
    # step2: output overall column description according to step1
    assert len(overall_description) <= 100
    return overall_description

overall_description = convert_schema_to_comprehensive_description({input_paras})

print(overall_description)

#Output: """


dummy_sql_prompt = """# the key is the table, the value is a dict which key is original column name and value is the column information including full name, column description, value_description and example values.
database_schema = {database_schema}

# the key is the table, the value is the list of its counterpart primary keys
primary_keys = {primary_key_dic}

# the key is the source column, the value is the target column referenced by foreign key relationship.
foreign_keys = {foreign_key_dic}

question = "{question_prompt}"

Hint = "{evidence}"

def question_to_SQL(question):
  # DO NOT select more things other than what the question asks
  # Generate the SQL to answer the question considering database_schema, primary_keys and foreign_keys
  # Also consider the Hint when generating the SQL
  SQL = ""
"""
  

sr_examples = """#SR is a piece of pandas-like code. Learn to generate SR based on the question and the schema. Later, the SR will be converted to SQL. 
#SR ignore 'join' action. Do not generate 'join' action.
#In the generated SR, only select the thing that request in the question. Do not select any non-requested stuff. 
#The filter condition in the 'where' function doesn't directly match the text in the question. To find the correct value for the 'where' function, you need to reference the example values or all possible values in column description.

question = "How many movies directed by Francis Ford Coppola have a popularity of more than 1,000? Please also show the critic of these movies."
schema = [movies.movie_title, ratings.critic, movies.director_name, movies.movie_popularity, ratings.movie_id, movies.movie_id']
Hint = "Francis Ford Coppola refers to director_name; popularity of more than 1,000 refers to movie_popularity >1000" 
SR = "df1 = df.where(element = movies.director_name, filter = 'Francis Ford Coppola')
    df2 = df1.where(element = movies.movie_popularity, filter = '> 1000')
    res = df2.select(movies.movie_title, ratings.critic)"

question = "What is the first name of clients who have the highest priority?"
schema = [client.first, client.client_id, callcenterlogs.`rand client`,callcenterlogs.priority]
Hint = "first name refers to first; highest priority refers to priority = 2"
SR = "df1 = df.where(element = callcenterlogs.priority, filter = max(callcenterlogs.priority))
    res = df1.select(client.first)"
    
question = "What is the difference between the number of children's films and action films?"
schema = [category.name, film_category.category_id, category.category_id]
Hint = ""
SR = "df1 = df.where(element = category.name, filter = 'ChildrenFilm')
    df2 = df.where(element = category.name, filter = 'ActionFilm')
    res = df.select(df1.count() - df2.count())"
"""


generate_sr = """{sr_example}
column_description = {column_description}
question = {question}
schema = {schema}
Hint = "{evidence}"
SR =
"""



sr2sql = """# Understand the pandas-like SR first. Then convert the SR in to executable SQL based on the question, the schema, the evidence and the imported keywords. 
# Notice the order of the action in SR may not same as the executable SQL. Make sure the generated SQL is executable and can answer the question accurately according to the schema. 
# Only select the thing that the question required. Do not select any non-requested stuff. 
# You may need to look back to the column_description and schema to get the correct value used in the final SQL

from CLAUSE_KEYWORDS import select, from, where, group by, order by, union, limit, having, distinct, as, between, like, all, on, partition by
from JOIN_KEYWORDS import inner join, left join
from WHERE_OPERATIONS import is, not, null, none, in, =, >, <, >=, <=, !=, <>
from DATE_OPERATIONS import now, curdate, strftime
from UNIT_OPERATIONS import -, +, *, /
from COND_OPERATIONS import and, or, case, iif
from SQL_OPERATIONS import avg, count, max, min, round, abs, sum, length, cast, substr, cast, instr
from ORDER_OPERATIONS import desc, asc

column_description = {column_description}

foreign_keys = {foreign_key_dic}

question = {question}
schema = {schema}
evidence = "{evidence}"
SR = "{SR}"
SQL = ""
"""