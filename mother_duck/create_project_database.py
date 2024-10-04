# imports
from google.cloud import secretmanager
import duckdb
import feedparser
import pandas as pd

# instantiate the service
sm = secretmanager.SecretManagerServiceClient()

# replace below with your own product settings
project_id = 'ba882-victorgf'
secret_id = 'mother_duck'   #<---------- this is the name of the secret your created above!
version_id = 'latest'

# Build the resource name of the secret version
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

# Access the secret version
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")


# initiate the MotherDuck connection through an access token through
md = duckdb.connect(f'md:?motherduck_token={md_token}') 

# lets look at the high level bits
md.sql("SHOW DATABASES").show()

# some setup that we will use for this task (db and schema names)
db = 'aws_blogs'
schema = "sandbox"

# define the DDL statement with an f string
create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"   

# execute the command to create the database
md.sql(create_db_sql)

# confirm it exists
md.sql("SHOW DATABASES").show()

# create a fully qualified name that we can use from this point forward
db_schema = f"{db}.{schema}"

# use DDL to create the schema if it doesnt exist via an F string
md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")


# create some tables in the database via the explicit database and schema
# https://duckdb.org/docs/sql/statements/create_table.html

tbl_name = "users"
table_create_sql = f"""
CREATE TABLE IF NOT EXISTS {db_schema}.{tbl_name}  (
    user_id INTEGER PRIMARY KEY,  -- Primary key
    username VARCHAR,             -- Username field
    email VARCHAR,                -- Email field
    created_at TIMESTAMP          -- Timestamp when the user was created
);
"""

md.sql(table_create_sql)


#Check the table just created
md.sql(f"DESCRIBE {db_schema}.{tbl_name};").show()


#Simple sample data load --comment it out after first insertion
import pandas as pd
from datetime import datetime
import random
'''
# Sample data for the table
data = {
    'user_id': range(1, 6),  # User IDs
    'username': ['alice', 'bob', 'charlie', 'david', 'eve'],  # Usernames
    'email': [
        'alice@example.com',
        'bob@example.com',
        'charlie@example.com',
        'david@example.com',
        'eve@example.com'
    ],  # Email addresses
    'created_at': [
        datetime(2023, 1, random.randint(1, 28)),
        datetime(2023, 2, random.randint(1, 28)),
        datetime(2023, 3, random.randint(1, 28)),
        datetime(2023, 4, random.randint(1, 28)),
        datetime(2023, 5, random.randint(1, 28)),
    ]  # Timestamps for user creation
}

# create the dataframe
df = pd.DataFrame(data)

# now insert the data directly from pands into the table
md.execute(f"INSERT INTO {db_schema}.{tbl_name} SELECT * FROM df;")
'''
