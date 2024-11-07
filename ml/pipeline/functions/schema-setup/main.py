import functions_framework
from google.cloud import secretmanager
import duckdb

# settings
project_id = 'ba882-victorgf'
secret_id = 'mother_duck'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'awsblogs'
schema = "mlops"
db_schema = f"{db}.{schema}"


@functions_framework.http
def task(request):

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    ##################################################### create the schema

    # Massive assumption the db is up!

    # create the schema
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};") 

    ##################################################### create the core tables in stage

    # model runs
    raw_tbl_name = f"{db_schema}.model_runs"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        job_id VARCHAR PRIMARY KEY
        ,name VARCHAR
        ,gcs_path VARCHAR
        ,model_path VARCHAR
        ,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # job metrics
    raw_tbl_name = f"{db_schema}.job_metrics"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        job_id VARCHAR
        ,metric_name VARCHAR
        ,metric_value FLOAT
        ,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # job parameters
    raw_tbl_name = f"{db_schema}.job_parameters"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        job_id VARCHAR
        ,parameter_name VARCHAR
        ,parameter_value VARCHAR
        ,created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    return {}, 200