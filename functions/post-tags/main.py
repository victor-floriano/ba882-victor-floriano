# create/update a view to associate tags to post content, intended for ML classification

import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
from google.cloud import aiplatform
import duckdb
import pandas as pd
import datetime


# settings
project_id = 'ba882-victorgf'
project_region = 'us-central1'
secret_id = 'mother_duck'   #<---------- this is the name of the secret you created
version_id = 'latest'
bucket_name = 'ba882-victorgf-awsblogs'
ml_bucket_name = 'ba882-victorgf-vertex-models'
ml_dataset_path = '/training-data/post-tags/'

# db setup
db = 'awsblogs'
stage_db_schema = f"{db}.stage"
ml_schema = f"{db}.ml"
ml_view_name = "post-tags"


ingest_timestamp = pd.Timestamp.now()


############################################################### helpers

## define the SQL
ml_view_sql = f"""
CREATE OR REPLACE VIEW {ml_schema}.{ml_view_name} AS
with
posts as (
    select id, content_text
    from awsblogs.stage.posts
),

top_tags as (
    select lower(term) as term, count(*) as total
    from awsblogs.stage.tags
    group by term
    order by total desc
    limit 20
),

tags as (
    select t.post_id, lower(t.term) as term
    from awsblogs.stage.tags t
    inner join top_tags tt on lower(t.term) = lower(tt.term)
)

select
    p.id,
    p.content_text,
    string_agg(lower(t.term), ',') AS labels,
    CURRENT_TIMESTAMP AS created_at
from posts p
inner join tags t on p.id = t.post_id
group by p.id, p.content_text;
"""

############################################################### main task


@functions_framework.http
def task(request):

    # we will not be passing in any data into the request

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # connect to motherduck, the cloud datawarehouse
    print("connecting to Motherduck")
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    # create the view
    print("creating the schema if it doesnt exist and creating/updating the view")
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {ml_schema};")
    md.sql(ml_view_sql)

    # grab the view as a pandas dataframe, just the text and the labels
    df = md.sql(f"select content_text, labels from {ml_schema}.{ml_view_name};").df()

    # cleanup for modeling - a list column for use in sklearn
    df['labels'] = df['labels'].apply(lambda x: x.split(','))

    # write the dataset to the training dataset path on GCS
    print("writing the csv file to gcs")
    dataset_path = "gcs://" + ml_bucket_name + ml_dataset_path + "post-tags.csv"
    df.to_csv(dataset_path, index=False)

    return {"dataset_path": dataset_path}, 200