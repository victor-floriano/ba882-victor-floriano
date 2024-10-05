import functions_framework
from google.cloud import secretmanager
from google.cloud import storage
import requests
import feedparser
import json
import datetime
import uuid

def upload_to_gcs(bucket_name, job_id, data):
    """Uploads data to a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob_name = f"jobs/{job_id}/extracted_entries.json"
    blob = bucket.blob(blob_name)

    # Upload the data (here it's a serialized string)
    blob.upload_from_string(data)
    print(f"File {blob_name} uploaded to {bucket_name}.")

    return {'bucket_name':bucket_name, 'blob_name': blob_name}

@functions_framework.http
def task(request):
    print("Function has been triggered")
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())
    print(f"Generated job_id: {job_id}")

    try:
        # Instantiate the services
        sm = secretmanager.SecretManagerServiceClient()
        storage_client = storage.Client()

        # Access the secret version
        name = f"projects/ba882-victorgf/secrets/mother_duck/versions/latest"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")
        print("Secret accessed successfully")
    except Exception as e:
        print(f"Error accessing Secret Manager: {e}")
        return {"error": "Failed to access secret"}, 500

    try:
        # Get RSS feeds
        feeds = [
            'https://aws.amazon.com/blogs/big-data/feed/',
            'https://aws.amazon.com/blogs/compute/feed/',
            'https://aws.amazon.com/blogs/database/feed/',
            'https://aws.amazon.com/blogs/machine-learning/feed/',
            'https://aws.amazon.com/blogs/containers/feed/',
            'https://aws.amazon.com/blogs/infrastructure-and-automation/feed/',
            'https://aws.amazon.com/blogs/aws/feed/',
            'https://aws.amazon.com/blogs/business-intelligence/feed/',
            'https://aws.amazon.com/blogs/storage/feed/'
        ]

        feed_list = []
        for feed in feeds:
            try:
                print(f"Fetching feed: {feed}")
                r = requests.get(feed)
                feed = feedparser.parse(r.content)
                print(f"Successfully fetched and parsed feed: {feed.feed.title}")
                feed_list.append(feed)
            except Exception as e:
                print(f"Error fetching or parsing feed {feed}: {e}")
                raise

        # Process feeds
        entries = []
        for feed in feed_list:
            entries.extend(feed.entries)
        print(f"Total entries extracted: {len(entries)}")

        # Serialize entries to JSON
        entries_json = json.dumps(entries)

        # Write to GCS
        gcs_path = upload_to_gcs(bucket_name, job_id, entries_json)
        print(f"Data uploaded to GCS: {gcs_path}")
    except Exception as e:
        print(f"Error during processing: {e}")
        return {"error": "Failed to process and upload feeds"}, 500

    return {
        "num_entries": len(entries),
        "job_id": job_id,
        "bucket_name": gcs_path.get('bucket_name'),
        "blob_name": gcs_path.get('blob_name')
    }, 200
