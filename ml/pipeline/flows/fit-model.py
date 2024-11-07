# The ML job orchestrator

# imports
import requests
import json
from prefect import flow, task

# helper function - generic invoker
def invoke_gcf(url:str, payload:dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()


@task(retries=2)
def schema_setup():
    """Setup the stage schema"""
    url = "https://mlops-schema-setup-548628906045.us-central1.run.app"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def train():
    """Train the model against data from the cloud warehouse"""
    url = "https://mlops-postlength-trainer-548628906045.us-central1.run.app"
    resp = invoke_gcf(url, payload={})
    return resp

# Prefect Flow
@flow(name="mlops-post-length-model", log_prints=True)
def training_flow():
    """The ETL flow which orchestrates Cloud Functions for the ML task"""
    
    result = schema_setup()
    print("The schema setup completed")

    stats = train()
    print("The model training completed successfully")
    print(f"{stats}")


# the job
if __name__ == "__main__":
    training_flow()