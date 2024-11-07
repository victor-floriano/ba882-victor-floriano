from google.cloud import aiplatform

project_id = 'ba882-victorgf'
gcp_region = 'us-central1'
exp_name = "post-length-revision-pl045"


aiplatform.init(project=project_id, 
                location=gcp_region,
                experiment=exp_name,
                experiment_description="Testing Experiment",
                experiment_tensorboard=None)

# Start a run in the experiment
aiplatform.start_run(run="post-length-2")

# Define metrics and parameters to log
metrics = {
    "r2": 0.90,
    "mape": 9.5,
    "mae": 2.9
}

params = {
    'max_tokens': 400,
    'ngram': str((1,2)),
    'model': 'LinearRegression'
}

# Log metrics and parameters
aiplatform.log_metrics(metrics)
aiplatform.log_params(params)

# End the run
aiplatform.end_run()