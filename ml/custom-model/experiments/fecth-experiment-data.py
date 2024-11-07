from google.cloud import aiplatform

# Initialize Vertex AI with your project
aiplatform.init(
    project="ba882-victorgf",
    location="us-central1",
)

# Set an experiment name (should match the one previously used for logging)
exp_name = "post-length-revision-pl045"

# Start the experiment (this ensures it's recognized in Vertex AI)
aiplatform.start_run(run="fetch-experiment-data")

# Fetch the experiment data
experiments_df = aiplatform.get_experiment_df()
print(experiments_df)

# End the run
aiplatform.end_run()
