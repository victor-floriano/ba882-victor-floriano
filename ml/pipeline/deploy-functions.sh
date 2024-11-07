######
## simple script for now to deploy functions
## deploys all, which may not be necessary for unchanged resources
######

# setup the project
gcloud config set project ba882-victorgf

# schema setup
echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy mlops-schema-setup \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/schema-setup \
    --stage-bucket ba882-victorgf-stage-bucket \
    --service-account etl-pipeline@ba882-victorgf.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB 

# the training module
echo "======================================================"
echo "deploying the trainer"
echo "======================================================"

gcloud functions deploy mlops-postlength-trainer \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/trainer \
    --stage-bucket ba882-victorgf-stage-bucket \
    --service-account etl-pipeline@ba882-victorgf.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB