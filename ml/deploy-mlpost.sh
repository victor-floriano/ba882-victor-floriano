# set the project
gcloud config set project ba882-victorgf


echo "======================================================"
echo "deploying the post length training function"
echo "======================================================"

gcloud functions deploy ml-postwc-train \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/ml-post_wc_train \
    --stage-bucket ba882-victorgf-stage-bucket \
    --service-account etl-pipeline@ba882-victorgf.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 60s 

echo "======================================================"
echo "deploying the post length inference function"
echo "======================================================"

gcloud functions deploy ml-postwc-serve \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source ./functions/ml-post_wc_serve \
    --stage-bucket ba882-victorgf-stage-bucket \
    --service-account etl-pipeline@ba882-victorgf.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 60s 