gcloud config set project ba882-victorgf

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-victorgf/streamlit-poc .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-victorgf/streamlit-poc

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-poc \
    --image gcr.io/ba882-victorgf/streamlit-poc \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account etl-pipeline@ba882-victorgf.iam.gserviceaccount.com\
    --memory 1Gi