echo "======================================================"
echo "itemize the project"
echo "======================================================"

gcloud config set project ba882-victorgf

echo "======================================================"
echo "ensure the APIs are enabled"
echo "======================================================"

gcloud services enable aiplatform.googleapis.com
gcloud services enable storage.googleapis.com


echo "======================================================"
echo "build and push the image"
echo "======================================================"

docker build -t gcr.io/ba882-victorgf/blog-post-length-model .

docker push gcr.io/ba882-victorgf/blog-post-length-model

echo "======================================================"
echo "register the model"
echo "======================================================"


gcloud ai models upload \
  --region=us-central1 \
  --display-name=sklearn-post-length \
  --container-image-uri=gcr.io/ba882-victorgf/blog-post-length-model \
  --container-predict-route=/predict \
  --container-health-route=/health