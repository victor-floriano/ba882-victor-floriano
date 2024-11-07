gcloud ai endpoints create \
  --region=us-central1 \
  --display-name=post-length-endpoint

gcloud ai endpoints deploy-model 44104709924978688 \
  --region=us-central1 \
  --model=6863667251531218944 \
  --display-name=post-length-deployment \
  --machine-type=n1-standard-4 \
  --min-replica-count=1 \
  --max-replica-count=1 \
  --service-account=etl-pipeline@ba882-victorgf.iam.gserviceaccount.com \
  --traffic-split=0=100