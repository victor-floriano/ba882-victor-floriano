import requests

EP = "https://mlops-postlength-prediction-548628906045.us-central1.run.app"
sample = {"data":['AWS Bedrock is expanding services', 'GENAI is our next theme!']}

try:
    resp = requests.post(EP, json=sample)
    resp.raise_for_status()  # Raises an error for bad status codes
    print(resp.json())  # Print the JSON response
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
