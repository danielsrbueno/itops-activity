import os
import random
import boto3
from dotenv import load_dotenv
import psutil
import json
import datetime

load_dotenv()

AP_ID="AP001"
BUCKET_NAME = os.getenv("BUCKET_NAME")

session = boto3.Session(
  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
  aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
  region_name="us-east-1"
)
s3_client = session.client("s3")

def __init__():
  network_data = psutil.net_io_counters()
  bytes_sent = network_data.bytes_sent
  bytes_recv = network_data.bytes_recv

  active_conn = random.randint(0, 50)

  cpu_percent = psutil.cpu_percent(interval=1)
  ram_percent = psutil.virtual_memory().percent

  date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")

  data = {
    "id": AP_ID,
    "bytes_sent": bytes_sent, 
    "bytes_recv": bytes_recv,
    "active_conn": active_conn,
    "cpu_percent": cpu_percent,
    "ram_percent": ram_percent,
    "created_at": date
  }
  file_name = f"./data-01-bronze/{date}_{AP_ID.lower()}.json"
  with open(file_name, "w") as file:
    json.dump(data, file, indent=2)

  s3_client.upload_file(file_name, BUCKET_NAME, f"01-bronze/{date}_{AP_ID.lower()}.json")

__init__()