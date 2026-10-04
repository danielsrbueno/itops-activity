import random
import psutil
import json
import datetime
import platform
import subprocess
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

AP_ID="firewall"
BUCKET_NAME = os.getenv("BUCKET_NAME")

session = boto3.Session(
  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
  aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
  region_name="us-east-1"
)
s3_client = session.client("s3")

def __init__():
  # A função platform.system() está a fim de teste no ambiente de desenvolvimento (macOS), pois este sistema operacional recusa a permissão de certos comandos
  if platform.system() == "Darwin":
    output = subprocess.check_output("netstat -an | grep ESTABLISHED", shell=True).decode("utf-8")
    active_sessions = len(output.split("\n")) -1
  else:
    active_sessions = len(psutil.net_connections(kind='inet'))

  network_data = psutil.net_io_counters()
  bytes_sent = network_data.bytes_sent
  bytes_recv = network_data.bytes_recv

  if platform.system() == "Darwin":
    dropped_packets = random.randint(0,1000)
  else:
    dropped_packets = network_data.dropin +  network_data.dropout

  top_blocked_ip = ""

  for i in range(4):
    top_blocked_ip += str(random.randint(0,255))
    if i != 3:
      top_blocked_ip += "."

  cpu_percent = psutil.cpu_percent(interval=1)
  ram_percent = psutil.virtual_memory().percent
  
  date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")

  data = {
    "active_sessions": active_sessions, 
    "dropped_packets": dropped_packets,
    "top_blocked_ip": top_blocked_ip,
    "bytes_sent": bytes_sent, 
    "bytes_recv": bytes_recv,
    "cpu_percent": cpu_percent,
    "ram_percent": ram_percent,
    "created_at": date,
  }
  
  file_name = f"./data-01-bronze/{date}_{AP_ID.lower()}.json"
  with open(file_name, "w") as file:
    json.dump(data, file, indent=2)
  
  s3_client.upload_file(file_name, BUCKET_NAME, f"01-bronze/{date}_{AP_ID.lower()}.json")

__init__()