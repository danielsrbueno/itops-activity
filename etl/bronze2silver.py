import csv
import datetime
import json
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")

session = boto3.Session(
  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
  aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
  region_name="us-east-1"
)
s3_client = session.client("s3")

def __init__():
  res_chk = s3_client.get_object(Bucket=BUCKET_NAME, Key="01-bronze/checkpoint.json")
  last_read = json.loads(res_chk["Body"].read())

  response = s3_client.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="01-bronze/"
  )

  all_files = []

  csv_rows = [["created_at", "mbps_aps", "mbps_firewall", "consistency", "status"]]

  for obj in response.get("Contents", []):
    filename_fomatted = obj["Key"].split("/")[-1]

    if filename_fomatted == "" or filename_fomatted == "checkpoint.json":
      continue

    all_files.append(filename_fomatted)

  all_files.sort()

  files_to_transform = [
    file 
    for file in all_files 
    if file.split("/")[-1].split("_a")[0].split("_f")[0] > last_read["date"]
  ]

  dates = []
  for file_name in files_to_transform:
    date = file_name.split("_")[0] + " " + file_name.split("_")[1].replace("-", ":")
    if date not in dates:
      dates.append(date)

  old_bytes_access_point = 0
  old_bytes_firewall = 0

  for date in dates:
    bytes_access_point = 0
    bytes_firewall = 0
    load_status = ""

    files_to_read = [
      file 
      for file in files_to_transform 
      if file_name.split("_")[0] + " " + file_name.split("_")[1].replace("-", ":") == date
    ]

    for file_name in files_to_read:
      type = getDeviceType(file_name)

      res = s3_client.get_object(
        Bucket=BUCKET_NAME,
        Key=f"01-bronze/{file_name}"
      )

      data = json.loads(res["Body"].read())

      if type == "ACCESS_POINT":
        load_status += getLoadStatus(data)
        bytes_access_point += data["bytes_sent"]

      if type == "FIREWALL":
        bytes_firewall += data["bytes_sent"]

    if load_status == "":
      load_status = "NA"
    else:
      load_status = str(list(set(load_status.split(",")))).replace("[", "").replace("]", "").replace("'", "").replace(" ", "")[1:]

    diff_access_point = bytes_access_point - old_bytes_access_point
    diff_firewall = bytes_firewall - old_bytes_firewall

    mbps_access_point = diff_access_point * 8 / 1_000_000 / 60
    mbps_firewall = diff_firewall * 8 / 1_000_000 / 60

    consistency = 0

    if diff_firewall != 0:
      consistency = (diff_access_point / diff_firewall) * 100

    csv_rows.append([
      date,
      mbps_access_point,
      mbps_firewall,
      consistency,
      load_status
    ])

    old_bytes_access_point = bytes_access_point
    old_bytes_firewall = bytes_firewall

  with open("./data-01-bronze/checkpoint.json", "w") as file:
    json.dump({
      "date": files_to_transform[-1].split("_")[0] + "_" + files_to_transform[-1].split("_")[1]
    }, file, indent=2)
    
  s3_client.upload_file("./data-01-bronze/checkpoint.json", BUCKET_NAME, "01-bronze/checkpoint.json")

  date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
  file_name = f"./data-02-silver/{date}.csv"
  with open(file_name, "w") as csvfile:
    for row in csv_rows:
      csv.writer(csvfile, delimiter=";", lineterminator='\n').writerow(row) 

  s3_client.upload_file(file_name, BUCKET_NAME, f"02-silver/{date}.csv")

def getLoadStatus (data):
  status = ""

  if data["active_conn"] > 40:
    status += "HIGH_DENSITY,"
  if data["cpu_percent"] > 80:
    status += "HIGH_PROCCESSING,"
  if data["ram_percent"] > 75:
    status += "OOM,"

  return status

def getDeviceType (file_name: str):
  if file_name.split("_")[2][0] == "a":
    return "ACCESS_POINT"
   
  if file_name.split("_")[2][0] == "f":
    return "FIREWALL"

  return None

__init__()