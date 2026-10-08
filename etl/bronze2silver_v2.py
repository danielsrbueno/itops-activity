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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SILVER_DIR = os.path.join(BASE_DIR, "data-02-silver")
BRONZE_DIR = os.path.join(BASE_DIR, "data-01-bronze")

os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(BRONZE_DIR, exist_ok=True)

def __init__():
  last_read_data = {
    "last_file_read": "", "last_bytes": None
  }

  try:
    res_chk = s3_client.get_object(Bucket=BUCKET_NAME, Key="01-bronze/checkpoint.json")
    chk = json.loads(res_chk["Body"].read())
    last_read_data["last_file_read"] = chk.get("last_file_read", "")
    last_read_data["last_bytes"] = chk.get("last_bytes")
  
  except:
    pass

  print(last_read_data)
  try:
    response = s3_client.list_objects_v2(
      Bucket=BUCKET_NAME,
      Prefix="01-bronze/"
    )
  except Exception as e:
    print(e)
    return

  csv_rows = [["created_at", "id", "cpu", "ram", "bytes_sent", "bytes_recv", "active_conn", "consistency", "status", "mbps", "top_blocked_ip", "dropped_packets", "active_sessions"]]

  all_files = [
    file["Key"].split("/")[-1] for file in response.get("Contents", []) if file["Key"].split("/")[-1] not in ["", "checkpoint.json"] and file["Key"].split("/")[-1] > last_read_data["last_file_read"] 
  ]

  if len(all_files) == 0:
    return print("Não há novos dados para transformar.")

  all_files.sort()

  dates = [
    getFileDate(file) for file in all_files
  ]

  dates = list(set(dates))
  dates.sort()

  ids = [
    file.split("_")[-1].split(".")[0].upper() for file in all_files
  ]

  ids = list(set(ids))
  ids.sort()

  last_read = [
    {
      "bytes_sent": 0, 
      "bytes_recv": 0
    }
    for _ in range(0, len(ids))
  ]

  if last_read_data["last_bytes"] is not None:
    last_read = [
      last_read_data["last_bytes"].get(id, {
        "bytes_sent": 0, "bytes_recv": 0
      })
      for id in ids
    ]

  for i, date in enumerate(dates):
    files2read = [
      file for file in all_files if getFileDate(file) == date
    ]

    total_bytes_ap = 0
    csv_row: list
    for file_name in files2read:
      try:
        file = s3_client.get_object(
          Bucket=BUCKET_NAME,
          Key=f"01-bronze/{file_name}"
        )
      except Exception as e:
        print(e)
        continue

      data = json.loads(file["Body"].read())

      idx = ids.index(data.get("id", "FIREWALL"))
      sent_diff = data["bytes_sent"] - last_read[idx]["bytes_sent"]
      recv_diff = data["bytes_recv"] - last_read[idx]["bytes_recv"]

      if (last_read[idx]["bytes_sent"] == 0 and i == 0):
        sent_diff = 0
        recv_diff = 0

      csv_row = [
        getFormmatedDate(data["created_at"]),
        data.get("id", "FIREWALL"),
        data["cpu_percent"],
        data["ram_percent"],
        sent_diff,
        recv_diff
      ]

      if file_name.split("_")[-1].startswith("ap"):
        total_bytes_ap += sent_diff
        csv_row.append(data["active_conn"])
        csv_row.append("")
        csv_row.append(getLoadStatus(data))

      elif file_name.split("_")[-1].startswith("firewall"):
        print(data["bytes_sent"])
        print(last_read[idx]["bytes_sent"])

        if (last_read[idx]["bytes_sent"] == 0 and i == 0):
          consistency = 0
        else:
          consistency = (total_bytes_ap / 4) / sent_diff * 100

        csv_row.append("")
        csv_row.append(consistency)
        csv_row.append("")

      mbps = sent_diff * 8 / 1_000_000 / 60
      if (last_read[idx]["bytes_sent"] == 0 and i == 0):
        mbps = 0

      csv_row.append(mbps)

      if file_name.split("_")[-1].startswith("firewall"):
        csv_row.append(data.get("top_blocked_ip", ""))
        csv_row.append(data.get("dropped_packets", ""))
        csv_row.append(data.get("active_sessions", ""))
      else:
        csv_row.extend(["", "", ""])

      csv_rows.append(csv_row)
      last_read[idx] = {
        "bytes_sent": data["bytes_sent"],
        "bytes_recv": data["bytes_recv"]
      }

  print(ids)
  print(last_read)
  date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
  file_name = os.path.join(SILVER_DIR, f"{date}.csv")

  with open(file_name, "w") as csvfile:
    for row in csv_rows:
      csv.writer(csvfile, delimiter=";", lineterminator='\n').writerow(row) 
  
  s3_client.upload_file(file_name, BUCKET_NAME, f"02-silver/{date}.csv")

  with open("./data-01-bronze/checkpoint.json", "w") as file:
    checkpoint = {
      "last_file_read": files2read[-1],
      "last_bytes": dict(zip(ids, last_read))
    }
    json.dump(checkpoint, file, indent=2)

  s3_client.upload_file("./data-01-bronze/checkpoint.json", BUCKET_NAME, "01-bronze/checkpoint.json")

  
def getFileDate(filename: str):
  date = filename.split("_")[0] + "_" + filename.split("_")[1]
  return date

def getFormmatedDate(date: str):
  formmatedDate = date.split("_")[0] + " " + date.split("_")[1].replace("-", ":")
  return formmatedDate

def getLoadStatus (data: object):
  status = ""

  if data["active_conn"] > 40:
    status += "HIGH_DENSITY,"
  if data["cpu_percent"] > 80:
    status += "HIGH_PROCCESSING,"
  if data["ram_percent"] > 75:
    status += "OOM,"

  if status == "":
    status = "na"
  else:
    status = status[:-1]

  return status

__init__()