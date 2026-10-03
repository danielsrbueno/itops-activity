import random
import psutil
import json
import datetime

AP_ID="AP001"

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
  
  with open(f"./data/{date}_{AP_ID.lower()}.json", "w") as file:
    json.dump(data, file, indent=2)

__init__()