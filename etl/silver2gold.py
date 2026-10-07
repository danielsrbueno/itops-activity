import csv
import datetime
import json
import boto3
from dotenv import load_dotenv
import os

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
MINUTES = 6

session = boto3.Session(
  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
  aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
  region_name="us-east-1"
)
s3_client = session.client("s3")

def __init__():
  try:
    response = s3_client.list_objects_v2(
      Bucket=BUCKET_NAME,
      Prefix="01-bronze/"
    )
  except Exception as e:
    print(e)
    return
  
  # Relatório de "Zonas Mortas" ou Subutilizadas
  # Um CSV que agrupa as antenas pelo menor volume
  # de tráfego. Identifica Access Points que estão ligados gastando energia, mas que ninguém usa,
  # sugerindo um redimensionamento físico das antenas no prédio.
  csv_rows = [["created_at", "id", "priority", "mbps_sent", "mbps_recv", "active_conn", "description"]]

  all_files = [
    file["Key"].split("/")[-1] for file in response.get("Contents", []) if file["Key"].split("/")[-1] not in ["", "checkpoint.json"]
  ]

  ids = [
    file.split("_")[-1].split(".")[0].upper() for file in all_files
  ]

  ids = list(set(ids))
  ids.sort()

  for id in ids:
    if id == "FIREWALL":
      continue

    files_to_read = [
      file
      for file in all_files if file.split("_")[-1].split(".")[0].upper() == id
    ]
    mbps_sent_mean = 0
    mbps_recv_mean = 0
    active_conn_mean = 0
    for file_name in files_to_read:
      try:
        file = s3_client.get_object(
          Bucket=BUCKET_NAME,
          Key=f"01-bronze/{file_name}"
        )
      except Exception as e:
        print(e)
        continue

      data = json.loads(file["Body"].read())

      if data.get("bytes_sent", 0) == 0 or data.get("bytes_recv", 0) == 0:
        continue

      mbps_sent_mean += data.get("bytes_sent", 0)
      mbps_recv_mean += data.get("bytes_recv", 0)
      active_conn_mean += data.get("active_conn", 0)
      print(active_conn_mean)

    mbps_sent_mean = mbps_sent_mean / len(files_to_read)
    mbps_recv_mean = mbps_recv_mean / len(files_to_read)
    active_conn_mean = active_conn_mean / len(files_to_read)
    print(active_conn_mean)

    mbps_sent_mean = mbps_sent_mean * 8 / 1_000_000 / (60 * MINUTES)
    mbps_recv_mean = mbps_recv_mean * 8 / 1_000_000 / (60 * MINUTES)

    if mbps_sent_mean < 5 and mbps_recv_mean < 5 and active_conn_mean < 5:
      csv_rows.append([
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        id,
        "Alta",
        f"{int(mbps_sent_mean)} Mbps",
        f"{int(mbps_recv_mean)} Mbps",
        f"{int(active_conn_mean)} conexões",
        "O access point está em uma zona morta ou subutilizada, com baixa taxa de transferência e poucas conexões ativas."
      ])
    elif mbps_sent_mean < 10 and mbps_recv_mean < 10 and active_conn_mean < 10:
      csv_rows.append([
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        id,
        "Média",
        f"{int(mbps_sent_mean)} Mbps",
        f"{int(mbps_recv_mean)} Mbps",
        f"{int(active_conn_mean)} conexões",
        "O access point está em uma zona de baixa utilização, com taxa de transferência e conexões ativas moderadas."
      ])
    elif mbps_sent_mean < 20 and mbps_recv_mean < 20 and active_conn_mean < 20:
      csv_rows.append([
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        id,
        "Baixa",
        f"{int(mbps_sent_mean)} Mbps",
        f"{int(mbps_recv_mean)} Mbps",
        f"{int(active_conn_mean)} conexões",
        "O access point está em uma zona de utilização moderada, com taxa de transferência e conexões ativas razoáveis."
      ])
    else:
      csv_rows.append([
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        id,
        "Muito Baixa",
        f"{int(mbps_sent_mean)} Mbps",
        f"{int(mbps_recv_mean)} Mbps",
        f"{int(active_conn_mean)} conexões",
        "O access point está em uma zona de alta utilização, com boa taxa de transferência e conexões ativas."
      ])

  date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
  file_name = f"./data-03-gold/{date}-deadzones.csv"
  with open(file_name, "w") as csvfile:
    for row in csv_rows:
      csv.writer(csvfile, delimiter=";", lineterminator='\n').writerow(row) 
  
    s3_client.upload_file(file_name, BUCKET_NAME, f"03-gold/{date}-deadzones.csv")

  # Relatório de Eficiência de Hardware: 
  # Um ranking das antenas que possuem a maior relação Tráfego de Rede / Consumo de CPU. 
  # Identificar equipamentos antigos ou com firmware problemático que consomem muita 
  # CPU para processar pouco tráfego.
  csv_rows = [["created_at", "id", "rating", "mbps_sent", "mbps_recv", "cpu", "ram", "description", "ranking"]]
  id_ranking = []
  rankings = []
  for id in ids:
    if id == "FIREWALL":
      continue

    files_to_read = [
      file
      for file in all_files if file.split("_")[-1].split(".")[0].upper() == id
    ]

    mbps_sent_mean = 0
    mbps_recv_mean = 0
    cpu_mean = 0
    ram_mean = 0
    for file_name in files_to_read:
      try:
        file = s3_client.get_object(
          Bucket=BUCKET_NAME,
          Key=f"01-bronze/{file_name}"
        )
      except Exception as e:
        print(e)
        continue

      data = json.loads(file["Body"].read())

      if data.get("bytes_sent", 0) == 0 or data.get("bytes_recv", 0) == 0:
        continue

      mbps_sent_mean += data.get("bytes_sent", 0)
      mbps_recv_mean += data.get("bytes_recv", 0)
      cpu_mean += data.get("cpu_percent", 0)
      ram_mean += data.get("ram_percent", 0)

    mbps_sent_mean = mbps_sent_mean / len(files_to_read)
    mbps_recv_mean = mbps_recv_mean / len(files_to_read)
    cpu_mean = cpu_mean / len(files_to_read)
    ram_mean = ram_mean / len(files_to_read)

    mbps_sent_mean = mbps_sent_mean * 8 / 1_000_000 / (60 * MINUTES)
    mbps_recv_mean = mbps_recv_mean * 8 / 1_000_000 / (60 * MINUTES)

    eficiency = (mbps_sent_mean * mbps_recv_mean) / (cpu_mean * ram_mean)
    id_ranking.append(id)
    rankings.append(eficiency)

    if eficiency < 0.1:
      csv_rows.append([
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        id,
        f"{int(eficiency * 100)} pontos",
        f"{int(mbps_sent_mean)} Mbps",
        f"{int(mbps_recv_mean)} Mbps",
        f"{int(cpu_mean)}%",
        f"{int(ram_mean)}%",
        "O access point possui uma relação ruim entre tráfego de rede e consumo de CPU e RAM, sugerindo problemas de desempenho ou firmware."
      ])
    elif eficiency < 0.5:
      csv_rows.append([
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        id,
        f"{int(eficiency * 100)} pontos",
        f"{int(mbps_sent_mean)} Mbps",
        f"{int(mbps_recv_mean)} Mbps",
        f"{int(cpu_mean)}%",
        f"{int(ram_mean)}%",
        "O access point possui uma relação moderada entre tráfego de rede e consumo de CPU e RAM, podendo ser melhorado com ajustes de configuração ou atualização de firmware."
      ])
    else:
      csv_rows.append([
        datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        id,
        f"{int(eficiency * 100)} pontos",
        f"{int(mbps_sent_mean)} Mbps",
        f"{int(mbps_recv_mean)} Mbps",
        f"{int(cpu_mean)}%",
        f"{int(ram_mean)}%",
        "O access point possui uma boa relação entre tráfego de rede e consumo de CPU e RAM."
      ])

  id_ranking = [x for _, x in sorted(zip(rankings, id_ranking), reverse=True)]
  for i, id in enumerate(id_ranking):
    for row in csv_rows:
      if row[1] == id:
        row.append(i + 1)
        break

  date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
  file_name = f"./data-03-gold/{date}-efficiency.csv"
  with open(file_name, "w") as csvfile:
    for row in csv_rows:
      csv.writer(csvfile, delimiter=";", lineterminator='\n').writerow(row) 
  
    s3_client.upload_file(file_name, BUCKET_NAME, f"03-gold/{date}-efficiency.csv")    
  
__init__()