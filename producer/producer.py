import json
import time
import random
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

# config kafka
KAFKA_SERVER = "localhost:9092"
TOPIC = "transactions"
JUMLAH_EVENT = 60

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

sources = ["mobile", "web", "pos"]


def get_waktu(mundur_detik=0):
    waktu = datetime.now(timezone.utc) - timedelta(seconds=mundur_detik)
    return waktu.strftime("%Y-%m-%dT%H:%M:%SZ")


def buat_event():
    return {
        "user_id": "U" + str(random.randint(10000, 99999)),
        "amount": random.randint(10000, 2000000),
        "timestamp": get_waktu(),
        "source": random.choice(sources),
    }


# event ke berapa yang dibikin invalid / late
# late taruh agak belakang biar watermark nya udah jalan dulu
event_khusus = {
    5: "amount_minus",
    8: "timestamp_salah",
    11: "source_salah",
    14: "duplikat",
    17: "amount_kegedean",
    20: "late",
    23: "tanpa_user_id",
    26: "amount_string",
    29: "late",
    32: "duplikat",
    35: "late",
}

event_sebelumnya = None

for i in range(1, JUMLAH_EVENT + 1):
    jenis = event_khusus.get(i, "valid")
    data = buat_event()

    if jenis == "amount_minus":
        data["amount"] = -50000
    elif jenis == "amount_kegedean":
        data["amount"] = 999999999
    elif jenis == "amount_string":
        data["amount"] = "seratus ribu"
    elif jenis == "timestamp_salah":
        data["timestamp"] = "12/14/2025"
    elif jenis == "source_salah":
        data["source"] = "telegram"
    elif jenis == "tanpa_user_id":
        del data["user_id"]
    elif jenis == "duplikat":
        data = event_sebelumnya.copy()
    elif jenis == "late":
        # mundur 5-10 menit, lebih dari watermark 3 menit
        data["timestamp"] = get_waktu(random.randint(300, 600))

    if jenis == "valid":
        event_sebelumnya = data

    key = data.get("user_id")
    if key:
        key = key.encode("utf-8")

    producer.send(TOPIC, key=key, value=json.dumps(data).encode("utf-8"))
    producer.flush()

    print(i, jenis, data)

    # kirim tiap 1-2 detik
    time.sleep(random.uniform(1, 2))

producer.close()
print("selesai kirim", JUMLAH_EVENT, "event")
