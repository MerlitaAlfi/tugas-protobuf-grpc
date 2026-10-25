from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'stream-log-audit',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(' [*] KONSUMEN KAFKA: Menunggu aliran log audit... (Tekan Ctrl+C untuk keluar)')

for message in consumer:
    log = message.value
    print(f" [AUDIT DATA] Menyimpan riwayat transaksi {log['id_transaksi']} ke sistem arsip Linux.")