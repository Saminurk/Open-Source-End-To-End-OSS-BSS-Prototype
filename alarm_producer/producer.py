import os
import time
import uuid
import json
from kafka import KafkaProducer
import requests


KAFKA = os.environ.get('KAFKA_BOOTSTRAP','kafka:9092')
NMS = os.environ.get('NMS_URL','http://nms_mock:5100')
INTERVAL = int(os.environ.get('PRODUCER_INTERVAL','5'))


producer = KafkaProducer(bootstrap_servers=[KAFKA], value_serializer=lambda v: json.dumps(v).encode('utf-8'))


while True:
    aid = 'alarm-' + uuid.uuid4().hex[:8]
    alarm = {
        'alarm_id': aid,
        'severity': 'MAJOR',
        'resource': 'cell-123',
        'description': 'Simulated major alarm',
        'state': 'RAISED'
    }
    # post to NMS
    try:
        requests.post(f"{NMS}/alarms", json=alarm, timeout=3)
    except Exception as e:
        print('NMS POST failed', e)
    # send to kafka topic
    producer.send('alarms', alarm)
    producer.flush()
    print('Produced alarm', aid)
    time.sleep(INTERVAL)