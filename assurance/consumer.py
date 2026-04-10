import os
from flask import Flask, Response
from kafka import KafkaConsumer
import json
from prometheus_client import Counter, generate_latest, CollectorRegistry, CONTENT_TYPE_LATEST
import threading


app = Flask(__name__)
KAFKA = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')


# prometheus metrics
registry = CollectorRegistry()
alarm_counter = Counter('ossbss_alarms_total', 'Total alarms processed', ['severity'], registry=registry)


# kafka consumer runs in thread


def consume():
    consumer = KafkaConsumer('alarms', bootstrap_servers=[KAFKA], value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest', enable_auto_commit=True, group_id='assurance-group')
    for msg in consumer:
        alarm = msg.value
        sev = alarm.get('severity', 'UNKNOWN')
        alarm_counter.labels(severity=sev).inc()
        print('Consumed alarm', alarm.get('alarm_id'))


threading.Thread(target=consume, daemon=True).start()


@app.route('/metrics')
def metrics():
    return Response(generate_latest(registry), mimetype=CONTENT_TYPE_LATEST)


@app.route('/')
def index():
    return 'Assurance service. Metrics at /metrics'


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)