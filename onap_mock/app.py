from flask import Flask, request, jsonify
import os
import requests
from kafka import KafkaProducer
import json


app = Flask(__name__)


KAFKA_BOOTSTRAP = os.environ.get('KAFKA_BOOTSTRAP', 'kafka:9092')
NMS_URL = os.environ.get('NMS_URL', 'http://nms_mock:5100')
producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP], value_serializer=lambda v: json.dumps(v).encode('utf-8'))


@app.route('/orchestrate/create_service', methods=['POST'])
def create_service():
    data = request.json or {}
    service_id = data.get('service_id', 'svc-' + str(os.getpid()))
    # 1) create inventory entry in NMS
    resp = requests.post(f"{NMS_URL}/inventory/services", json={"service_id": service_id, "meta": data.get('meta', {})})
    # 2) emit event on Kafka that orchestration executed
    event = {"type": "service_created", "service_id": service_id}
    producer.send('orchestration-events', event)
    producer.flush()
    return jsonify({"status": "created", "service_id": service_id, "nms": resp.json()}), 201


@app.route('/orchestrate/delete_service', methods=['POST'])
def delete_service():
    data = request.json or {}
    service_id = data.get('service_id')
    if not service_id:
        return jsonify({"error": "service_id required"}), 400
    requests.delete(f"{NMS_URL}/inventory/services/{service_id}")
    event = {"type": "service_deleted", "service_id": service_id}
    producer.send('orchestration-events', event)
    producer.flush()
    return jsonify({"status": "deleted", "service_id": service_id})


@app.route('/deploy', methods=['POST'])
def deploy():
    data = request.json or {}
    service = data.get('service')
    if not service:
        return jsonify({"error": "service required"}), 400
    # Use the existing create_service logic
    service_id = f"svc-{service}-{os.getpid()}"
    # 1) create inventory entry in NMS
    resp = requests.post(f"{NMS_URL}/inventory/services", json={"service_id": service_id, "meta": {"service": service}})
    # 2) emit event on Kafka that orchestration executed
    event = {"type": "service_created", "service_id": service_id}
    producer.send('orchestration-events', event)
    producer.flush()
    return jsonify({"status": "created", "service_id": service_id, "nms": resp.json()}), 201


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)