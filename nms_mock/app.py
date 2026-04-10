from flask import Flask, request, jsonify
app = Flask(__name__)


# in-memory store for inventory + alarms
inventory = {"services": {}}
alarms = {}


@app.route('/inventory/services', methods=['POST'])
def add_service():
    data = request.json
    sid = data['service_id']
    inventory['services'][sid] = data.get('meta', {})
    return jsonify({"status": "ok", "service_id": sid})


@app.route('/inventory/services/<sid>', methods=['DELETE'])
def delete_service(sid):
    inventory['services'].pop(sid, None)
    return jsonify({"status": "deleted", "service_id": sid})


# Alarm lifecycle
@app.route('/alarms', methods=['POST'])
def create_alarm():
    data = request.json
    aid = data.get('alarm_id')
    alarms[aid] = data
    return jsonify({"status": "created", "alarm_id": aid}), 201


@app.route('/alarms/<aid>/ack', methods=['POST'])
def ack_alarm(aid):
    if aid in alarms:
        alarms[aid]['state'] = 'ACK'
        return jsonify({"status": "acked", "alarm_id": aid})
    return jsonify({"error": "notfound"}), 404


@app.route('/alarms/<aid>/clear', methods=['POST'])
def clear_alarm(aid):
    if aid in alarms:
        alarms[aid]['state'] = 'CLEARED'
        return jsonify({"status": "cleared", "alarm_id": aid})
    return jsonify({"error": "notfound"}), 404


@app.route('/alarms', methods=['GET'])
def list_alarms():
    return jsonify(list(alarms.values()))


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5100)