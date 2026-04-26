from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from mini_redis import MiniRedis
from disk_storage import DiskStorage
import time

app = Flask(__name__)
CORS(app)

redis_store = MiniRedis(capacity=10)
disk_store = DiskStorage(algorithm="linear")
operation_log = []
session_locked = False


def log_operation(op, key, redis_result, disk_result):
    entry = {
        "id": len(operation_log) + 1,
        "op": op,
        "key": key,
        "redis_ms": redis_result.get("time_ms", 0),
        "disk_ms": disk_result.get("time_ms", 0),
        "redis_status": redis_result.get("status", ""),
        "disk_status": disk_result.get("status", ""),
        "disk_steps": disk_result.get("steps", 0),
        "disk_store_size": disk_result.get("store_size", len(disk_store.store)),
        "redis_original_size": redis_result.get("original_size", 0),
        "redis_compressed_size": redis_result.get("compressed_size", 0),
        "timestamp": time.strftime("%H:%M:%S"),
        "algorithm": disk_store.algorithm
    }
    operation_log.append(entry)
    return entry


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/session/start", methods=["POST"])
def start_session():
    global session_locked
    data = request.json or {}
    algo = data.get("algorithm", "linear")
    disk_store.set_algorithm(algo)
    redis_store.flush()
    operation_log.clear()
    session_locked = True
    return jsonify({"status": "OK", "algorithm": algo})


@app.route("/api/session/reset", methods=["POST"])
def reset_session():
    global session_locked
    redis_store.flush()
    disk_store.flush()
    operation_log.clear()
    session_locked = False
    return jsonify({"status": "OK"})


@app.route("/api/command", methods=["POST"])
def command():
    if not session_locked:
        return jsonify({"error": "No active session. Start a session first."}), 400

    data = request.json or {}
    raw = data.get("command", "").strip()
    parts = raw.split()

    if not parts:
        return jsonify({"error": "Empty command"}), 400

    op = parts[0].upper()

    if op == "SET":
        if len(parts) < 3:
            return jsonify({"error": "Usage: SET key value [EX seconds]"}), 400
        key = parts[1]
        ttl = None
        if len(parts) >= 5 and parts[3].upper() == "EX":
            try:
                ttl = int(parts[4])
            except ValueError:
                return jsonify({"error": "TTL must be integer"}), 400
            value = parts[2]
        else:
            value = " ".join(parts[2:])
        redis_result = redis_store.set(key, value, ttl=ttl)
        disk_result = disk_store.set(key, value)
        log_entry = log_operation("SET", key, redis_result, disk_result)
        return jsonify({
            "op": "SET", "key": key, "value": value,
            "redis": redis_result, "disk": disk_result, "log": log_entry,
            "redis_state": redis_store.get_state(),
            "disk_state": disk_store.get_state()
        })

    elif op == "GET":
        if len(parts) < 2:
            return jsonify({"error": "Usage: GET key"}), 400
        key = parts[1]
        redis_result = redis_store.get(key)
        disk_result = disk_store.get(key)
        log_entry = log_operation("GET", key, redis_result, disk_result)
        return jsonify({
            "op": "GET", "key": key,
            "redis": redis_result, "disk": disk_result, "log": log_entry,
            "redis_state": redis_store.get_state(),
            "disk_state": disk_store.get_state()
        })

    elif op == "DEL":
        if len(parts) < 2:
            return jsonify({"error": "Usage: DEL key"}), 400
        key = parts[1]
        redis_result = redis_store.delete(key)
        disk_result = disk_store.delete(key)
        log_entry = log_operation("DEL", key, redis_result, disk_result)
        return jsonify({
            "op": "DEL", "key": key,
            "redis": redis_result, "disk": disk_result, "log": log_entry,
            "redis_state": redis_store.get_state(),
            "disk_state": disk_store.get_state()
        })

    else:
        return jsonify({"error": f"Unknown command: {op}. Use SET, GET, or DEL"}), 400


@app.route("/api/state", methods=["GET"])
def state():
    return jsonify({
        "redis": redis_store.get_state(),
        "disk": disk_store.get_state(),
        "session_active": session_locked,
        "algorithm": disk_store.algorithm
    })


@app.route("/api/logs", methods=["GET"])
def logs():
    return jsonify({"logs": operation_log})


@app.route("/api/analytics", methods=["GET"])
def analytics():
    if not operation_log:
        return jsonify({"labels": [], "redis_times": [], "disk_times": [], "speedup": [], "redis_memory": 0, "disk_memory": 0, "op_count": 0})
    labels = [f"#{e['id']} {e['op']} {e['key']}" for e in operation_log]
    redis_times = [e["redis_ms"] for e in operation_log]
    disk_times = [e["disk_ms"] for e in operation_log]
    speedup = [round(d / r, 1) if r > 0 else 0 for r, d in zip(redis_times, disk_times)]
    redis_state = redis_store.get_state()
    disk_state = disk_store.get_state()
    return jsonify({
        "labels": labels, "redis_times": redis_times,
        "disk_times": disk_times, "speedup": speedup,
        "redis_memory": redis_state["total_memory"],
        "disk_memory": disk_state["total_memory"],
        "op_count": len(operation_log)
    })


if __name__ == "__main__":
    app.run(debug=True, port=5000)
