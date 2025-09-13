import os
import threading
from flask import Flask, jsonify

app = Flask(__name__)

@app.get("/")
def index():
    return "OK", 200

@app.get("/health")
def health():
    return jsonify({"ok": True}), 200

def run():
    # ЯВНО отключаем reloader, порт читаем из ENV (дефолт 8080)
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

def start():
    # Запускаем Flask ОДИН раз в отдельном daemon-потоке
    t = threading.Thread(target=run, daemon=True, name="keep-alive")
    t.start()
