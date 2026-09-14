import logging
import os

from flask import Flask, jsonify, request, render_template, send_file, abort

import pipeline
import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

app = Flask(__name__)

storage.init()

_started = os.environ.get("WEB_CONCURRENCY", "1")  # gunicorn sets this; used only to guard the scheduler below
_scheduler_started = False


@app.before_request
def _ensure_scheduler():
    global _scheduler_started
    if not _scheduler_started:
        _scheduler_started = True
        pipeline.start_background_loop()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/healthz")
def healthz():
    return jsonify(ok=True)


@app.route("/api/state")
def api_state():
    return jsonify({
        "tracked": storage.kv_get("tracked_player"),
        "search": storage.kv_get("search_current"),
        "stats": storage.kv_get("stats_summary"),
        "status": storage.kv_get("status"),
    })


@app.route("/api/search", methods=["POST"])
def api_search():
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    if not name:
        return jsonify(error="name is required"), 400
    pipeline.submit_search(name)
    return jsonify(ok=True)


@app.route("/api/track", methods=["POST"])
def api_track():
    body = request.get_json(silent=True) or {}
    candidate = body.get("candidate")
    if not candidate or not candidate.get("profile_id"):
        return jsonify(error="candidate with profile_id is required"), 400
    pipeline.confirm_player(candidate)
    return jsonify(ok=True)


@app.route("/api/untrack", methods=["POST"])
def api_untrack():
    pipeline.untrack_player()
    return jsonify(ok=True)


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    import threading
    threading.Thread(target=pipeline.refresh_tracked_player, kwargs={"force": False}, daemon=True).start()
    return jsonify(ok=True)


@app.route("/replays/<int:match_id>")
def download_replay(match_id):
    match = storage.get_match(match_id)
    if not match or not match.get("replay_path") or not os.path.exists(match["replay_path"]):
        abort(404)
    return send_file(
        match["replay_path"],
        as_attachment=True,
        download_name=f"{match_id}.aoe2record",
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
