import argparse
import datetime
import json
import logging
import os.path
import pathlib
import signal
import time
from flask import Flask, request, abort, send_file
import gevent.pywsgi
from PIL import Image, UnidentifiedImageError

from database import ModelDatabase
    

if __name__ == "__main__":

    # Parse command line arguments
    args_parser = argparse.ArgumentParser()
    storage_args = args_parser.add_argument_group(
        title="Storage"
    )
    storage_args.add_argument(
        "--upload-dir",
        help="Specify the directory to save the uploaded images.",
        type=str,
        default=pathlib.Path(__file__).parent.joinpath("upload")
    )
    network_args = args_parser.add_argument_group(
        title="Networking"
    )
    network_args.add_argument(
        "--ipv4",
        help="Listen on IPv4 only.",
        action="store_true"
    )
    args = args_parser.parse_args()

    # Initialize database
    config = json.load(open(pathlib.Path(__file__).parent.joinpath("config.json")))
    db = ModelDatabase(
        host=config["database"]["host"],
        port=config["database"]["port"],
        user=config["database"]["user"],
        password=config["database"]["password"],
        database=config["database"]["schema"]
    )
    db.connect()

    # Define Flask application
    app = Flask(__name__)

    # Path for handling data upload
    @app.route("/upload", methods=['POST'])
    def upload():
        if not all([form_name in request.form for form_name in ["river_name", "points", "depth"]]):
            logging.warning("Missing some fields from the sent form.")
            abort(400)
        if "image" not in request.files:
            logging.warning("Missing \"image\" from the sent form.")
            abort(400)
        try:
            json.loads(request.form["points"])
        except json.JSONDecodeError as e:
            logging.exception("Invalid \"points\" data.", exc_info=e)
            abort(400)
        try:
            depth = float(request.form["depth"])
        except ValueError as e:
            logging.exception("Invalid \"depth\" data.", exc_info=e)
            abort(400)
        try:
            image = Image.open(request.files["image"].stream)
        except UnidentifiedImageError as e:
            logging.exception("Invalid \"image\" data.", exc_info=e)
            abort(400)

        try:
            timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            image_path = pathlib.Path(args.upload_dir).joinpath(f"{timestamp}.png")
            if not os.path.isdir(image_path.parent):
                os.makedirs(image_path.parent)
            image.save(image_path)
            db.insert_data(
                timestamp=timestamp,
                river_name=request.form["river_name"],
                country_name=request.form["country_name"] if "country_name" in request.form else "",
                basin_name=request.form["basin_name"] if "basin_name" in request.form else "",
                est_level=depth,
                points=json.loads(request.form["points"])
            )
            return {"status": "OK"}
        except Exception as e:
            logging.exception("Unable to process data!", exc_info=e)
            return {"error": f"{e.__class__.__name__}: {e}"}, 500
        
    # Path for handling data request
    @app.route("/retrieve", methods=["POST"])
    def send_data():
        if not request.is_json:
            logging.warning("Rejected non-JSON request for '/retrieve'.")
            abort(400)
        start_ts = request.json["start"] if "start" in request.json and isinstance(request.json["start"], int) else None
        end_ts = request.json["end"] if "end" in request.json and isinstance(request.json["end"], int) else None
        
        try:
            return [ r for r in db.retrieve_data(start_ts=start_ts, end_ts=end_ts) ]
        except Exception as e:
            logging.exception("Unable to retrieve data!", exc_info=e)
            return {"error": f"{e.__class__.__name__}: {e}"}, 500
        
    # Path for handling data request
    @app.route("/image/<int:timestamp>")
    def send_image(timestamp):
        image_path = pathlib.Path(args.upload_dir).joinpath(f"{timestamp}.png")
        if not image_path.is_file():
            abort(404)
        return send_file(image_path)
    
    # Demo pages
    @app.route("/demo")
    def demo_index():
        return open(pathlib.Path(__file__).parent.joinpath("demo", "index.html"), "rb")

    # Log level
    logging.basicConfig(level=logging.DEBUG)

    # Start HTTP server
    server = gevent.pywsgi.WSGIServer(
        listener=("127.0.0.1" if args.ipv4 else "::1", config["port"]),
        application=app,
        log=logging.getLogger()
    )
    for signum in [ signal.SIGINT, signal.SIGTERM ]:
        gevent.signal_handler(
            signalnum=signum,
            handler=lambda : server.close()
        )
    server.serve_forever()
