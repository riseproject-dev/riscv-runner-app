import logging
from waitress import serve
from handler import app
from worker import start_worker

# Set the logging level for all loggers to DEBUG
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

HOST = "0.0.0.0"
PORT = 8080

start_worker()

print(f"Starting server on http://{HOST}:{PORT}")
serve(app, host=HOST, port=PORT)
