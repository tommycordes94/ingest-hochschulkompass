import traceback
import os
from flask import Flask, request
import logging
from logging.config import dictConfig
from trdpipe.structify_publish.helper import loadConfig
from service.ingester import Ingester
from datetime import date

# Konfiguration des Loggings
dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '%(message)s',
    }},
    'handlers': {'console': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://sys.stdout',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['console']
    }
})

app = Flask(__name__)

@app.route("/", methods=['GET'])
def ingest():
    # Holen des 'env'-Parameters aus der GET-Anfrage
    env = request.args.get("env") 
    # Laden der Konfiguration basierend auf dem 'env'-Parameter
    config = loadConfig(env,'') 

    try:
        # Hier kommt der Ingestion-Code
        i = Ingester(config=config, subsrc=date.today().strftime("%Y%m%d"))
        i.ingest()

        return "successfully ingested", 200
    
    except Exception as e:
        error = f"ERROR OCCURRED: {e}"
        logging.error(error)
        logging.error(traceback.format_exc())
        return error, 400

"""
THIS IS THE MAIN ENTRY POINT
"""
if __name__ == "__main__":
    app.logger.setLevel(logging.DEBUG)
    app.run(
        debug=True, 
        host="0.0.0.0", 
        port=int(os.environ.get("PORT", 8080)))
