from flask import Flask
from flask_cors import CORS
from housing.logger import logging
from housing.exception import HousingException
import sys

app = Flask(__name__)
CORS(app)


@app.route("/")
def hello():
    try:
        raise Exception("testing custom exception")
    except Exception as e:
        housing = HousingException(e, sys)
        logging.info(housing.error_message)
        logging.info("testing the logging module")
    return "Hello, World!"


if __name__ == "__main__":
    app.run(debug=True)
