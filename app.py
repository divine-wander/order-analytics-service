# AI was used to generate the python code below.
import os

from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS


# Load environment variables from a local .env file.
# In Azure, these values will come from App Service application settings.
load_dotenv()


# Create the Flask application.
app = Flask(__name__)

# Allow requests from other origins such as the Store Front application.
CORS(app)


@app.route("/health", methods=["GET"])
def health():
    """
    Health check endpoint used to confirm that the service is running.
    """

    return jsonify({
        "status": "healthy",
        "service": "order-analytics-service"
    })


if __name__ == "__main__":
    # Read the service port from the environment.
    # Port 4000 is used as the default for local development.
    port = int(os.getenv("PORT", "4000"))

    # Listen on all interfaces so the service can later run inside Azure.
    app.run(host="0.0.0.0", port=port)
    
    
