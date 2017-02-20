from flask import Flask
from .blueprint import BLUEPRINT

app = Flask(__name__)

app.config.from_envvar("MATERIALSUITE_ENDPOINT_CONFIG", silent=True)

app.register_blueprint(BLUEPRINT)
