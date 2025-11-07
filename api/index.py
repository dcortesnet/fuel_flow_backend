from flask import Flask, jsonify
from flask_cors import CORS
from .routes import pedidos_bp, auth_bp
#from config import Config

app = Flask(__name__)
#app.config.from_object(Config)

CORS(app)

#app.register_blueprint(pedidos_bp, url_prefix='/api')
#app.register_blueprint(auth_bp, url_prefix='/api')

print("HELLO")

@app.route('/', methods=['GET'])
def hello_world():
    return jsonify({"message": "Hello World!"})

#if __name__ == '__main__':
#    app.run(debug=False, port=5000)
