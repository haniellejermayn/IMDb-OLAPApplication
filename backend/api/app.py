# Main Flask application

from flask import Flask, jsonify
from flask_cors import CORS
from routes.reports import reports_bp
from flask import Flask, send_from_directory
from config import DB_CONFIG
import os
import config

# =========== SET UP ============ #

# Base Root Directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

app = Flask(
    __name__,
    static_folder=os.path.join(BASE_DIR, 'frontend', 'public'),
    template_folder=os.path.join(BASE_DIR, 'frontend', 'pages')
)
CORS(app)  # Enable CORS for frontend
app.config['DB_CONFIG'] = config.DB_CONFIG

# Load database configuration
app.config['DB_CONFIG'] = DB_CONFIG

# Register blueprints
app.register_blueprint(reports_bp, url_prefix='/api/reports')

# =========== APIs ============ #

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

# =========== PAGES ============ #

@app.route('/')
def base_page():
    return send_from_directory(os.path.join(BASE_DIR, 'frontend', 'pages'), 'main_page.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)