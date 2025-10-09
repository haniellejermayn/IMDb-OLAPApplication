# Main Flask application

from flask import Flask, jsonify
from flask_cors import CORS
from routes.olap import olap_bp
from routes.stats import stats_bp

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Register blueprints
app.register_blueprint(olap_bp, url_prefix='/api/olap')
app.register_blueprint(stats_bp, url_prefix='/api/stats')

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "service": "IMDb OLAP API"
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)