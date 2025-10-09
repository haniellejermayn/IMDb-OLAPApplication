# OLAP operation endpoints

# This should handle all OLAP cube operations like slice, dice, roll-up, drill-down, pivot
# Can also include endpoints that list dimensions (e.g., genres, years)

from flask import Blueprint, request, jsonify
from database import execute_query

olap_bp = Blueprint('olap', __name__)