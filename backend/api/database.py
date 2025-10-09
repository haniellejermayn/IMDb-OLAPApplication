# Database connection helpers

import mysql.connector
from flask import g, current_app

def get_db():
    """Get database connection for current request"""
    if 'db' not in g:
        g.db = mysql.connector.connect(**current_app.config['DB_CONFIG'])
    return g.db

def get_cursor(dictionary=True):
    """Get cursor from connection"""
    db = get_db()
    return db.cursor(dictionary=dictionary)

def execute_query(query, params=None, fetch_one=False):
    """Helper to execute query and return results"""
    cursor = get_cursor()
    cursor.execute(query, params or ())
    
    if fetch_one:
        result = cursor.fetchone()
    else:
        result = cursor.fetchall()
    
    cursor.close()
    return result

def close_db(e=None):
    """Close database connection"""
    db = g.pop('db', None)
    if db is not None:
        db.close()

def init_db(app):
    """Initialize database with app"""
    app.teardown_appcontext(close_db)