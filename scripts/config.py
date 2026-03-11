"""
Centralized configuration for MySQL database connection.
Update the credentials below to match your MySQL setup.
"""

DB_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "12345",
    "database": "analytics",
}

def get_connection_string():
    """Return a SQLAlchemy connection string for the configured database."""
    return (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

def get_server_connection_string():
    """Return a SQLAlchemy connection string WITHOUT a specific database (for CREATE DATABASE)."""
    return (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
    )
