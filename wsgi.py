"""
wsgi.py
-------
WSGI entry point for production servers (Gunicorn / Nginx / AWS EC2).
"""

from app import app

if __name__ == "__main__":
    app.run()
