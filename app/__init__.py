from flask import Flask, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from config import config
from flask_migrate import Migrate
import os
from dotenv import load_dotenv
import logging
# from flask_debugtoolbar import DebugToolbarExtension
from sqlalchemy import MetaData
from sqlalchemy import event
from sqlalchemy.engine import Engine
import sqlite3
# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask extensions
naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}
db = SQLAlchemy(metadata=MetaData(naming_convention=naming_convention))
migrate = Migrate()
login_manager = LoginManager()
# Load environment variables from .env file
load_dotenv()

def create_app(config_name='default'):
    """
    Factory function to create and configure the Flask application.
    Args:
        config_name (str): Configuration environment name (default: 'default')
    Returns:
        Flask application instance
    """
    # Create Flask app instance and load configuration
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    try:
        # Initialize Flask extensions with app context
        db.init_app(app)
        migrate.init_app(app, db)
        with app.app_context():
            # Create all database tables if they don't exist
            db.create_all()
            logger.info('Database tables created successfully.')
    except Exception as e:
        # Log any database initialization errors
        logger.error(f'Database initialization error: {e}')

    # Configure login manager
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'

    # Import models to ensure SQLAlchemy registers them
    from app import models

    # Setup database and initialize tables
    with app.app_context():
        try:
            # Update admin status for existing users
            from app.models.user import User
            User.update_admin_status()
        except Exception as e:
            # Log any database initialization errors
            logger.error(f'Database initialization error: {e}')

    # Register route for root URL
    @app.route('/')
    def index():
        """Redirect root URL to login page"""
        return redirect(url_for('auth.login'))

    # Register blueprints for different app modules
    from app.auth import auth as auth_blueprint
    from app.home import home as home_blueprint
    from app.inventory import inventory as inventory_blueprint
    from app.request import request as request_blueprint
    from app.purchases import purchases as purchases_blueprint
    from app.report import reports as reports_blueprint

    app.register_blueprint(reports_blueprint)
    app.register_blueprint(auth_blueprint)
    app.register_blueprint(home_blueprint)
    app.register_blueprint(inventory_blueprint)
    app.register_blueprint(request_blueprint)
    app.register_blueprint(purchases_blueprint)

    # Register custom CLI commands
    from app.management.commands import import_stock_report, clean_reports, reprocess_stock
    import_stock_report.register(app)
    clean_reports.register(app)
    reprocess_stock.register(app)
 
 
    # print("Registered endpoints:")
    # for rule in app.url_map.iter_rules():
    #     print(rule.endpoint, rule)

    # if app.config.get("DEBUG", False):
    #     app.config['SECRET_KEY'] = app.config.get('SECRET_KEY', 'dev')
    #     app.config['DEBUG_TB_INTERCEPT_REDIRECTS'] = False
    #     DebugToolbarExtension(app)

    return app

@login_manager.user_loader
def load_user(user_id):
    """
    Flask-Login user loader callback.
    Args:
        user_id: User ID to load
    Returns:
        User object or None
    """
    from app.models.user import User
    return User.query.get(int(user_id))

@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, sqlite3.Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()