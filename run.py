import os
import click
from dotenv import load_dotenv
from app import create_app, db
from app.models.report_cache import ReportCache
from flask.cli import with_appcontext
from flask_migrate import Migrate
# Load environment variables from .env file
load_dotenv()


# Get configuration from environment variable, default to 'development' if not set
config_name = os.getenv('FLASK_CONFIG', 'default')
app = create_app(config_name)
migrate = Migrate(app, db)


if __name__ == '__main__':
    debug = config_name == 'development'
    app.run(host='0.0.0.0', 
            port=int(os.environ.get('PORT', 8000)),
            debug=debug)