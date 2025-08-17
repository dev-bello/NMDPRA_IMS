import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Base configuration class."""
    # SECURITY WARNING: Keep the secret key secret in production!
    SECRET_KEY = os.environ.get("SECRET_KEY")

    # Flask environment
    FLASK_ENV = os.environ.get("FLASK_ENV")
    FLASK_APP = os.environ.get("FLASK_APP")
    FLASK_CONFIG = os.environ.get("FLASK_CONFIG")

    # Common Microsoft Azure AD settings
    MICROSOFT_CLIENT_ID = os.environ.get('MICROSOFT_CLIENT_ID')
    MICROSOFT_CLIENT_SECRET = os.environ.get('MICROSOFT_CLIENT_SECRET')
    MICROSOFT_TENANT_ID = os.environ.get('MICROSOFT_TENANT_ID')
    MICROSOFT_REDIRECT_URI = os.environ.get('MICROSOFT_REDIRECT_URI')
    ALLOWED_EMAIL_DOMAINS = os.environ.get('ALLOWED_EMAIL_DOMAINS')
    MICROSOFT_AUTHORITY = os.environ.get('MICROSOFT_AUTHORITY')
    ADMIN_GROUP_ID = os.environ.get('ADMIN_GROUP_ID')
    ADMIN_EMAILS = [email.strip().lower() for email in 
                    os.environ.get('ADMIN_EMAILS', '').split(',') 
                    if email.strip()]

   # Scheduler settings
    REPORT_CLEANUP_INTERVAL = int(os.environ.get('REPORT_CLEANUP_INTERVAL', 6))

class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = os.environ.get('DEBUG', 'True').lower() == 'true'
    SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI', 'sqlite:///app.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = os.environ.get('SQLALCHEMY_TRACK_MODIFICATIONS', 'True').lower() == 'true'
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')
    DEBUG_TB_INTERCEPT_REDIRECTS = os.environ.get('DEBUG_TB_INTERCEPT_REDIRECTS', 'False').lower() == 'true'
    DEBUG_TB_TEMPLATE_EDITOR_ENABLED = os.environ.get('DEBUG_TB_TEMPLATE_EDITOR_ENABLED', 'True').lower() == 'true'
    WTF_CSRF_ENABLED = os.environ.get('WTF_CSRF_ENABLED', 'True').lower() == 'true'
    SESSION_COOKIE_SECURE = os.environ.get('SESSION_COOKIE_SECURE', 'False').lower() == 'true'
    REMEMBER_COOKIE_SECURE = os.environ.get('REMEMBER_COOKIE_SECURE', 'False').lower() == 'true'

class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = os.environ.get('DEBUG', 'False').lower() == 'true'
    SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = os.environ.get('SQLALCHEMY_TRACK_MODIFICATIONS', 'False').lower() == 'true'
    # Use the CA path from the environment (default to app/ssl/ca.pem)
    MYSQL_CA_PATH = os.environ.get('MYSQL_CA_PATH', 'app/ssl/ca.pem')
    SQLALCHEMY_ENGINE_OPTIONS = {
        'connect_args': {
            'ssl': {
                'ca': MYSQL_CA_PATH
            }
        },
        'pool_size': 10,
        'pool_recycle': 3600
    }
    PREFERRED_URL_SCHEME = os.environ.get('PREFERRED_URL_SCHEME', 'https')
    SESSION_COOKIE_SECURE = os.environ.get('SESSION_COOKIE_SECURE', 'True').lower() == 'true'
    REMEMBER_COOKIE_SECURE = os.environ.get('REMEMBER_COOKIE_SECURE', 'True').lower() == 'true'
    WTF_CSRF_ENABLED = os.environ.get('WTF_CSRF_ENABLED', 'True').lower() == 'true'
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'ERROR')
    CACHE_TYPE = os.environ.get('CACHE_TYPE', 'simple')
    # Add any other production-specific settings here

class TestingConfig(Config):
    """Testing configuration."""
    TESTING = os.environ.get('TESTING', 'True').lower() == 'true'
    DEBUG = os.environ.get('DEBUG', 'True').lower() == 'true'
    SQLALCHEMY_DATABASE_URI = os.environ.get('SQLALCHEMY_DATABASE_URI', 'sqlite:///test.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = os.environ.get('SQLALCHEMY_TRACK_MODIFICATIONS', 'False').lower() == 'true'
    WTF_CSRF_ENABLED = os.environ.get('WTF_CSRF_ENABLED', 'False').lower() == 'true'
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'DEBUG')
    RATELIMIT_ENABLED = os.environ.get('RATELIMIT_ENABLED', 'False').lower() == 'true'
    CACHE_TYPE = os.environ.get('CACHE_TYPE', 'null')

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}