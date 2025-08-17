from app import db, login_manager
from flask_login import UserMixin
from msal import ConfidentialClientApplication
import requests
from datetime import datetime, UTC
from flask import current_app
import logging
import os
from werkzeug.security import generate_password_hash, check_password_hash

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

class User(db.Model, UserMixin):
    """User model for storing user account information and authentication details."""
    __tablename__ = 'users'

    # Database columns
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    email = db.Column(db.String(120), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    azure_id = db.Column(db.String(100), unique=True, nullable=True)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())
    last_login = db.Column(db.DateTime, default=db.func.current_timestamp())
    
    # Added new user profile fields
    job_title = db.Column(db.String(100), nullable=True)
    company_name = db.Column(db.String(100), nullable=True)
    department = db.Column(db.String(100), nullable=True)
    office_location = db.Column(db.String(100), nullable=True)
    
    # Added fields for local authentication
    password_hash = db.Column(db.String(255), nullable=True)
    auth_method = db.Column(db.String(20), default='microsoft')  # 'microsoft' or 'local'



    @staticmethod
    def get_microsoft_client():
        """Initialize and return Microsoft MSAL client instance."""
        return ConfidentialClientApplication(
            client_id=current_app.config['MICROSOFT_CLIENT_ID'],
            client_credential=current_app.config['MICROSOFT_CLIENT_SECRET'],
            authority=current_app.config['MICROSOFT_AUTHORITY']
        )
    
    @classmethod
    def authenticate_microsoft_user(cls, auth_code):
        """Authenticate user with Microsoft and create/update user record."""
        client = cls.get_microsoft_client()
        try:
            token_result = client.acquire_token_by_authorization_code(
                code=auth_code,
                scopes=["User.Read"],
                redirect_uri=current_app.config['MICROSOFT_REDIRECT_URI']
            )

            if 'error' in token_result or 'access_token' not in token_result:
                return None

            graph_data = cls.get_user_info(token_result['access_token'])
            if not graph_data:
                return None

            email = graph_data.get('mail', '').lower()
            user = cls.query.filter_by(email=email).first()

            if not user:
                user = cls.create_user(graph_data, None)  # Don't store token in DB
            else:
                user.update_login()  # Remove token parameter
                user.update_profile_info(graph_data)

            # Store access token in session if needed, but don't return a response object
            # The controller should handle response creation
            
            return user  # Return the user object directly

        except Exception as e:
            return None

    @staticmethod
    def get_user_info(access_token):
        """Retrieve user information from Microsoft Graph API."""
        headers = {'Authorization': f'Bearer {access_token}'}
        # Request user profile information including job title, department, etc.
        response = requests.get(
            'https://graph.microsoft.com/v1.0/me?$select=id,displayName,userPrincipalName,mail,jobTitle,department,companyName,officeLocation',
            headers=headers
        )
        
        if response.status_code != 200:
            return None

        user_data = response.json()
        upn = user_data.get('userPrincipalName')
        
        if not upn:
            return None

        # Handle external (guest) users
        if '#EXT#' in upn:
            original_part = upn.split('#EXT#')[0]
            domain = original_part.split('_')[-1]
            username = '_'.join(original_part.split('_')[:-1])
            email = f"{username}@{domain}"
        else:
            email = upn

        user_data['mail'] = email.lower()
        return user_data

    @classmethod
    def create_user(cls, graph_data, token_result):
        """Create new user from Microsoft Graph API data."""
        try:
            # Log the current configuration
            # logger.info(f"Current app config: {current_app.config}")

            # Access the ADMIN_EMAILS property correctly
            admin_emails = current_app.config['ADMIN_EMAILS']
            # logger.info(f"Admin emails: {admin_emails}")

            user = cls(
                email=graph_data.get('mail', '').lower(),
                name=graph_data.get('displayName', ''),
                azure_id=graph_data.get('id'),
                is_admin=graph_data.get('mail', '').lower() in admin_emails,
                auth_method='microsoft',
                job_title=graph_data.get('jobTitle', ''),
                company_name=graph_data.get('companyName', ''),
                department=graph_data.get('department', ''),
                office_location=graph_data.get('officeLocation', '')
            )
            db.session.add(user)
            db.session.commit()
            return user
        except Exception as e:
            # logger.error(f"Error creating user: {e}")
            db.session.rollback()
            return None

    def update_profile_info(self, graph_data):
        """Update user's profile information from Microsoft Graph API data."""
        try:
            self.job_title = graph_data.get('jobTitle', '')
            self.company_name = graph_data.get('companyName', '')
            self.department = graph_data.get('department', '')
            self.office_location = graph_data.get('officeLocation', '')
            db.session.commit()
            return True
        except Exception:
            db.session.rollback()
            return False

    def update_login(self):
        """Update user's last login timestamp."""
        try:
            self.last_login = datetime.now(UTC)
            db.session.commit()
            return True
        except Exception:
            db.session.rollback()
            return False

    @classmethod
    def update_admin_status(cls):
        """Update admin status for all users based on configuration."""
        try:
            admin_emails = current_app.config.get('ADMIN_EMAILS', [])
            users = cls.query.all()
            
            for user in users:
                user.is_admin = user.email.lower() in [email.lower() for email in admin_emails]
            
            db.session.commit()
            return True
        except Exception:
            db.session.rollback()
            return False

    @property
    def is_authenticated(self):
        """Check if user is authenticated."""
        return True

    @property
    def is_active(self):
        """Check if user is active."""
        return True

    @property
    def is_anonymous(self):
        """Check if user is anonymous."""
        return False

    def get_id(self):
        """Get user ID as string."""
        return str(self.id)

    # Local authentication methods
    def set_password(self, password):
        """Set password hash for local authentication."""
        self.password_hash = generate_password_hash(password)
        # Don't override auth_method if it's already set
        if not self.auth_method:
            self.auth_method = 'local'
    
    def check_password(self, password):
        """Check if provided password matches the stored hash."""
        if self.password_hash:
            return check_password_hash(self.password_hash, password)
        return False
    
    @classmethod
    def create_local_user(cls, email, name, password, is_admin=False, **kwargs):
        """Create a new user with local authentication."""
        import logging
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        try:
            # Check if user already exists
            existing_user = cls.query.filter_by(email=email.lower()).first()
            if existing_user:
                logger.debug(f"User with email {email} already exists. is_admin={existing_user.is_admin}")
                print(f"[DEBUG] User with email {email} already exists. is_admin={existing_user.is_admin}")
                return None, "User with this email already exists"
            
            user = cls(
                email=email.lower(),
                name=name,
                is_admin=is_admin,
                auth_method='local',
                job_title=kwargs.get('job_title'),
                company_name=kwargs.get('company_name'),
                department=kwargs.get('department'),
                office_location=kwargs.get('office_location')
            )
            logger.debug(f"[create_local_user] After creation: {user.email} is_admin={user.is_admin}")
            print(f"[DEBUG] [create_local_user] After creation: {user.email} is_admin={user.is_admin}")
            user.set_password(password)
            logger.debug(f"[create_local_user] After set_password: {user.email} is_admin={user.is_admin}")
            print(f"[DEBUG] [create_local_user] After set_password: {user.email} is_admin={user.is_admin}")
            db.session.add(user)
            db.session.commit()
            logger.debug(f"[create_local_user] After commit: {user.email} is_admin={user.is_admin}")
            print(f"[DEBUG] [create_local_user] After commit: {user.email} is_admin={user.is_admin}")
            return user, None
        except Exception as e:
            db.session.rollback()
            logger.error(f"[create_local_user] Exception: {e}")
            print(f"[DEBUG] [create_local_user] Exception: {e}")
            return None, f"Error creating user: {str(e)}"
    
    @classmethod
    def authenticate_local_user(cls, email, password):
        """Authenticate user with email and password."""
        user = cls.query.filter_by(email=email.lower(), auth_method='local').first()
        if user and user.check_password(password):
            user.update_login()
            return user
        return None

    def to_dict(self):
        """Convert user object to dictionary representation."""
        return {
            'id': self.id,
            'email': self.email,
            'name': self.name,
            'is_admin': self.is_admin,
            'azure_id': self.azure_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_login': self.last_login.isoformat() if self.last_login else None,
            'job_title': self.job_title,
            'company_name': self.company_name,
            'department': self.department,
            'office_location': self.office_location,
            'auth_method': self.auth_method
        }

    def __repr__(self):
        """String representation of User object."""
        return f'<User {self.email}>'
