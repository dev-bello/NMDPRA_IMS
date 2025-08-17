from flask import render_template, redirect, url_for, flash, request, current_app
from flask_login import login_user, logout_user, login_required, current_user
from . import auth
from app.models.user import User
from app import db
from app import login_manager
import logging

# Configure logging (for debugging; comment out or set to WARNING in production)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@auth.route('/')
@auth.route('/login')
def login():
    """
    Render the login page with both Microsoft OAuth and local authentication options.
    Local authentication is for testing/development only and should be removed before production deployment.
    """
    if current_user.is_authenticated:
        # logger.info(f"User {current_user.email} is already authenticated, redirecting to dashboard.")
        return redirect_to_appropriate_page()
    
    try:
        # Generate Microsoft OAuth URL
        client = User.get_microsoft_client()
        auth_url = client.get_authorization_request_url(
            scopes=["User.Read"],
            redirect_uri=current_app.config['MICROSOFT_REDIRECT_URI']
        )
        # logger.info("Generated Microsoft OAuth authorization URL successfully.")
        return render_template('auth/login.html', auth_url=auth_url)
    except Exception as e:
        # logger.error(f"Error during Microsoft OAuth initialization: {e}")
        flash('Microsoft login is currently unavailable. Please try local login.', 'error')
        return render_template('auth/login.html', auth_url=None)

@auth.route('/login/local', methods=['POST'])
def local_login():
    """
    Handle local email/password authentication.
    This route is for testing/development only and should be removed before production deployment.
    """
    if current_user.is_authenticated:
        # logger.info(f"User {current_user.email} is already authenticated, redirecting to dashboard.")
        return redirect_to_appropriate_page()
    
    email = request.form.get('email', '').strip().lower()
    password = request.form.get('password', '')
    
    # logger.info(f"Local login attempt for email: {email}")
    
    # Validate input
    if not email or not password:
        # logger.warning("Local login failed: Missing email or password")
        flash('Please provide both email and password.', 'error')
        return redirect(url_for('auth.login'))
    
    try:
        # Attempt local authentication
        user = User.authenticate_local_user(email, password)
        
        if user:
            # logger.info(f"Local authentication successful for user: {user.email}")
            login_user(user)
            flash(f'Welcome back, {user.name}!', 'success')
            return redirect_to_appropriate_page()
        else:
            # logger.warning(f"Local authentication failed for email: {email} - Invalid credentials")
            flash('Invalid email or password. Please try again.', 'error')
            
    except Exception as e:
        # logger.error(f"Error during local authentication for {email}: {e}")
        flash('Authentication failed due to server error. Please try again.', 'error')
    
    return redirect(url_for('auth.login'))

@auth.route('/auth/callback')
def auth_callback():
    """Process the callback from Microsoft OAuth authentication."""
    code = request.args.get('code')
    
    if not code:
        # logger.warning("Microsoft authentication failed - no authorization code received")
        flash('Microsoft authentication failed - no authorization code received.', 'error')
        return redirect(url_for('auth.login'))

    # logger.info("Processing Microsoft OAuth callback")
    
    try:
        user = User.authenticate_microsoft_user(code)
        
        if user:
            # logger.info(f"Microsoft authentication successful for user: {user.email}")
            login_user(user)
            flash(f'Welcome back, {user.name}!', 'success')
            return redirect_to_appropriate_page()
        else:
            # logger.warning("Microsoft authentication failed - could not create or retrieve user")
            flash('Microsoft authentication failed - could not create or retrieve user.', 'error')
    except Exception as e:
        # logger.error(f"Microsoft authentication error: {e}")
        flash('Microsoft authentication failed - server error.', 'error')
    
    return redirect(url_for('auth.login'))

@auth.route('/logout')
@login_required
def logout():
    """Handle user logout and session cleanup."""
    user_email = current_user.email if current_user.is_authenticated else 'Unknown'
    auth_method = getattr(current_user, 'auth_method', 'unknown')
    
    # logger.info(f"User logout: {user_email} (auth_method: {auth_method})")
    
    # Remove user's session
    logout_user()
    
    # Notify user of successful logout
    flash('You have been logged out successfully.', 'info')
    
    # Redirect to login page
    return redirect(url_for('auth.login'))

def redirect_to_appropriate_page():
    """Determine and redirect to the correct dashboard based on user role."""
    if not current_user.is_authenticated:
        # logger.warning("Attempted to redirect unauthenticated user")
        return redirect(url_for('auth.login'))
    
    # Check if user is admin and redirect accordingly
    if current_user.is_admin:
        # logger.info(f"Redirecting admin user {current_user.email} to admin dashboard")
        return redirect(url_for('home.admin_dashboard'))
    
    # Redirect non-admin users to regular dashboard
    # logger.info(f"Redirecting regular user {current_user.email} to user dashboard")
    return redirect(url_for('home.user_dashboard'))