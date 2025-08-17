from flask import render_template, redirect, url_for, flash, current_app
from flask_login import login_required, current_user
from . import home

@home.route('/admin/dashboard')
@login_required
def admin_dashboard():
    """
    Admin dashboard view.
    Requires: User authentication and admin privileges
    Returns: Admin dashboard template or redirects to user dashboard
    """
    # Check if user has admin privileges
    if not current_user.is_admin:
        # Redirect non-admin users to regular dashboard
        flash('You do not have permission to access the admin dashboard.', 'error')
        return redirect(url_for('home.user_dashboard'))
    
    # Render admin dashboard for authorized users
    return render_template('home/admin_dashboard.html')

@home.route('/dashboard')
@login_required
def user_dashboard():
    """
    User dashboard view.
    Requires: User authentication
    Returns: User dashboard template
    """
    return render_template('home/user_dashboard.html')