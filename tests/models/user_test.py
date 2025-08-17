import pytest
from unittest.mock import patch, MagicMock
from app.models.user import User
from flask import current_app
import pytest
from dotenv import load_dotenv
import os
import logging
from datetime import datetime


def pytest_configure(config):
    """Load environment variables before running tests"""
    load_dotenv()

# filepath: nmdpra_ims/tests/user_test.py

@pytest.fixture

def app_context():
    """Create a real Flask application context for testing"""
    from app import create_app
    app = create_app('testing')
    
    # Set configuration values for testing
    app.config['ADMIN_EMAILS'] = 'admin@example.com,admin2@example.com'
    
    # Create and enter an application context
    with app.app_context():
        yield app




@pytest.fixture
def mock_db_session(mocker):
    """Mock database session"""
    session = MagicMock()
    mocker.patch('app.db.session', session)
    return session


def test_get_user_info_regular_user(mocker, caplog):
    """Test getting info for regular organizational user"""
    caplog.set_level(logging.DEBUG)
    
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'userPrincipalName': 'john.doe@organization.com',
        'displayName': 'John Doe',
        'id': '12345'
    }

    mocker.patch('requests.get', return_value=mock_response)
    
    result = User.get_user_info('fake_token')
    
    # Log the results
    logging.info(f"Test result: {result}")
    
    assert result['mail'] == 'john.doe@organization.com'
    # print(f"Mail assertion passed: {result['mail']}")
    
    assert result['displayName'] == 'John Doe'
    # print(f"Display name assertion passed: {result['displayName']}")

def test_get_user_info_guest_user(mocker):
    """Test getting info for guest user"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'userPrincipalName': 'john_doe_yahoo.com#EXT#@contoso.onmicrosoft.com',
        'displayName': 'John Doe',
        'id': '12345'
    }
    
    mocker.patch('requests.get', return_value=mock_response)
    
    result = User.get_user_info('fake_token')
    
    assert result['mail'] == 'john_doe@yahoo.com'

    print(f"Mail assertion passed: {result['mail']}")

def test_get_user_info_invalid_token(mocker, app_context):
    """Test getting info with invalid token"""
    mock_response = MagicMock()
    mock_response.status_code = 401
    
    mocker.patch('requests.get', return_value=mock_response)
    
    result = User.get_user_info('invalid_token')
    
    # Verify logger was called with error message
    # app_context.logger.error.assert_called_once_with("Failed to get user info from Graph API")
    
    # Verify the result
    assert result is None
    print("Result assertion passed: None expected")
    print("Invalid token test passed")
    print("Invalid token test completed - response code: 401")
    print(result)
    
    # Log the test execution
    logging.info("Invalid token test completed - response code: 401")


def test_create_user_success(app_context, mock_db_session):
    """Test successful user creation"""
    graph_data = {
        'mail': 'john.doe@example.com',
        'displayName': 'John Doe',
        'id': '12345'
    }
    token_result = {
        'access_token': 'fake_token'
    }
    
    user = User.create_user(graph_data, token_result)
    
    assert user is not None
    assert user.email == 'john.doe@example.com'
    assert user.name == 'John Doe'
    assert user.azure_id == '12345'
    assert not user.is_admin
    mock_db_session.add.assert_called_once()
    mock_db_session.commit.assert_called_once()

    print("User creation test passed")
    print(f"User email: {user.email}")
    print(f"User name: {user.name}")
    print(f"User azure_id: {user.azure_id}")
    print(f"User is_admin: {user.is_admin}")

def test_create_admin_user(app_context, mock_db_session):
    """Test creating an admin user"""
    graph_data = {
        'mail': 'admin@example.com',
        'displayName': 'Admin User',
        'id': '12345'
    }
    token_result = {
        'access_token': 'fake_token'
    }
    user = User.create_user(graph_data, token_result)
    
    assert user is not None
    assert user.is_admin is True
    

    print("Starting admin user creation test")
    print(f"Graph data: {graph_data}")
    print(f"Token result: {token_result}")
    print(f"Admin emails: {app_context.config['ADMIN_EMAILS']}")
    print(f"User email: {user.email}")
    print(f"User is_admin: {user.is_admin}")

def test_create_user_db_error(app_context, mock_db_session):
    """Test handling database error during user creation"""
    mock_db_session.commit.side_effect = Exception('Database error')
    
    graph_data = {
        'mail': 'john.doe@example.com',
        'displayName': 'John Doe',
        'id': '12345'
    }
    token_result = {
        'access_token': 'fake_token'
    }
    
    user = User.create_user(graph_data, token_result)
    
    assert user is None

    # print("User creation test with DB error passed")
    # print("Database error test passed") 
    # print("Database error test completed")
    # print("Database error test completed - exception raised")
    # print("Database error test completed - exception raised - user is None")
    # print(user)

def test_update_login(app_context, mock_db_session):
    """Test updating user's login time and access token"""
    # Create a test user
    user = User(
        email='test@example.com',
        name='Test User',
        azure_id='12345',
        is_admin=False
    )
    
    # Test successful update
    result = user.update_login('new_token')
    
    assert result is True
    assert user.access_token == 'new_token'
    mock_db_session.commit.assert_called_once()
    
    # Test database error
    mock_db_session.commit.side_effect = Exception('Database error')
    result = user.update_login('another_token')
    assert result is False
    mock_db_session.rollback.assert_called_once()

def test_get_id():
    """Test get_id method returns string representation of user ID"""
    user = User(
        id=123,
        email='test@example.com',
        name='Test User',
        azure_id='12345'
    )
    
    assert user.get_id() == '123'
    assert isinstance(user.get_id(), str)


def test_to_dict(app_context):
    """Test converting user object to dictionary"""
    # Create a test user with specific datetime values
    test_date = datetime(2024, 3, 29, 12, 0, 0)
    user = User(
        id=123,
        email='test@example.com',
        name='Test User',
        azure_id='12345',
        is_admin=False,
        created_at=test_date,
        last_login=test_date
    )
    
    user_dict = user.to_dict()
    
    assert isinstance(user_dict, dict)
    assert user_dict['id'] == 123
    assert user_dict['email'] == 'test@example.com'
    assert user_dict['name'] == 'Test User'
    assert user_dict['azure_id'] == '12345'
    assert user_dict['is_admin'] is False
    assert user_dict['created_at'] == test_date.isoformat()
    assert user_dict['last_login'] == test_date.isoformat()

    # Test with None values
    user.created_at = None
    user.last_login = None
    user_dict = user.to_dict()
    assert user_dict['created_at'] is None
    assert user_dict['last_login'] is None