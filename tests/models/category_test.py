# category_test.py

import unittest
from unittest.mock import patch, MagicMock
from app import create_app, db
from app.models.inventory import Category

class TestCategory(unittest.TestCase):

    def setUp(self):
        # Create a test app and push the context
        self.app = create_app('testing')  # Ensure you have a 'testing' config
        self.app_context = self.app.app_context()
        self.app_context.push()

        # Set up a test database or mock the database session
        self.db_session_patcher = patch('app.models.inventory.db.session', autospec=True)
        self.mock_db_session = self.db_session_patcher.start()

        # Mock current_user
        self.current_user_patcher = patch('app.models.inventory.current_user', autospec=True)
        self.mock_current_user = self.current_user_patcher.start()
        self.mock_current_user.is_admin = True
        self.mock_current_user.email = 'admin@example.com'

    def tearDown(self):
        # Stop patching
        self.db_session_patcher.stop()
        self.current_user_patcher.stop()

        # Pop the app context
        self.app_context.pop()

    def test_create_category(self):
        # Test creating a category
        category, error = Category.create_category('Electronics', 'Devices and gadgets')
        self.assertIsNotNone(category)
        self.assertIsNone(error)
        self.mock_db_session.add.assert_called_once()
        self.mock_db_session.commit.assert_called_once()

    def test_create_category_without_admin(self):
        # Test creating a category without admin privileges
        self.mock_current_user.is_admin = False
        category, error = Category.create_category('Electronics', 'Devices and gadgets')
        self.assertIsNone(category)
        self.assertEqual(error, "Permission denied: Admin privileges required")

    def test_update_category(self):
        # Mock a category object
        mock_category = MagicMock()
        mock_category.id = 1
        self.mock_db_session.get.return_value = mock_category

        # Test updating a category
        category, error = Category.update_category(1, name='Updated Electronics')
        self.assertIsNotNone(category)
        self.assertIsNone(error)
        self.assertEqual(category.name, 'Updated Electronics')
        self.mock_db_session.commit.assert_called_once()

    def test_delete_category(self):
        # Mock a category object
        mock_category = MagicMock()
        mock_category.id = 1
        mock_category.inventory_items = []
        
        # Mock the db.session.get method directly
        self.mock_db_session.get.return_value = mock_category

        # Test deleting a category
        success, error = Category.delete_category(1)
        self.assertTrue(success)
        self.assertIsNone(error)
        self.mock_db_session.delete.assert_called_once_with(mock_category)
        self.mock_db_session.commit.assert_called_once()

    def test_delete_category_with_items(self):
        # Mock a category object with inventory items
        mock_category = MagicMock()
        mock_category.id = 1
        mock_category.inventory_items = [MagicMock()]
        self.mock_db_session.get.return_value = mock_category
        # Test deleting a category with items
        success, error = Category.delete_category(1)
        self.assertFalse(success)
        self.assertEqual(error, "Cannot delete category with associated inventory items")

if __name__ == '__main__':
    unittest.main()
