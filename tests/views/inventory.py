# tests/inventory/inventory_views_test.py

import unittest
from flask import url_for
from app import create_app, db
from app.models.inventory import Inventory, Category
from unittest.mock import patch, MagicMock

class TestInventoryViews(unittest.TestCase):

    def setUp(self):
        # Create a test app and push the context
        self.app = create_app('testing')
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.client = self.app.test_client()

        # Set up a test database or mock the database session
        self.db_session_patcher = patch('app.models.inventory.db.session', autospec=True)
        self.mock_db_session = self.db_session_patcher.start()

    def tearDown(self):
        # Stop patching
        self.db_session_patcher.stop()

        # Pop the app context
        self.app_context.pop()

    def test_inventory_list(self):
        # Mock the query to return a list of inventory items
        mock_inventory = MagicMock()
        mock_inventory.id = 1
        mock_inventory.item_name = 'Laptop'
        mock_inventory.quantity = 10
        self.mock_db_session.query().all.return_value = [mock_inventory]

        # Send a GET request to the inventory list route
        response = self.client.get(url_for('inventory.inventory_list'))

        # Check the response
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Laptop', response.data)
        self.assertIn(b'10', response.data)

if __name__ == '__main__':
    unittest.main()