import unittest
from unittest.mock import patch, MagicMock
from app import create_app, db
from app.models.inventory import Inventory, Category
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]  # Ensure logs are printed to console
)

class TestInventory(unittest.TestCase):

    def setUp(self):
        logging.info("Setting up the test environment.")
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
        self.mock_current_user.id = 1  # Add this line to set the id attribute
        logging.info("Test environment set up complete.")

    def tearDown(self):
        logging.info("Tearing down the test environment.")
        # Stop patching
        self.db_session_patcher.stop()
        self.current_user_patcher.stop()

        # Pop the app context
        self.app_context.pop()
        logging.info("Test environment torn down.")

    def test_create_inventory(self):
        logging.info("Starting test_create_inventory.")
        # Mock a category object
        mock_category = MagicMock()
        mock_category.id = 1  # Ensure the id attribute is set
        mock_category.name = 'Electronics'  # Add any other necessary attributes
        self.mock_db_session.get.return_value = mock_category

        # Test creating an inventory item
        inventory, error = Inventory.create_inventory(
            item_name='Laptop',
            category_id=1,
            quantity=10,
            description='A high-end laptop',
            unit_price=1500.00,
            location='Warehouse'
        )
        logging.info(f"Inventory created: {inventory}, Error: {error}")
        logging.info(f"Inventory created with name: {inventory.item_name}, Error: {error}")
        logging.info(f"Inventory with quantity: {inventory.quantity}, Error: {error}")
        logging.info(f"Inventory with description: {inventory.description}, Error: {error}")
        logging.info(f"Inventory with unit price: {inventory.unit_price}, Error: {error}")
        self.assertIsNotNone(inventory)
        self.assertIsNone(error)
        self.mock_db_session.add.assert_called_once()
        self.mock_db_session.commit.assert_called_once()
        logging.info("test_create_inventory completed successfully.")

    def test_create_inventory_without_admin(self):
        logging.info("Starting test_create_inventory_without_admin.")
        # Test creating an inventory item without admin privileges
        self.mock_current_user.is_admin = False
        inventory, error = Inventory.create_inventory(
            item_name='Laptop',
            category_id=1,
            quantity=10
        )
        logging.info(f"Inventory created: {inventory}, Error: {error}")
        self.assertIsNone(inventory)
        self.assertEqual(error, "Permission denied: Admin privileges required")
        logging.info("test_create_inventory_without_admin completed successfully.")

    def test_update_inventory(self):
        logging.info("Starting test_update_inventory.")
        # Mock an inventory object
        mock_inventory = MagicMock()
        mock_inventory.id = 1
        self.mock_db_session.get.return_value = mock_inventory

        # Test updating an inventory item
        inventory, error = Inventory.update_inventory(
            inventory_id=1,
            item_name='Updated Laptop',
            quantity=5
        )
        logging.info(f"Inventory updated: {inventory}, Error: {error}")
        self.assertIsNotNone(inventory)
        self.assertIsNone(error)
        self.assertEqual(inventory.item_name, 'Updated Laptop')
        self.mock_db_session.commit.assert_called_once()
        logging.info("test_update_inventory completed successfully.")

    def test_delete_inventory(self):
        logging.info("Starting test_delete_inventory.")
        # Mock an inventory object
        mock_inventory = MagicMock()
        mock_inventory.id = 1
        self.mock_db_session.get.return_value = mock_inventory

        # Test deleting an inventory item
        success, error = Inventory.delete_inventory(1)
        logging.info(f"Delete operation success: {success}, Error: {error}")
        self.assertTrue(success)
        self.assertIsNone(error)
        self.mock_db_session.delete.assert_called_once_with(mock_inventory)
        self.mock_db_session.commit.assert_called_once()
        logging.info("test_delete_inventory completed successfully.")

    def test_delete_inventory_not_found(self):
        logging.info("Starting test_delete_inventory_not_found.")
        # Test deleting an inventory item that does not exist
        self.mock_db_session.get.return_value = None
        success, error = Inventory.delete_inventory(1)
        logging.info(f"Delete operation success: {success}, Error: {error}")
        self.assertFalse(success)
        self.assertEqual(error, "Inventory item not found")
        logging.info("test_delete_inventory_not_found completed successfully.")

if __name__ == '__main__':
    unittest.main()
