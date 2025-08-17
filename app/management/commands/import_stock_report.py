import csv
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import click
from flask.cli import with_appcontext
from app import db
from app.models.inventory import Inventory, Category
from app.models.inventory_transaction import InventoryTransaction
from app.models.request import Request, RequestItem, DirectorateEnum, RequestStatus, ItemRequestStatus
from app.models.user import User

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_existing_data(category_names):
    """Deletes all inventory data associated with a list of category names."""
    if not category_names:
        logger.info("No categories specified for clearing.")
        return

    logger.info(f"Clearing existing data for categories: {', '.join(category_names)}")
    try:
        # Find all categories matching the names
        categories = Category.query.filter(Category.name.in_(category_names)).all()
        if not categories:
            logger.info("No existing categories found for clearing.")
            return

        category_ids = [c.id for c in categories]

        # Find all inventory items in these categories
        inventory_items = Inventory.query.filter(Inventory.category_id.in_(category_ids)).all()
        if not inventory_items:
            logger.info("No existing inventory items to clear for the specified categories.")
            return

        inventory_ids = [item.id for item in inventory_items]

        # Find and delete related transactions first
        transactions_to_delete = InventoryTransaction.query.filter(InventoryTransaction.inventory_id.in_(inventory_ids)).all()
        for trans in transactions_to_delete:
            db.session.delete(trans)
        logger.info(f"Deleted {len(transactions_to_delete)} related transaction(s).")

        # Find and delete related requests. The cascade option on the model will handle RequestItems.
        requests_to_delete = Request.query.join(RequestItem).filter(RequestItem.inventory_id.in_(inventory_ids)).all()
        for req in requests_to_delete:
            db.session.delete(req)
        logger.info(f"Deleted {len(requests_to_delete)} related request(s).")

        # Finally, delete the inventory items themselves
        for item in inventory_items:
            db.session.delete(item)
        logger.info(f"Deleted {len(inventory_items)} inventory item(s).")

        db.session.commit()
        logger.info("Data clearing complete for specified categories.")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Error clearing existing data: {e}")
        raise

def register(app):
    @app.cli.command('import_stock_report')
    @click.argument('filepath')
    @click.option('--clear', is_flag=True, help='Clear all existing data for categories found in the CSV before importing.')
    @with_appcontext
    def import_stock_report(filepath, clear):
        """
        Imports stock data from a CSV file, dynamically handling categories.
        """
        try:
            # --- 1. Pre-flight checks ---
            admin_user = User.query.filter_by(is_admin=True).first()
            if not admin_user:
                logger.error("No admin user found. Please create one.")
                return

            requester_user = User.query.filter_by(is_admin=False).first()
            if not requester_user:
                logger.error("No non-admin user found to act as requester. Please create one.")
                return

            # --- 2. Pre-read CSV to get categories and clear data if requested ---
            if clear:
                with open(filepath, mode='r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    categories_in_csv = {row['Category'].strip() for row in reader if row.get('Category')}
                    if categories_in_csv:
                        clear_existing_data(list(categories_in_csv))
                    else:
                        logger.warning("No 'Category' column found or it is empty. Skipping data clearing.")

            # --- 3. Process the CSV file ---
            with open(filepath, mode='r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row_num, row in enumerate(reader, start=2):
                    try:
                        with db.session.begin_nested():
                            # --- Data Validation and Parsing ---
                            item_name_raw = row.get('Item Name')
                            if not item_name_raw or not item_name_raw.strip():
                                logger.warning(f"Skipping row {row_num}: 'Item Name' is missing. Row data: {row}")
                                continue
                            
                            item_name = item_name_raw.strip()

                            category_name = row.get('Category', '').strip()
                            if not category_name:
                                logger.warning(f"Skipping row {row_num}: 'Category' is missing. Row data: {row}")
                                continue

                            # Use the report start date for all timestamps
                            report_date_str = row.get('Report Start Date')
                            if not report_date_str:
                                logger.warning(f"Skipping row {row_num}: 'Report Start Date' is missing. Row data: {row}")
                                continue
                            
                            import_date = datetime.strptime(report_date_str, '%Y-%m-%d')

                            # Filter for June data
                            if import_date.month != 6:
                                continue

                            # --- Get or Create Category ---
                            category = Category.query.filter(Category.name.ilike(category_name)).first()
                            if not category:
                                category = Category(name=category_name)
                                db.session.add(category)
                                db.session.flush() # Flush to get the new category's ID
                                logger.info(f"Created new category: '{category_name}'")

                            # --- Get or Create Inventory Item ---
                            inventory_item = Inventory.query.filter(Inventory.item_name.ilike(item_name)).first()
                            if inventory_item:
                                # Update existing item
                                inventory_item.description = row.get('DESCRIPTION')
                                inventory_item.quantity = int(float(row.get('Closing Stock', 0)))
                                inventory_item.unit_price = Decimal(row.get('Unit Price', '0.0'))
                                inventory_item.category_id = category.id
                                inventory_item.updated_by = admin_user.id
                                inventory_item.updated_at = import_date
                                logger.info(f"Updated existing inventory item: '{item_name}'")
                            else:
                                # Create new item
                                inventory_item = Inventory(
                                    item_name=item_name,
                                    description=row.get('DESCRIPTION'),
                                    quantity=int(float(row.get('Closing Stock', 0))),
                                    unit_price=Decimal(row.get('Unit Price', '0.0')),
                                    category_id=category.id,
                                    location='Headquarters',
                                    created_by=admin_user.id,
                                    updated_by=admin_user.id,
                                    created_at=import_date,
                                    updated_at=import_date
                                )
                                db.session.add(inventory_item)
                                logger.info(f"Created new inventory item: '{item_name}'")
                            
                            db.session.flush() # Flush to get the item's ID

                            # --- Create Transactions ---
                            unit_price_from_csv = Decimal(row.get('Unit Price', '0.0'))

                            opening_stock = int(float(row.get('Opening Stock', 0)))
                            if opening_stock > 0:
                                db.session.add(InventoryTransaction(
                                    inventory_id=inventory_item.id, transaction_type='initial',
                                    quantity=opening_stock, performed_by=admin_user.id,
                                    timestamp=import_date.replace(hour=0, minute=0, second=0), note='Initial stock from June 2025 report.',
                                    unit_price=unit_price_from_csv
                                ))

                            purchases = int(float(row.get('Purchases', 0)))
                            if purchases > 0:
                                db.session.add(InventoryTransaction(
                                    inventory_id=inventory_item.id, transaction_type='purchase',
                                    quantity=purchases, performed_by=admin_user.id,
                                    timestamp=import_date.replace(hour=12, minute=0, second=0), note='Purchases from June 2025 report.',
                                    unit_price=unit_price_from_csv
                                ))

                            issued = int(float(row.get('Issued', 0)))
                            if issued > 0:
                                # Create a Request for the issued items
                                issue_request = Request(
                                    user_id=requester_user.id,
                                    location='Headquarters',
                                    directorate=DirectorateEnum.ACE,
                                    unit='ACE',
                                    status=RequestStatus.COLLECTED, # Mark as collected since it's historical
                                    created_at=import_date,
                                    updated_at=import_date,
                                    reference_number=f"REQ-IMPORT-{inventory_item.id}-{row_num}"
                                )
                                db.session.add(issue_request)
                                db.session.flush()

                                # Create the RequestItem
                                db.session.add(RequestItem(
                                    request_id=issue_request.id, inventory_id=inventory_item.id,
                                    quantity=issued, quantity_approved=issued,
                                    status=ItemRequestStatus.COLLECTED
                                ))

                                # Create the 'issue' transaction and link it to the request
                                db.session.add(InventoryTransaction(
                                    inventory_id=inventory_item.id, transaction_type='issue',
                                    quantity=-issued, performed_by=admin_user.id,
                                    timestamp=import_date, note='Issued stock from June 2025 report.',
                                    related_request_id=issue_request.id
                                ))

                    except (ValueError, TypeError, InvalidOperation) as e:
                        logger.error(f"Skipping row {row_num} due to data error: {e}. Data: {row}")
                        continue
                    except Exception as e:
                        logger.error(f"An unexpected error occurred at row {row_num}: {e}. Data: {row}")
                        continue
                
                db.session.commit()
                logger.info("Stock report import completed successfully.")

        except FileNotFoundError:
            logger.error(f"Error: The file at path '{filepath}' was not found.")
        except Exception as e:
            db.session.rollback()
            logger.error(f"An error occurred during the import process: {e}")