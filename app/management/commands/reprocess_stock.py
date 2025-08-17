import logging
import click
from flask.cli import with_appcontext
from sqlalchemy import text
from app import db
from app.models.inventory import Inventory
from app.models.inventory_transaction import InventoryTransaction

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Updated items list with stock quantities
ITEMS_TO_REPROCESS = [
    {"name": "TP Link", "stock": 30},
    {"name": "APC Surge Protector (PM 50- UK)/Extension Box", "stock": 241},
    {"name": "CD Re-Writable", "stock": 126},
    {"name": "CD Recordable LG", "stock": 44}
]

# Target date for command parameter (transactions will use May 30, 2025 for June report)
TARGET_DATE = '2025-05-30'


def register(app):
    @app.cli.command("reprocess-stock")
    @click.option('--target-date', default=TARGET_DATE, help='Target date for transactions (YYYY-MM-DD)')
    @click.option('--dry-run', is_flag=True, help='Show what would be processed without making changes')
    @with_appcontext
    def reprocess_stock(target_date, dry_run):
        """
        Reprocesses initial stock transactions for a predefined list of inventory items
        with specified stock quantities. Creates initial transactions with date of
        May 30, 2025 to reflect in June 2025 monthly report.
        """
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        logger.info(f"Starting stock reprocessing for date: {target_date}")
        
        processed_count = 0
        skipped_count = 0
        error_count = 0

        for item_data in ITEMS_TO_REPROCESS:
            item_name = item_data["name"]
            stock_quantity = item_data["stock"]
            
            try:
                # Find the inventory item
                inventory_item = Inventory.query.filter(
                    Inventory.item_name.ilike(item_name)
                ).first()

                if not inventory_item:
                    logger.warning(
                        f"Item '{item_name}' not found in inventory. Skipping reprocessing."
                    )
                    skipped_count += 1
                    continue

                logger.info(
                    f"Processing item '{item_name}' (ID: {inventory_item.id}) "
                    f"with stock quantity: {stock_quantity}"
                )

                if dry_run:
                    logger.info(
                        f"[DRY RUN] Would create initial transaction for item ID {inventory_item.id} "
                        f"with quantity {stock_quantity} and date 2025-05-30"
                    )
                    processed_count += 1
                    continue

                # Execute the reprocessing with stock quantity and target date
                with db.session.begin_nested():
                    # Find existing initial transaction for this inventory item
                    existing_transaction = InventoryTransaction.query.filter_by(
                        inventory_id=inventory_item.id,
                        transaction_type='initial'
                    ).first()
                    
                    # Delete existing initial transaction if found
                    if existing_transaction:
                        db.session.delete(existing_transaction)
                    
                    # Calculate the actual current quantity by accounting for all transactions
                    # Start with the initial stock quantity
                    current_quantity = stock_quantity
                    
                    # Get all transactions for this inventory item (excluding the initial transaction we just deleted)
                    transactions = InventoryTransaction.query.filter(
                        InventoryTransaction.inventory_id == inventory_item.id,
                        InventoryTransaction.transaction_type != 'initial'
                    ).all()
                    
                    # Adjust the quantity based on all transactions
                    for transaction in transactions:
                        current_quantity += transaction.quantity
                    
                    # Update the inventory item's quantity to the calculated current quantity
                    inventory_item.quantity = current_quantity
                    
                    # Create new initial transaction with May 30, 2025 date for June report
                    from datetime import datetime
                    transaction_date = datetime(2025, 5, 30)  # May 30, 2025 for June report
                    new_transaction = InventoryTransaction(
                        inventory_id=inventory_item.id,
                        transaction_type='initial',
                        quantity=stock_quantity,  # Use the intial quantity for processing item
                        timestamp=transaction_date,
                        note='Initial stock reprocessed for June 2025 report',
                        performed_by=1  # Admin user ID - you may want to adjust this
                    )
                    db.session.add(new_transaction)
                    
                    logger.info(f"Successfully reprocessed item '{item_name}' with initial quantity {stock_quantity}, final quantity {current_quantity} on date {transaction_date.strftime('%Y-%m-%d')}")
                    processed_count += 1

            except Exception as e:
                logger.error(
                    f"An error occurred while reprocessing item '{item_name}': {str(e)}"
                )
                error_count += 1
                try:
                    db.session.rollback()
                except:
                    pass  # Session might already be rolled back

        # Commit all successful transactions
        if not dry_run and processed_count > 0:
            try:
                db.session.commit()
                logger.info("All transactions committed successfully.")
            except Exception as e:
                logger.error(f"Failed to commit transactions: {str(e)}")
                db.session.rollback()
                error_count += processed_count
                processed_count = 0

        # Summary
        logger.info("=" * 50)
        logger.info("STOCK REPROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Target Date: {target_date}")
        logger.info(f"Total Items: {len(ITEMS_TO_REPROCESS)}")
        logger.info(f"Successfully Processed: {processed_count}")
        logger.info(f"Skipped (Not Found): {skipped_count}")
        logger.info(f"Errors: {error_count}")
        
        if dry_run:
            logger.info("DRY RUN COMPLETED - No actual changes were made")
        else:
            logger.info("Stock reprocessing completed.")

    @app.cli.command("add-stock-item")
    @click.argument('item_name')
    @click.argument('stock_quantity', type=int)
    @click.option('--target-date', default=TARGET_DATE, help='Target date for transaction (YYYY-MM-DD)')
    @click.option('--dry-run', is_flag=True, help='Show what would be processed without making changes')
    @with_appcontext
    def add_single_stock_item(item_name, stock_quantity, target_date, dry_run):
        """
        Process a single inventory item with the specified stock quantity.
        Creates an initial transaction with date of May 30, 2025 to reflect in June 2025 report.
        
        Usage: flask add-stock-item "Item Name" 15
        """
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        
        logger.info(f"Processing single item: '{item_name}' with quantity: {stock_quantity}")
        
        try:
            # Find the inventory item
            inventory_item = Inventory.query.filter(
                Inventory.item_name.ilike(item_name)
            ).first()

            if not inventory_item:
                logger.error(f"Item '{item_name}' not found in inventory.")
                return

            logger.info(
                f"Found item '{item_name}' (ID: {inventory_item.id})"
            )

            if dry_run:
                logger.info(
                    f"[DRY RUN] Would create initial transaction for item ID {inventory_item.id} "
                    f"with quantity {stock_quantity} and date 2025-05-30"
                )
                return

            # Execute the reprocessing
            # Find existing initial transaction for this inventory item
            existing_transaction = InventoryTransaction.query.filter_by(
                inventory_id=inventory_item.id,
                transaction_type='initial'
            ).first()
            
            # Delete existing initial transaction if found
            if existing_transaction:
                db.session.delete(existing_transaction)
            
            # Calculate the actual current quantity by accounting for all transactions
            # Start with the initial stock quantity
            current_quantity = stock_quantity
            
            # Get all transactions for this inventory item (excluding the initial transaction we just deleted)
            transactions = InventoryTransaction.query.filter(
                InventoryTransaction.inventory_id == inventory_item.id,
                InventoryTransaction.transaction_type != 'initial'
            ).all()
            
            # Adjust the quantity based on all transactions
            for transaction in transactions:
                current_quantity += transaction.quantity
            
            # Update the inventory item's quantity to the calculated current quantity
            inventory_item.quantity = current_quantity
            
            # Create new initial transaction with May 30, 2025 date for June report
            from datetime import datetime
            transaction_date = datetime(2025, 5, 30)  # May 30, 2025 for June report
            new_transaction = InventoryTransaction(
                inventory_id=inventory_item.id,
                transaction_type='initial',
                quantity=current_quantity,  # Use the calculated quantity
                timestamp=transaction_date,
                note='Initial stock reprocessed for June 2025 report',
                performed_by=1  # Admin user ID - you may want to adjust this
            )
            db.session.add(new_transaction)
            
            db.session.commit()
            
            logger.info(f"Successfully processed item '{item_name}' with initial quantity {stock_quantity}, final quantity {current_quantity} on date {transaction_date.strftime('%Y-%m-%d')}")

        except Exception as e:
            logger.error(f"An error occurred while processing item '{item_name}': {str(e)}")
            db.session.rollback()

    @app.cli.command("list-stock-items")
    @with_appcontext
    def list_stock_items():
        """
        List all items that would be processed by the reprocess-stock command.
        """
        logger.info("Items configured for stock reprocessing:")
        logger.info("=" * 50)
        
        total_stock = 0
        found_count = 0
        
        for item_data in ITEMS_TO_REPROCESS:
            item_name = item_data["name"]
            stock_quantity = item_data["stock"]
            
            # Check if item exists in inventory
            inventory_item = Inventory.query.filter(
                Inventory.item_name.ilike(item_name)
            ).first()
            
            status = "✓ FOUND" if inventory_item else "✗ NOT FOUND"
            item_id = f"(ID: {inventory_item.id})" if inventory_item else ""
            
            logger.info(f"{status} - {item_name} {item_id} - Stock: {stock_quantity}")
            
            if inventory_item:
                found_count += 1
                total_stock += stock_quantity
        
        logger.info("=" * 50)
        logger.info(f"Summary: {found_count}/{len(ITEMS_TO_REPROCESS)} items found")
        logger.info(f"Total stock to be processed: {total_stock}")