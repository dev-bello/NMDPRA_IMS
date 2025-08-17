from flask import render_template, request, redirect, url_for, flash
from app.models.inventory import Inventory, Category
from app.models.inventory_transaction import InventoryTransaction
from app.models.inventory_supplier import InventorySupplier
from app.models.user import User
from app.models.request import Request
from flask_login import login_required, current_user
from . import purchases
from app import db
from datetime import datetime, UTC, date, time, timedelta
from sqlalchemy import and_, or_

# Helper function to check admin access
def admin_required(f):
    """Decorator to require admin access for a view."""
    @login_required
    def decorated_function(*args, **kwargs):
        if not current_user.is_admin:
            flash('You do not have permission to perform this action.', 'error')
            return redirect(url_for('inventory.index'))
        return f(*args, **kwargs)
    decorated_function.__name__ = f.__name__
    return decorated_function

# Purchase Views
@purchases.route('/')
@admin_required
def list_purchases():
    # Retrieve filter parameters from the request
    supplier_name = request.args.get('supplier_name', '').strip()
    item_name = request.args.get('item_name', '').strip()
    start_date_str = request.args.get('start_date', '').strip()
    end_date_str = request.args.get('end_date', '').strip()

    # Start with all purchase transactions
    query = InventoryTransaction.query.filter_by(transaction_type='purchase')

    # Apply filters based on provided parameters
    if supplier_name:
        # Filter by supplier name (case-insensitive search)
        query = query.join(InventoryTransaction.supplier).filter(
            InventorySupplier.supplier_name.ilike(f'%{supplier_name}%')
        )

    if item_name:
        # Filter by inventory item name (case-insensitive search)
        query = query.join(InventoryTransaction.inventory).filter(
            Inventory.item_name.ilike(f'%{item_name}%')
        )

    # Date filtering
    try:
        start_date = None
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=UTC)
            query = query.filter(InventoryTransaction.timestamp >= start_date)

        end_date = None
        if end_date_str:
            # For end_date, filter up to the end of the day (23:59:59)
            end_date_parsed = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=UTC)
            end_of_day = end_date_parsed + timedelta(days=1) - timedelta(microseconds=1)
            query = query.filter(InventoryTransaction.timestamp <= end_of_day)

    except ValueError:
        flash("Invalid date format. Please use YYYY-MM-DD.", "danger")
        # Reset date filters if invalid to prevent breaking the page
        start_date_str = ''
        end_date_str = ''

    # Order the results
    purchases = query.order_by(InventoryTransaction.timestamp.desc()).all()

    # Pass the current filter values back to the template to persist them in the form
    return render_template(
        'purchases/list.html',
        purchases=purchases,
        current_supplier_name=supplier_name,
        current_item_name=item_name,
        current_start_date=start_date_str,
        current_end_date=end_date_str
    )

@purchases.route('/new', methods=['GET', 'POST'])
@admin_required
def new_purchase():
    items = Inventory.query.all()
    categories = Category.query.all()
    if request.method == 'POST':
        # Get the number of items in the purchase
        num_items = len(request.form.getlist('inventory_id'))

        try:
            for i in range(num_items):
                inventory_id = int(request.form.getlist('inventory_id')[i])
                quantity = int(request.form.getlist('quantity')[i])
                supplier_name = request.form.getlist('supplier')[i]
                unit_price = float(request.form.getlist('unit_price')[i] or 0)

                inventory = Inventory.query.get(inventory_id)
                if not inventory or quantity <= 0:
                    flash(f"Invalid item or quantity for item {i+1}.", "danger")
                    return redirect(url_for('purchases.new_purchase'))

                # Update inventory
                inventory.quantity += quantity
                if supplier_name:
                    inventory.supplier = supplier_name  # Keep this for backward compatibility
                if unit_price:
                    inventory.unit_price = unit_price  # Keep this for backward compatibility
                inventory.updated_by = current_user.id
                inventory.updated_at = datetime.now(UTC)

                # Create or update supplier record
                supplier = None
                if supplier_name:
                    supplier, error = InventorySupplier.get_or_create_supplier(
                        inventory_id=inventory.id,
                        supplier_name=supplier_name,
                        unit_price=unit_price
                    )
                    if error:
                        flash(f"Error managing supplier: {error}", "warning")

                # Log transaction
                transaction = InventoryTransaction(
                    inventory_id=inventory.id,
                    transaction_type='purchase',
                    quantity=quantity,
                    performed_by=current_user.id,
                    note=f"Purchased {quantity} of {inventory.item_name} from {supplier_name}",
                    supplier_id=supplier.id if supplier else None,
                    unit_price=unit_price
                )
                db.session.add(transaction)

            db.session.commit()
            flash("Purchase recorded!", "success")
            return redirect(url_for('purchases.list_purchases'))
        except Exception as e:
            db.session.rollback()
            flash(f"Failed to record purchase: {str(e)}", "danger")
            return redirect(url_for('purchases.new_purchase'))
    return render_template('purchases/new.html', items=items, categories=categories)

@purchases.route('/<int:purchase_id>')
@admin_required
def purchase_detail(purchase_id):
    purchase = InventoryTransaction.query.get_or_404(purchase_id)
    return render_template('purchases/detail.html', purchase=purchase)

@purchases.route('/<int:purchase_id>/delete', methods=['POST'])
@admin_required
def delete_purchase(purchase_id):
    purchase = InventoryTransaction.query.get_or_404(purchase_id)
    db.session.delete(purchase)
    db.session.commit()
    flash("Purchase deleted successfully.", "success")
    return redirect(url_for('purchases.list_purchases'))
