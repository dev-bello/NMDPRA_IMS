from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify
from flask_login import login_required, current_user
from app.models.inventory import Inventory, Category
from app.models.inventory_transaction import InventoryTransaction
from app.models.inventory_supplier import InventorySupplier
from . import inventory
import logging

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

def get_stock_status(quantity):
    """Helper function to determine stock status based on quantity."""
    if quantity == 0:
        return 'out-of-stock'
    elif quantity < 15:
        return 'low-stock'
    else:
        return 'in-stock'

def get_stock_status_text(quantity):
    """Helper function to get stock status text."""
    if quantity == 0:
        return 'Out of Stock'
    elif quantity < 15:
        return 'Low Stock'
    else:
        return 'In Stock'

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

# Inventory Views
@inventory.route('/inventory', methods=['GET'])
@login_required
def index():
    """Main inventory view - displays all items with filtering capabilities"""
    inventories = Inventory.get_all_inventory()
    categories = Category.query.all()
    return render_template('inventory/index.html',
                          inventories=inventories,
                          categories=categories,
                          is_admin=current_user.is_admin,
                          get_stock_status=get_stock_status,
                          get_stock_status_text=get_stock_status_text)

@inventory.route('/item/<int:inventory_id>')
@login_required
def view_item(inventory_id):
    """Detailed view of a single inventory item with full information"""
    inventory = Inventory.get_inventory_by_id(inventory_id)
    if not inventory:
        flash('Inventory item not found.', 'error')
        return redirect(url_for('inventory.index'))
    
    # Get supplier history for this inventory item
    supplier_history = InventorySupplier.get_suppliers_for_inventory(inventory_id)
    
    return render_template('inventory/view_item.html',
                          inventory=inventory,
                          supplier_history=supplier_history,
                          is_admin=current_user.is_admin,
                          get_stock_status=get_stock_status,
                          get_stock_status_text=get_stock_status_text)

@inventory.route('/bulk-create', methods=['GET', 'POST'])
@admin_required
def bulk_create_items():
    categories = Category.query.all()
    if request.method == 'POST':
        item_names = request.form.getlist('item_name')
        category_ids = request.form.getlist('category_id')
        quantities = request.form.getlist('quantity')
        descriptions = request.form.getlist('description')
        unit_prices = request.form.getlist('unit_price')
        locations = request.form.getlist('location')
        suppliers = request.form.getlist('supplier')

        results = []
        for i in range(len(item_names)):
            inventory, error = Inventory.create_inventory(
                item_name=item_names[i],
                category_id=category_ids[i],
                quantity=int(quantities[i]),
                description=descriptions[i],
                unit_price=float(unit_prices[i]) if unit_prices[i] else None,
                location=locations[i],
                supplier=suppliers[i]
            )
            results.append((item_names[i], error))

        for name, error in results:
            if error:
                flash(f"Error adding '{name}': {error}", 'error')
            else:
                flash(f"Item '{name}' added successfully.", 'success')
        return redirect(url_for('inventory.index'))

    return render_template('inventory/bulk_create_items.html', categories=categories)

@inventory.route('/create', methods=['GET', 'POST'])
@admin_required
def create_item():
    """Admin only: Creates new inventory items with category assignment"""
    categories = Category.query.all()
    
    if request.method == 'POST':
        item_name = request.form['item_name']
        category_id = request.form['category_id']
        quantity = int(request.form['quantity'])
        description = request.form.get('description')
        unit_price = float(request.form['unit_price']) if request.form.get('unit_price') else None
        location = request.form.get('location')
        supplier = request.form.get('supplier')
        
        inventory, error = Inventory.create_inventory(
            item_name=item_name,
            category_id=category_id,
            quantity=quantity,
            description=description,
            unit_price=unit_price,
            location=location,
            supplier=supplier
        )
        
        if error:
            flash(error, 'error')
            return render_template('inventory/create_item.html', categories=categories)
        
        flash('Inventory item created successfully.', 'success')
        return redirect(url_for('inventory.index'))
    
    return render_template('inventory/create_item.html', categories=categories)
@inventory.route('/edit/<int:inventory_id>', methods=['GET', 'POST'])
@admin_required
def edit_item(inventory_id):
    """Admin only: Modifies existing inventory items including quantity and details"""
    inventory = Inventory.get_inventory_by_id(inventory_id)
    if not inventory:
        flash('Inventory item not found.', 'error')
        return redirect(url_for('inventory.index'))
    
    categories = Category.query.all()
    
    if request.method == 'POST':
        item_name = request.form['item_name']
        category_id = request.form['category_id']
        quantity = int(request.form['quantity'])
        description = request.form.get('description')
        unit_price = float(request.form['unit_price']) if request.form.get('unit_price') else None
        location = request.form.get('location')
        supplier = request.form.get('supplier')
        
        updated_inventory, error = Inventory.update_inventory(
            inventory_id=inventory_id,
            item_name=item_name,
            category_id=category_id,
            quantity=quantity,
            description=description,
            unit_price=unit_price,
            location=location,
            supplier=supplier
        )
        
        if error:
            flash(error, 'error')
            return render_template('inventory/edit_item.html', 
                                  inventory=inventory,
                                  categories=categories)
        
        flash('Inventory item updated successfully.', 'success')
        return redirect(url_for('inventory.view_item', inventory_id=inventory_id))
    
    return render_template('inventory/edit_item.html', 
                          inventory=inventory,
                          categories=categories)

@inventory.route('/delete/<int:inventory_id>', methods=['POST'])
@admin_required
def delete_item(inventory_id):
    """Admin only: Can delete an existing inventory item."""
    success, error = Inventory.delete_inventory(inventory_id)
    
    if error:
        flash(error, 'error')
    else:
        flash('Inventory item deleted successfully.', 'success')
    
    return redirect(url_for('inventory.index'))

@inventory.route('/adjust-quantity/<int:inventory_id>', methods=['POST'])
@admin_required
def adjust_quantity(inventory_id):
    """Admin only: Only an admin can adjust the quantity of an inventory item."""
    try:
        quantity_change = int(request.form['quantity_change'])
        inventory, error = Inventory.adjust_quantity(inventory_id, quantity_change)
        
        if error:
            flash(error, 'error')
        else:
            flash(f'Inventory quantity adjusted successfully. New quantity: {inventory.quantity}', 'success')
    except ValueError:
        flash('Invalid quantity value.', 'error')
    
    return redirect(url_for('inventory.view_item', inventory_id=inventory_id))

# Category Views
@inventory.route('/categories')
@login_required
def categories():
    """All users: Display all categories."""
    categories = Category.query.all()
    return render_template('inventory/category/index.html', 
                          categories=categories,
                          is_admin=current_user.is_admin)

@inventory.route('/category/create', methods=['GET', 'POST'])
@admin_required
def create_category():
    """Admin only: Only admins have access to create a new category."""
    if request.method == 'POST':
        name = request.form['name']
        description = request.form.get('description')
        
        category, error = Category.create_category(name=name, description=description)
        
        if error:
            flash(error, 'error')
            return render_template('inventory/category/create_category.html')
        
        flash('Category created successfully.', 'success')
        return redirect(url_for('inventory.categories'))
    
    return render_template('inventory/category/create_category.html')

@inventory.route('/category/edit/<int:category_id>', methods=['GET', 'POST'])
@admin_required
def edit_category(category_id):
    """Admin only: Only admin have access to edit an existing category."""
    category = Category.query.get(category_id)
    if not category:
        flash('Category not found.', 'error')
        return redirect(url_for('inventory.categories'))
    
    if request.method == 'POST':
        name = request.form['name']
        description = request.form.get('description')
        
        updated_category, error = Category.update_category(
            category_id=category_id,
            name=name,
            description=description
        )
        
        if error:
            flash(error, 'error')
            return render_template('inventory/category/edit_category.html', category=category)
        
        flash('Category updated successfully.', 'success')
        return redirect(url_for('inventory.categories'))
    
    return render_template('inventory/category/edit_category.html', category=category)

@inventory.route('/category/delete/<int:category_id>', methods=['POST'])
@admin_required
def delete_category(category_id):
    """Admin only: Only admin have access to delete an existing category."""
    success, error = Category.delete_category(category_id)
    
    if error:
        flash(error, 'error')
    else:
        flash('Category deleted successfully.', 'success')
    
    return redirect(url_for('inventory.categories'))

# API Routes for AJAX requests
@inventory.route('/api/items', methods=['GET'])
@login_required
def api_get_items():
    """API endpoint to get all inventory items."""
    try:
        items = [item.to_dict() for item in Inventory.get_all_inventory()]
        return jsonify(items)
    except Exception as e:
        # logger.error(f"Error fetching items: {e}")
        return jsonify({'error': 'Failed to fetch items'}), 500

@inventory.route('/api/items/<int:category_id>', methods=['GET'])
@login_required
def api_get_items_by_category(category_id):
    """API endpoint to get inventory items by category."""
    try:
        items = [item.to_dict() for item in Inventory.get_inventory_by_category(category_id)]
        return jsonify(items)
    except Exception as e:
        # logger.error(f"Error fetching items by category: {e}")
        return jsonify({'error': 'Failed to fetch items'}), 500
