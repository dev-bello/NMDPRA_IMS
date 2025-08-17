from app import db
from datetime import datetime, UTC
from flask_login import current_user
from app.models.user import User
import logging
from app.models.inventory_transaction import InventoryTransaction
from app.models.inventory_supplier import InventorySupplier

# Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

class Category(db.Model):
    """Model for inventory categories."""
    __tablename__ = 'categories'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    description = db.Column(db.String(255), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now(UTC))
    updated_at = db.Column(db.DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))
    
    # Relationship with Inventory items
    inventory_items = db.relationship('Inventory', backref='category', lazy=True)
    
    @classmethod
    def create_category(cls, name, description=None):
        """Create a new category if user is admin."""
        if not current_user.is_admin:
            # logger.warning(f"User {current_user.email} attempted to create category without admin privileges")
            return None, "Permission denied: Admin privileges required"
            
        try:
            # Check if category already exists
            existing_category = cls.query.filter(cls.name.ilike(name)).first()
            if existing_category:
                return None, f"Category '{name}' already exists"
                
            category = cls(name=name, description=description)
            db.session.add(category)
            db.session.commit()
            # logger.info(f"Category '{name}' created by {current_user.email}")
            return category, None
        except Exception as e:
            db.session.rollback()
            # logger.error(f"Error creating category: {e}")
            return None, f"Error creating category: {str(e)}"
    
    @classmethod
    def update_category(cls, category_id, name=None, description=None):
        """Update an existing category if user is admin."""
        if not current_user.is_admin:
            # logger.warning(f"User {current_user.email} attempted to update category without admin privileges")
            return None, "Permission denied: Admin privileges required"
            
        try:
            category = db.session.get(cls, category_id)
            if not category:
                return None, "Category not found"
                
            if name and name != category.name:
                # Check if new name already exists
                existing_category = cls.query.filter(
                    cls.name.ilike(name),
                    cls.id != category_id
                ).first()
                if existing_category:
                    return None, f"Category '{name}' already exists"
                category.name = name
                
            if description is not None:
                category.description = description
                
            category.updated_at = datetime.now(UTC)
            db.session.commit()
            # logger.info(f"Category {category_id} updated by {current_user.email}")
            return category, None
        except Exception as e:
            db.session.rollback()
            # logger.error(f"Error updating category: {e}")
            return None, f"Error updating category: {str(e)}"
    
    @classmethod
    def delete_category(cls, category_id):
        """Delete a category if user is admin."""
        if not current_user.is_admin:
            # logger.warning(f"User {current_user.email} attempted to delete category without admin privileges")
            return False, "Permission denied: Admin privileges required"
            
        try:
            category = db.session.get(cls, category_id)
            if not category:
                return False, "Category not found"
                
            # Check if category has inventory items
            if category.inventory_items:
                return False, "Cannot delete category with associated inventory items"
                
            db.session.delete(category)
            db.session.commit()
            # logger.info(f"Category {category_id} deleted by {current_user.email}")
            return True, None
        except Exception as e:
            db.session.rollback()
            # logger.error(f"Error deleting category: {e}")
            return False, f"Error deleting category: {str(e)}"
    
    def to_dict(self):
        """Convert category object to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        
    def __repr__(self):
        """String representation of Category object."""
        return f'<Category {self.name}>'

class Inventory(db.Model):
    """Model for inventory items."""
    __tablename__ = 'inventories'
    
    # Add these as class variables
    LOCATIONS = ['Headquarters']
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    item_name = db.Column(db.String(100), unique=True, nullable=False)
    description = db.Column(db.Text, nullable=True)
    quantity = db.Column(db.Integer, default=0)
    category_id = db.Column(db.Integer, db.ForeignKey('categories.id'), nullable=False)
    unit_price = db.Column(db.Numeric(10, 2), nullable=True)
    supplier = db.Column(db.String(255), nullable=True)
    location = db.Column(db.String(100), nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    updated_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(UTC))
    updated_at = db.Column(db.DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))
    
    # Relationships
    creator = db.relationship('User', foreign_keys=[created_by], backref='created_inventories')
    updater = db.relationship('User', foreign_keys=[updated_by], backref='updated_inventories')
    supplier_records = db.relationship(
    'InventorySupplier',
    backref='inventory',
    passive_deletes=True)
    
    @classmethod
    def get_all_inventory(cls):
        """Get all inventory items."""
        return cls.query.all()
    
    @classmethod
    def get_inventory_by_id(cls, inventory_id):
        """Get inventory item by ID."""
        return db.session.get(cls, inventory_id)
    
    @classmethod
    def get_inventory_by_category(cls, category_id):
        """Get inventory items by category."""
        return cls.query.filter_by(category_id=category_id).all()
    
    @classmethod
    def create_inventory(cls, item_name, category_id, quantity, description=None, 
                         unit_price=None, location=None, supplier=None):
        """Create a new inventory item if user is admin."""
        if not current_user.is_admin:
            return None, "Permission denied: Admin privileges required"
            
        try:
            # Check if item already exists
            existing_item = cls.query.filter(cls.item_name.ilike(item_name)).first()
            if existing_item:
                return None, f"Item '{item_name}' already exists"
            
            # Validate category exists
            category = db.session.get(Category, category_id)
            if not category:
                return None, "Invalid category ID"
                
            # Validate location
            if location not in cls.LOCATIONS:
                return None, "Invalid location. Must be 'Headquarters'"
                
            inventory = cls(
                item_name=item_name,
                category_id=category_id,
                quantity=quantity,
                description=description,
                unit_price=unit_price,
                supplier=supplier,
                location=location,
                created_by=current_user.id,
                updated_by=current_user.id
            )
            db.session.add(inventory)
            db.session.commit()

           
            initial_transaction = InventoryTransaction(
                inventory_id=inventory.id,
                transaction_type='initial',  
                quantity=quantity,
                performed_by=current_user.id,
                unit_price=unit_price,
                timestamp=inventory.created_at,  # Use the same timestamp as item creation
                note='Initial stock on item creation'
            )
            db.session.add(initial_transaction)
            db.session.commit()
           

            return inventory, None
        except Exception as e:
            db.session.rollback()
            return None, f"Error creating inventory: {str(e)}"
    
    @classmethod
    def update_inventory(cls, inventory_id, item_name=None, category_id=None, 
                         quantity=None, description=None, unit_price=None, location=None, supplier=None):
        """Update an existing inventory item if user is admin."""
        if not current_user.is_admin:
            return None, "Permission denied: Admin privileges required"
            
        try:
            inventory = db.session.get(Inventory, inventory_id)
            if not inventory:
                return None, "Inventory item not found"
                
            # Handle non-quantity updates
            original_price = inventory.unit_price
            if item_name and item_name != inventory.item_name:
                # Check if new name already exists
                existing_item = cls.query.filter(
                    cls.item_name.ilike(item_name),
                    cls.id != inventory_id
                ).first()
                if existing_item:
                    return None, f"Item '{item_name}' already exists"
                inventory.item_name = item_name
                
            if category_id:
                # Validate category exists
                category = Category.query.get(category_id)
                if not category:
                    return None, "Invalid category ID"
                inventory.category_id = category_id
            if description is not None:
                inventory.description = description
            if unit_price is not None:
                inventory.unit_price = unit_price
            if location is not None:
                if location not in cls.LOCATIONS:
                    return None, "Invalid location. Must be either 'Jabi' or 'Headquarters'"
                inventory.location = location
            if supplier is not None:
                inventory.supplier = supplier
            # Handle quantity adjustment if changed
            if quantity is not None and quantity != inventory.quantity:
                quantity_change = quantity - inventory.quantity
                inv, error = cls.adjust_quantity(
                    inventory_id,
                    quantity_change,
                    note=f"Manual adjustment by {current_user.name} via edit"
                )
                if error:
                    return None, error
            
            if unit_price is not None and unit_price != original_price:
                price_update_transaction = InventoryTransaction(
                    inventory_id=inventory.id,
                    transaction_type='price_update',
                    quantity=0,  # No change in quantity
                    unit_price=unit_price,
                    performed_by=current_user.id,
                    timestamp=datetime.now(UTC),
                    note=f"Price updated from {original_price} to {unit_price} by {current_user.name}"
                )
                db.session.add(price_update_transaction)
                
            inventory.updated_by = current_user.id
            inventory.updated_at = datetime.now(UTC)
            db.session.commit()
            return inventory, None
        except Exception as e:
            db.session.rollback()
            return None, f"Error updating inventory: {str(e)}"
    
    @classmethod
    def delete_inventory(cls, inventory_id):
        """Delete an inventory item if user is admin."""
        if not current_user.is_admin:
            return False, "Permission denied: Admin privileges required"
            
        try:
            inventory = db.session.get(Inventory, inventory_id)
            if not inventory:
                return False, "Inventory item not found"
                
            db.session.delete(inventory)
            db.session.commit()
            return True, None
        except Exception as e:
            db.session.rollback()
            return False, f"Error deleting inventory: {str(e)}"
    
    @classmethod
    def adjust_quantity(cls, inventory_id, quantity_change, note=None):
        """Adjust inventory quantity if user is admin."""
        if not current_user.is_admin:
            return None, "Permission denied: Admin privileges required"
            
        try:
            inventory = db.session.get(Inventory, inventory_id)
            if not inventory:
                return None, "Inventory item not found"
                
            new_quantity = inventory.quantity + quantity_change
            if new_quantity < 0:
                return None, "Insufficient quantity"
                
            inventory.quantity = new_quantity
            inventory.updated_by = current_user.id
            inventory.updated_at = datetime.now(UTC)

            # Log the transaction
            transaction = InventoryTransaction(
                inventory_id=inventory.id,
                transaction_type='adjustment',
                quantity=quantity_change,
                performed_by=current_user.id,
                timestamp=datetime.now(UTC),
                note=note or f"Manual adjustment by {current_user.name}"
            )
            db.session.add(transaction)
            
            db.session.commit()
            return inventory, None
        except Exception as e:
            db.session.rollback()
            return None, f"Error adjusting inventory quantity: {str(e)}"
    
    def to_dict(self):
        """Convert inventory object to dictionary."""
        return {
            'id': self.id,
            'item_name': self.item_name,
            'description': self.description,
            'quantity': self.quantity,
            'category_id': self.category_id,
            'category_name': self.category.name,
            'unit_price': self.unit_price,
            'supplier': self.supplier,
            'location': self.location,
            'created_by': self.created_by,
            'creator_name': self.creator.name,
            'updated_by': self.updated_by,
            'updater_name': self.updater.name,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        
    def __repr__(self):
        """String representation of Inventory object."""
        return f'<Inventory {self.item_name}>'

