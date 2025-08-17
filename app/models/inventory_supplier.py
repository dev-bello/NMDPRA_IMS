from app import db
from datetime import datetime, UTC
from flask_login import current_user
from app.models.user import User
import logging

class InventorySupplier(db.Model):
    """Model for inventory suppliers."""
    __tablename__ = 'inventory_suppliers'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    inventory_id = db.Column(db.Integer, db.ForeignKey('inventories.id', ondelete="CASCADE"), nullable=False)
    supplier_name = db.Column(db.String(255), nullable=False)
    unit_price = db.Column(db.Float, nullable=True)
    last_purchase_date = db.Column(db.DateTime, default=datetime.now(UTC))
    created_at = db.Column(db.DateTime, default=datetime.now(UTC))
    updated_at = db.Column(db.DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))
    
    # Relationship
    # inventory = db.relationship('Inventory', backref='supplier_records')
    
    @classmethod
    def get_or_create_supplier(cls, inventory_id, supplier_name, unit_price=None):
        """Get an existing supplier or create a new one."""
        try:
            # Check if supplier already exists for this inventory
            supplier = cls.query.filter_by(
                inventory_id=inventory_id,
                supplier_name=supplier_name
            ).first()
            
            if supplier:
                # Update existing supplier
                if unit_price is not None:
                    supplier.unit_price = unit_price
                supplier.last_purchase_date = datetime.now(UTC)
                supplier.updated_at = datetime.now(UTC)
                db.session.commit()
                return supplier, None
            else:
                # Create new supplier
                supplier = cls(
                    inventory_id=inventory_id,
                    supplier_name=supplier_name,
                    unit_price=unit_price
                )
                db.session.add(supplier)
                db.session.commit()
                return supplier, None
        except Exception as e:
            db.session.rollback()
            return None, f"Error managing supplier: {str(e)}"
    
    @classmethod
    def get_suppliers_for_inventory(cls, inventory_id):
        """Get all suppliers for a specific inventory item."""
        return cls.query.filter_by(inventory_id=inventory_id).order_by(cls.last_purchase_date.desc()).all()
    
    @classmethod
    def get_supplier_by_id(cls, supplier_id):
        """Get a supplier by ID."""
        return cls.query.get(supplier_id)
    
    @classmethod
    def get_supplier_by_name(cls, supplier_name):
        """Get a supplier by name."""
        return cls.query.filter_by(supplier_name=supplier_name).first()
    
    @classmethod
    def get_suppliers(cls):
        """Get all suppliers."""
        return cls.query.all()
    
    def to_dict(self):
        """Convert supplier object to dictionary."""
        return {
            'id': self.id,
            'inventory_id': self.inventory_id,
            'supplier_name': self.supplier_name,
            'unit_price': self.unit_price,
            'last_purchase_date': self.last_purchase_date.isoformat() if self.last_purchase_date else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self):
        """String representation of InventorySupplier object."""
        return f'<InventorySupplier {self.supplier_name} for Inventory {self.inventory_id}>'
