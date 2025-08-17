from app import db
from datetime import datetime, UTC
from flask_login import current_user
from app.models.user import User
from app.models.inventory_supplier import InventorySupplier
import logging


class InventoryTransaction(db.Model):
    __tablename__ = 'inventory_transactions'
    id = db.Column(db.Integer, primary_key=True)
    inventory_id = db.Column(db.Integer, db.ForeignKey('inventories.id', ondelete='CASCADE'),nullable=False)
    transaction_type = db.Column(db.String(50), nullable=False)  # 'purchase', 'issue', 'adjustment',  'initial'
    quantity = db.Column(db.Integer, nullable=False)  # +ve for in, -ve for out
    related_request_id = db.Column(db.Integer, db.ForeignKey('requests.id'), nullable=True)
    performed_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.now(UTC))
    note = db.Column(db.Text, nullable=True)
    supplier_id = db.Column(db.Integer, db.ForeignKey('inventory_suppliers.id'), nullable=True)
    unit_price = db.Column(db.Numeric(10, 2), nullable=True)

    # Relationships
    inventory = db.relationship('Inventory')
    user = db.relationship('User')
    request = db.relationship('Request')
    supplier = db.relationship('InventorySupplier')

    def to_dict(self):
        return {
            'id': self.id,
            'inventory_id': self.inventory_id,
            'transaction_type': self.transaction_type,
            'quantity': self.quantity,
            'related_request_id': self.related_request_id,
            'performed_by': self.performed_by,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'note': self.note,
            'supplier_id': self.supplier_id,
            'supplier_name': self.supplier.supplier_name if self.supplier else None,
            'unit_price': self.unit_price
        }