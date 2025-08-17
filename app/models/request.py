from app import db
from datetime import datetime, UTC
from flask_login import current_user
from app.models.user import User
from app.models.inventory import Inventory
import enum
import uuid
from sqlalchemy.orm import joinedload
from sqlalchemy import Enum as SQLAEnum
from app.models.inventory_transaction import InventoryTransaction
import sqlalchemy as sa

class DirectorateEnum(enum.Enum):
    ACE = "ACE"
    Audit = "Audit"
    DSSRI = "DSSRI"
    HPPITI = "HPPITI"
    CSA = "CS&A"
    MDGIF = "MDGIF"
    FA = "F&A"
    Procurement = "Procurement"
    HSEC = "HSEC"
    ERSP = "ERSP"
    ICT = "ICT"

class RequestStatus(enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    PARTIALLY_APPROVED = "partially approved"
    REJECTED = "rejected"
    COLLECTED = "collected"

class ItemRequestStatus(enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    COLLECTED = "collected"

class Request(db.Model):
    """Model for item requests."""
    __tablename__ = 'requests'

    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    reference_number = db.Column(db.String(50), unique=True, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    status = db.Column(SQLAEnum(RequestStatus), default=RequestStatus.PENDING, nullable=False)
    location = db.Column(db.String(100), nullable=False)
    admin_message = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.now(UTC))
    updated_at = db.Column(db.DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))
    deleted_at = db.Column(db.DateTime, nullable=True)
    deleted_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    deletion_reason = db.Column(db.Text, nullable=True)
    approved_by = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    directorate = db.Column(
        sa.Enum(DirectorateEnum, name="directorate_enum"), 
        nullable=False
    )
    department = db.Column(db.String(100), nullable=True)
    unit = db.Column(db.String(100), nullable=False)

    # Relationships
    user = db.relationship('User', foreign_keys=[user_id], backref='requests')
    approved_by_user = db.relationship('User', foreign_keys=[approved_by], backref='approved_requests')
    deleted_by_user = db.relationship('User', foreign_keys=[deleted_by], backref='deleted_requests')
    items = db.relationship('RequestItem', backref='request', cascade='all, delete-orphan')

    # TODO: Remove the request date functionality.
    # The `request_date` parameter should be removed from the method signature.
    # The method signature should be updated to:
    # def create_request(cls, user_id, location, directorate, department, unit):
    #
    # The `created_at` field should be updated to always use the current time.
    # The line should be updated to:
    # created_at=datetime.now(UTC)
    @classmethod
    def create_request(cls, user_id, location, directorate, department, unit, request_date=None):
        """Create a new request."""
        try:
            reference_number = f"REQ-{uuid.uuid4().hex[:8].upper()}"
            request = cls(
                reference_number=reference_number,
                user_id=user_id,
                location=location,
                directorate=DirectorateEnum(directorate),
                department=department,
                unit=unit,
                # START: MODIFICATION - Allow backdating of requests
                # If a request_date is provided, it overrides the default created_at timestamp.
                # This allows the request to be recorded for a past date.
                created_at=request_date if request_date else datetime.now(UTC)
                # END: MODIFICATION
            )
            db.session.add(request)
            db.session.commit()
            return request, None
        except Exception as e:
            db.session.rollback()
            return None, f"Error creating request: {str(e)}"

    @classmethod
    def get_user_requests(cls, user_id, search_item=None, search_date=None):
        """Get all requests for a user, excluding soft-deleted, with optional filters."""
        query = cls.query.options(
            joinedload(cls.items).joinedload(RequestItem.inventory)
        ).filter(
            cls.user_id == user_id,
            cls.deleted_at.is_(None)
        )

        # START: MODIFICATION - Add search functionality
        # Filter by item name if a search_item is provided.
        if search_item:
            query = query.join(RequestItem).join(Inventory).filter(Inventory.item_name.ilike(f"%{search_item}%"))

        # Filter by date if a search_date is provided.
        if search_date:
            try:
                search_date_obj = datetime.strptime(search_date, '%Y-%m-%d').date()
                query = query.filter(db.func.date(cls.created_at) == search_date_obj)
            except ValueError:
                # Handle invalid date format if necessary
                pass
        # END: MODIFICATION

        return query.order_by(cls.created_at.desc()).all()

    @classmethod
    def get_all_requests(cls, search_item=None, search_date=None):
        """Get all requests, excluding soft-deleted, with optional filters."""
        query = cls.query.options(
            joinedload(cls.items).joinedload(RequestItem.inventory)
        ).filter(cls.deleted_at.is_(None))

        # START: MODIFICATION - Add search functionality
        # Filter by item name if a search_item is provided.
        if search_item:
            query = query.join(RequestItem).join(Inventory).filter(Inventory.item_name.ilike(f"%{search_item}%"))

        # Filter by date if a search_date is provided.
        if search_date:
            try:
                search_date_obj = datetime.strptime(search_date, '%Y-%m-%d').date()
                query = query.filter(db.func.date(cls.created_at) == search_date_obj)
            except ValueError:
                # Handle invalid date format if necessary
                pass
        # END: MODIFICATION

        return query.order_by(cls.created_at.desc()).all()

    @classmethod
    def get_request_by_id(cls, request_id):
        """Get request by ID, eager load items and inventory."""
        return (
            cls.query.options(joinedload(cls.items).joinedload(RequestItem.inventory))
            .filter_by(id=request_id)
            .filter(cls.deleted_at.is_(None))
            .first()
        )

    def update_status(self, status, admin_message=None, approved_by_user_id=None):
        """Update request status with validation using Enum."""
        allowed_transitions = {
            RequestStatus.PENDING: [
                RequestStatus.APPROVED,
                RequestStatus.REJECTED,
                RequestStatus.PARTIALLY_APPROVED
            ],
            RequestStatus.APPROVED: [
                RequestStatus.COLLECTED
            ],
            RequestStatus.PARTIALLY_APPROVED: [
                RequestStatus.COLLECTED
            ],
            RequestStatus.REJECTED: [],
            RequestStatus.COLLECTED: []
        }
        if status not in allowed_transitions.get(self.status, []):
            return False, f"Invalid status transition from {self.status} to {status}"
        try:
            self.status = status
            if admin_message:
                self.admin_message = admin_message
            if approved_by_user_id:
                self.approved_by = approved_by_user_id
            self.updated_at = datetime.now(UTC)
            db.session.commit()
            return True, None
        except Exception as e:
            db.session.rollback()
            return False, f"Error updating request status: {str(e)}"

    def update_status_based_on_items(self):
        """Update request status to partially_approved if some items are approved and others rejected."""
        statuses = [item.status for item in self.items]
        if (ItemRequestStatus.APPROVED in statuses and
            ItemRequestStatus.REJECTED in statuses):
            self.status = RequestStatus.PARTIALLY_APPROVED
            self.updated_at = datetime.now(UTC)
            db.session.commit()

    # Soft delete a request
    def soft_delete(self, deleted_by_user_id, reason=None):
        """Soft delete the request."""
        try:
            self.deleted_at = datetime.now(UTC)
            self.deleted_by = deleted_by_user_id
            self.deletion_reason = reason
            db.session.commit()
            return True, None
        except Exception as e:
            db.session.rollback()
            return False, f"Error deleting request: {str(e)}"
        
   # Get all soft-deleted requests
    @classmethod
    def get_deleted_requests(cls):
        """Get all soft-deleted requests."""
        return (
            cls.query.options(joinedload(cls.items).joinedload(RequestItem.inventory))
            .filter(cls.deleted_at.isnot(None))
            .order_by(cls.deleted_at.desc())
            .all()
        )
    # Permanently delete a soft-deleted request
    def permanent_delete_if_soft_deleted(self):
        """Permanently delete the request if it is soft-deleted."""
        if self.deleted_at is not None:
            try:
                db.session.delete(self)
                db.session.commit()
                return True, None
            except Exception as e:
                db.session.rollback()
                return False, f"Error permanently deleting request: {str(e)}"
        else:
            return False, "Request is not soft-deleted and cannot be permanently deleted."

    # Restore a deleted request
    def restore(self):
        """Restore a soft-deleted request."""
        try:
            self.deleted_at = None
            self.deleted_by = None
            self.deletion_reason = None
            db.session.commit()
            return True, None
        except Exception as e:
            db.session.rollback()
            return False, f"Error restoring request: {str(e)}"

    # Approve a request (with inventory validation)
    def approve(self, admin_message=None, approved_by_user_id=None):
        """Approve the request with inventory validation."""
        try:
            # Use a transaction for atomicity
            with db.session.begin():
                for item in self.items:
                    if not item.validate_inventory_quantity(item.quantity_approved):
                        raise Exception(f"Insufficient inventory for item {item.inventory.item_name}")
                self.status = RequestStatus.APPROVED
                if admin_message:
                    self.admin_message = admin_message
                if approved_by_user_id:
                    self.approved_by = approved_by_user_id
                self.updated_at = datetime.now(UTC)
                for item in self.items:
                    item.approve(item.quantity_approved)
            return True, None
        except Exception as e:
            db.session.rollback()
            return False, f"Error approving request: {str(e)}"

    # Mark request as collected
    def mark_collected(self, admin_note=None, approved_by_user_id=None):
        """Mark request as collected and adjust inventory for approved items only."""
        try:
            if self.status not in [RequestStatus.APPROVED, RequestStatus.PARTIALLY_APPROVED]:
                raise Exception("Only approved or partially approved requests can be marked as collected")
            for item in self.items:
                if item.status == ItemRequestStatus.APPROVED:
                    if not item.process_collection():
                        raise Exception(f"Failed to process collection for item {item.inventory.item_name}")
                    item.status = ItemRequestStatus.COLLECTED
                    item.updated_at = datetime.now(UTC)
                    transaction = InventoryTransaction(
                        inventory_id=item.inventory_id,
                        transaction_type='issue',
                        quantity=-item.quantity_approved,
                        related_request_id=self.id,
                        performed_by=current_user.id,
                        # START: MODIFICATION - Ensure report accuracy
                        # The inventory transaction timestamp is set to the request's created_at date.
                        # This ensures that backdated requests are reflected correctly in reports.
                        timestamp=self.created_at,
                        # END: MODIFICATION
                        note=f"Collected by {current_user.name}"
                    )
                    db.session.add(transaction)
                    
            self.status = RequestStatus.COLLECTED
            
            if admin_note:
                self.admin_message = admin_note
            if approved_by_user_id:
                self.approved_by = approved_by_user_id
            self.updated_at = datetime.now(UTC)
            db.session.commit()
            return True, None
        except Exception as e:
            db.session.rollback()
            return False, f"Error marking request as collected: {str(e)}"
        
    
    def to_dict(self):
        """Convert request object to dictionary."""
        return {
            'id': self.id,
            'reference_number': self.reference_number,
            'user_id': self.user_id,
            'user_name': self.user.name,
            'user_email': self.user.email,
            'user_department': self.user.department,
            'user_job_title': self.user.job_title,
            'status': self.status.value,
            'location': self.location,
            'admin_message': self.admin_message,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'items': [item.to_dict() for item in self.items],
            "deleted_at": self.deleted_at.isoformat() if self.deleted_at else None,
            "deleted_by": self.deleted_by,
            "deletion_reason": self.deletion_reason,
            "approved_by": self.approved_by,
            "approved_by_name": User.query.get(self.approved_by).name if self.approved_by else None,
            "approved_by_email": User.query.get(self.approved_by).email if self.approved_by else None,
            "approved_by_department": User.query.get(self.approved_by).department if self.approved_by else None,
            "approved_by_job_title": User.query.get(self.approved_by).job_title if self.approved_by else None,
            'directorate': self.directorate.value if self.directorate else None,
            'department': self.department,
            'unit': self.unit
        }

    def __repr__(self):
        """String representation of Request object."""
        return f'<Request {self.reference_number}>'
    

class RequestItem(db.Model):
    """Model for requested items."""
    __tablename__ = 'request_items'
    
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    request_id = db.Column(db.Integer, db.ForeignKey('requests.id', ondelete="CASCADE"), nullable=False)
    inventory_id = db.Column(db.Integer, db.ForeignKey('inventories.id', ondelete='CASCADE'), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    quantity_approved = db.Column(db.Integer, nullable=False)  # New field, set to quantity by default
    status = db.Column(SQLAEnum(ItemRequestStatus), default=ItemRequestStatus.PENDING, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.now(UTC))
    updated_at = db.Column(db.DateTime, default=datetime.now(UTC), onupdate=datetime.now(UTC))
    
    # Relationships
    inventory = db.relationship('Inventory')

    @classmethod
    def create_request_item(cls, request_id, inventory_id, quantity):
        """Create a new request item."""
        try:
            item = cls(
                request_id=request_id,
                inventory_id=inventory_id,
                quantity=quantity,
                quantity_approved=quantity  # Default approved quantity to requested
            )
            db.session.add(item)
            db.session.commit()
            return item, None
        except Exception as e:
            db.session.rollback()
            return None, f"Error creating request item: {str(e)}"

    def approve(self, approved_quantity):
        """Approve the request item using Enum and set approved quantity."""
        # Allow quantity_approved > quantity for future flexibility, but comment for restriction
        # To restrict: if approved_quantity > self.quantity: raise Exception("Cannot approve more than requested")
        self.status = ItemRequestStatus.APPROVED
        self.quantity_approved = approved_quantity
        self.updated_at = datetime.now(UTC)

    def reject(self):
        """Reject the request item using Enum and set approved quantity to 0."""
        self.status = ItemRequestStatus.REJECTED
        self.quantity_approved = 0
        self.updated_at = datetime.now(UTC)
        
    def validate_inventory_quantity(self, approved_quantity):
        """Check if approved quantity is available in inventory."""
        return self.inventory.quantity >= int(approved_quantity)

    def process_collection(self):
        """Process item collection and adjust inventory."""
        try:
            if not self.validate_inventory_quantity(self.quantity_approved):
                return False
            if self.inventory.quantity - self.quantity_approved < 0:
                return False
            self.inventory.quantity -= self.quantity_approved
            self.updated_at = datetime.now(UTC)
            return True
        except Exception:
            return False
        
    def to_dict(self):
        """Convert request item object to dictionary."""
        return {
            'id': self.id,
            'request_id': self.request_id,
            'inventory_id': self.inventory_id,
            'item_name': self.inventory.item_name,
            'quantity': self.quantity,
            'quantity_approved': self.quantity_approved,
            'status': self.status.value,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
    
    def __repr__(self):
        """String representation of RequestItem object."""
        return f'<RequestItem {self.id}>'
