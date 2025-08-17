import uuid
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from app import db
from sqlalchemy.ext.hybrid import hybrid_property

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)

class ReportCache(db.Model):
    """
    Model for temporarily storing report data to avoid session cookie size limitations.
    
    This model stores serialized report data in the database with an expiration time.
    It's designed to work in cloud environments where files might not be accessible
    across multiple application instances.
    """
    __tablename__ = 'report_cache'
    
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'))
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    expires_at = db.Column(db.DateTime, index=True, default=lambda: datetime.now(timezone.utc) + timedelta(hours=24))
    
    # Store JSON data as text
    _report_data = db.Column('report_data', db.Text)
    _category_totals = db.Column('category_totals', db.Text)
    _grand_totals = db.Column('grand_totals', db.Text)
    _meta = db.Column('meta', db.Text)
    
    # Define hybrid properties for automatic serialization/deserialization
    @hybrid_property
    def report_data(self):
        return json.loads(self._report_data) if self._report_data else {}
    
    @report_data.setter
    def report_data(self, value):
        self._report_data = json.dumps(value, cls=DecimalEncoder)
    
    @hybrid_property
    def category_totals(self):
        return json.loads(self._category_totals) if self._category_totals else {}
    
    @category_totals.setter
    def category_totals(self, value):
        self._category_totals = json.dumps(value, cls=DecimalEncoder)
    
    @hybrid_property
    def grand_totals(self):
        return json.loads(self._grand_totals) if self._grand_totals else {}
    
    @grand_totals.setter
    def grand_totals(self, value):
        self._grand_totals = json.dumps(value, cls=DecimalEncoder)
    
    @hybrid_property
    def meta(self):
        return json.loads(self._meta) if self._meta else {}
    
    @meta.setter
    def meta(self, value):
        self._meta = json.dumps(value, cls=DecimalEncoder)
    
    @classmethod
    def cleanup_expired(cls):
        """
        Delete all expired report cache entries.
        
        This method should be called periodically to prevent database bloat.
        It can be triggered on report generation or via a scheduled task.
        """
        try:
            # Perform a bulk delete for efficiency.
            # This is much faster than loading all objects into the session.
            num_deleted = cls.query.filter(cls.expires_at < datetime.now(timezone.utc)).delete(synchronize_session=False)
            db.session.commit()
            return num_deleted
        except Exception:
            db.session.rollback()
            # Re-raise the exception to be logged by the caller
            raise
    
    @classmethod
    def get_for_user(cls, report_id, user_id):
        """
        Retrieve a report cache entry for a specific user.
        
        This method ensures that users can only access their own reports.
        
        Args:
            report_id (str): The UUID of the report
            user_id (int): The ID of the user
            
        Returns:
            ReportCache: The report cache entry or None if not found
        """
        return cls.query.filter_by(id=report_id, user_id=user_id).first()

