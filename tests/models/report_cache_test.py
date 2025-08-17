import unittest
from app import create_app, db
from app.models.user import User
from app.models.report_cache import ReportCache
from datetime import datetime, timedelta, timezone
import json

class ReportCacheTestCase(unittest.TestCase):
    def setUp(self):
        """Set up a test environment."""
        self.app = create_app('testing')
        self.app_context = self.app.app_context()
        self.app_context.push()
        # Ensure a clean slate for each test
        db.drop_all()
        db.create_all()
        
        # Create a dummy user for associating reports
        self.user = User(name="Test User", email="test@example.com", is_admin=True)
        db.session.add(self.user)
        db.session.commit()

    def tearDown(self):
        """Clean up the test environment."""
        db.session.remove()
        db.drop_all()
        self.app_context.pop()

    def test_cleanup_expired_reports(self):
        """
        Test that the cleanup_expired class method correctly deletes only the
        expired report cache entries.
        """
        # 1. Create one valid (not expired) report cache entry
        valid_report = ReportCache(
            user_id=self.user.id,
            _report_data=json.dumps({'data': 'valid'}),
            expires_at=datetime.now(timezone.utc) + timedelta(hours=1)
        )
        db.session.add(valid_report)

        # 2. Create one expired report cache entry
        expired_report = ReportCache(
            user_id=self.user.id,
            _report_data=json.dumps({'data': 'expired'}),
            expires_at=datetime.now(timezone.utc) - timedelta(hours=1)
        )
        db.session.add(expired_report)
        db.session.commit()

        # 3. Verify that both reports are initially in the database
        initial_count = ReportCache.query.count()
        self.assertEqual(initial_count, 2, "Should be 2 reports in the cache before cleanup")

        # 4. Run the cleanup method
        deleted_count = ReportCache.cleanup_expired()
        
        # 5. Assert that the method reported deleting one item
        self.assertEqual(deleted_count, 1, "cleanup_expired should report that it deleted 1 item")

        # 6. Assert that only one report remains in the database
        final_count = ReportCache.query.count()
        self.assertEqual(final_count, 1, "Should be 1 report in the cache after cleanup")

        # 7. Assert that the remaining report is the valid one
        remaining_report = ReportCache.query.first()
        self.assertIsNotNone(remaining_report)
        self.assertEqual(remaining_report.id, valid_report.id, "The remaining report should be the valid one")
        self.assertNotEqual(remaining_report.id, expired_report.id, "The expired report should have been deleted")

if __name__ == '__main__':
    unittest.main()