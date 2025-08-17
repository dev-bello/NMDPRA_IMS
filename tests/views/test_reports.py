import unittest
from datetime import datetime
from app.report.views import get_quarterly_dates, get_yearly_dates

class TestReportDateLogic(unittest.TestCase):

    def test_get_quarterly_dates(self):
        """
        Test the get_quarterly_dates function for all four quarters.
        """
        # Test Q1
        start, end = get_quarterly_dates(2023, 1)
        self.assertEqual(start, datetime(2023, 1, 1))
        self.assertEqual(end, datetime(2023, 3, 31))

        # Test Q2
        start, end = get_quarterly_dates(2023, 2)
        self.assertEqual(start, datetime(2023, 4, 1))
        self.assertEqual(end, datetime(2023, 6, 30))

        # Test Q3
        start, end = get_quarterly_dates(2023, 3)
        self.assertEqual(start, datetime(2023, 7, 1))
        self.assertEqual(end, datetime(2023, 9, 30))

        # Test Q4
        start, end = get_quarterly_dates(2023, 4)
        self.assertEqual(start, datetime(2023, 10, 1))
        self.assertEqual(end, datetime(2023, 12, 31))

    def test_get_yearly_dates(self):
        """
        Test the get_yearly_dates function.
        """
        start, end = get_yearly_dates(2023)
        self.assertEqual(start, datetime(2023, 1, 1))
        self.assertEqual(end, datetime(2023, 12, 31))

    def test_get_yearly_dates_leap_year(self):
        """
        Test the get_yearly_dates function for a leap year.
        """
        start, end = get_yearly_dates(2024)
        self.assertEqual(start, datetime(2024, 1, 1))
        self.assertEqual(end, datetime(2024, 12, 31))
        # The end date is still the 31st, the leap day is in Feb.
        # This test mainly ensures it handles the year correctly.

if __name__ == '__main__':
    unittest.main()