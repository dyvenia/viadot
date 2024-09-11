import unittest
from datetime import datetime
from your_module import VidClub, ValidationError

class TestVidClub(unittest.TestCase):
    def setUp(self):
        """Setup VidClub instance before each test."""
        # Sample input data for the constructor
        self.vid_club = VidClub(
            source="jobs",
            vid_club_credentials={"token": "test-token"}
        )

    def test_build_query(self):
        """Test correct URL generation for the 'jobs' endpoint."""
        # Sample input data for the build_query method
        from_date = "2023-01-01"
        to_date = "2023-01-31"
        api_url = "https://example.com/api/"
        items_per_page = 50
        source = "jobs"
        region = "pl"

        # Expected result URL
        expected_url = (
            "https://example.com/api/jobs?from=2023-01-01&to=2023-01-31&region=pl&limit=50"
        )

        # Check if the method returns the correct URL
        result_url = self.vid_club.build_query(from_date, to_date, api_url, items_per_page, source, region)
        self.assertEqual(result_url.strip(), expected_url.strip())
        
    def test_intervals(self):
        """Test breaking date range into intervals based on the days_interval."""
        # Sample input data for the intervals method
        from_date = "2023-01-01"
        to_date = "2023-01-15"
        days_interval = 5

        # Expected starts and ends lists
        expected_starts = ["2023-01-01", "2023-01-06", "2023-01-11"]
        expected_ends = ["2023-01-06", "2023-01-11", "2023-01-15"]

        # Check if the method returns correct intervals
        starts, ends = self.vid_club.intervals(from_date, to_date, days_interval)
        self.assertEqual(starts, expected_starts)
        self.assertEqual(ends, expected_ends)

    def test_intervals_invalid_date_range(self):
        """Test that ValidationError is raised when to_date is before from_date."""
        # Sample input data where to_date is before from_date
        from_date = "2023-01-15"
        to_date = "2023-01-01"
        days_interval = 5

        # Check if ValidationError is raised
        with self.assertRaises(ValidationError):
            self.vid_club.intervals(from_date, to_date, days_interval)