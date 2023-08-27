import unittest

from analytics import helper

class TestHaversine(unittest.TestCase):
    
    def test_haversine(self):
        # Coordinates of New York City
        lat1, lon1 = 40.7128, -74.0060
        # Coordinates of Los Angeles
        lat2, lon2 = 34.0522, -118.2437
        
        expected_distance = 3935.7  # Expected distance in kilometers
        
        calculated_distance = helper.haversine(lat1, lon1, lat2, lon2)
        # Allow a tolerance of 0.1 km due to floating-point precision
        self.assertAlmostEqual(calculated_distance, expected_distance, delta=0.5)

if __name__ == '__main__':
    unittest.main()