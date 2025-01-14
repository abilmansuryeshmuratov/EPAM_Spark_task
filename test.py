import unittest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock
import restaurant_weather_etl
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col

from restaurant_weather_etl import (
    create_spark_session,
    geocode_address,
    generate_geohash,
    create_geocoding_udf,
    scan_weather_folders,
    process_restaurants,
    process_weather,
    join_data,
    save_results,
)

class TestPurePythonFunctions(unittest.TestCase):

    def test_generate_geohash_happy_path(self):
        lat, lng = 37.7749, -122.4194  # San Francisco
        expected_prefix = '9q8y'      #Expected "9q8y" for SF, precision=4
        gh = generate_geohash(lat, lng)
        self.assertIsNotNone(gh)
        self.assertTrue(gh.startswith(expected_prefix))

    def test_generate_geohash_none_values(self):
        gh = generate_geohash(None, None)
        self.assertIsNone(gh)

    @patch('restaurant_weather_etl.requests.get')
    def test_geocode_address_successful(self, mock_requests_get):
        # Mock JSON response from OpenCage
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'status': {'code': 200},
            'results': [{
                'confidence': 8,
                'geometry': {'lat': 40.7128, 'lng': -74.0060}  # NYC
            }]
        }
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response

        lat, lng = geocode_address("New York City, USA", "fake_key")
        self.assertEqual(lat, 40.7128)
        self.assertEqual(lng, -74.0060)

    @patch('restaurant_weather_etl.requests.get')
    def test_geocode_address_low_confidence(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'status': {'code': 200},
            'results': [{
                'confidence': 3,
                'geometry': {'lat': 40.7128, 'lng': -74.0060}
            }]
        }
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response

        lat, lng = geocode_address("Somewhere with low confidence", "fake_key")
        self.assertIsNone(lat)
        self.assertIsNone(lng)

    @patch('restaurant_weather_etl.requests.get')
    def test_geocode_address_no_results(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'status': {'code': 200},
            'results': []
        }
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response

        lat, lng = geocode_address("No results land", "fake_key")
        self.assertIsNone(lat)
        self.assertIsNone(lng)

    @patch('restaurant_weather_etl.requests.get')
    def test_geocode_address_api_error(self, mock_requests_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            'status': {'code': 400, 'message': 'Bad request'}
        }
        mock_response.raise_for_status.side_effect = Exception("400 Client Error")
        mock_requests_get.return_value = mock_response

        lat, lng = geocode_address("Error land", "fake_key")
        self.assertIsNone(lat)
        self.assertIsNone(lng)

    def test_scan_weather_folders(self):
        """
        Temporary folder structure like the one made by zip_unpacker:
            tmp_dir/
              year=2023/
                  month=10/
                      day=05/
                          data.parquet
                      day=06/
                          data.parquet
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            year_dir = os.path.join(tmp_dir, "year=2023")
            month_dir = os.path.join(year_dir, "month=10")
            day_dir_1 = os.path.join(month_dir, "day=05")
            day_dir_2 = os.path.join(month_dir, "day=06")

            os.makedirs(day_dir_1)
            os.makedirs(day_dir_2)

            # Create dummy parquet files
            open(os.path.join(day_dir_1, "data1.parquet"), 'a').close()
            open(os.path.join(day_dir_2, "data2.parquet"), 'a').close()

            result = scan_weather_folders(tmp_dir)
            self.assertEqual(result, [(2023, 10, 5), (2023, 10, 6)])


class TestSparkFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch('restaurant_weather_etl.geocode_address', return_value=(40.0, -75.0))
    def test_process_restaurants(self, mock_geocode_address):
        
        # Test process_restaurants() by creating a small CSV file in a temp folder,
        # passing it in, and verifying the resulting DataFrame.
        # geocode_address() returns (40.0, -75.0) for any address.
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a CSV with two restaurant records
            csv_path = os.path.join(tmp_dir, "restaurants.csv")
            with open(csv_path, 'w') as f:
                f.write("id,city,country,lat,lng\n")
                f.write("1,New York,USA,,\n")  # Missing lat/lng
                f.write("2,Toronto,Canada,43.6532,-79.3832\n")  # Has lat/lng

            # Call the real process_restaurants() – the actual geocode_address
            # no HTTP call
            processed_df = process_restaurants(
                spark=self.spark,
                input_path=csv_path,
                api_key="fake_api_key"
            )

            results = processed_df.orderBy("id").collect()

            # Row 1 (New York) should be geocoded to (40.0, -75.0)
            self.assertEqual(results[0]["id"], 1)
            self.assertAlmostEqual(results[0]["lat"], 40.0, places=3)
            self.assertAlmostEqual(results[0]["lng"], -75.0, places=3)
            self.assertIsNotNone(results[0]["geohash"])

            # Row 2 (Toronto) must keep existing coords
            self.assertEqual(results[1]["id"], 2)
            self.assertAlmostEqual(results[1]["lat"], 43.6532, places=3)
            self.assertAlmostEqual(results[1]["lng"], -79.3832, places=3)
            self.assertIsNotNone(results[1]["geohash"])

    def test_process_weather(self):
        
        # Test process_weather() by creating a small Parquet dataset in a temp folder,
        # passing it in, and verifying the resulting DataFrame.
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a small in-memory weather DataFrame
            weather_data = [
                Row(weather_lat=40.0, weather_lng=-75.0, temperature=25.0),
                Row(weather_lat=43.6532, weather_lng=-79.3832, temperature=20.0),
            ]
            df_weather = self.spark.createDataFrame(weather_data)

            # Write it to parquet in the temp folder
            parquet_path = os.path.join(tmp_dir, "weather.parquet")
            df_weather.write.parquet(parquet_path)

            # Call process_weather() with that folder
            processed_weather_df = process_weather(
                spark=self.spark,
                weather_path=parquet_path
            )

            results = processed_weather_df.orderBy("temperature").collect()
            # Geohash expected to exist
            self.assertTrue("geohash" in processed_weather_df.columns)

            # Check one row
            row1 = results[0]  # Should be temperature=20.0 (Toronto)
            self.assertAlmostEqual(row1["weather_lat"], 43.6532, places=3)
            self.assertAlmostEqual(row1["weather_lng"], -79.3832, places=3)
            self.assertIsNotNone(row1["geohash"])

            row2 = results[1]  # temperature=25.0
            self.assertAlmostEqual(row2["weather_lat"], 40.0, places=3)
            self.assertAlmostEqual(row2["weather_lng"], -75.0, places=3)

    def test_join_data(self):
    
        #Test the join_data() function by creating small DataFrames for restaurants & weather,
        #ensuring they join on geohash.
        
        # Create in-memory restaurants DF
        restaurants_data = [
            Row(id=1, name="R1", lat=40.0, lng=-75.0, geohash="dr5r"),   # Example geohash near NYC
            Row(id=2, name="R2", lat=43.6532, lng=-79.3832, geohash="dpz8"),
        ]
        restaurants_df = self.spark.createDataFrame(restaurants_data)

        # Create in-memory weather DF with matching geohash
        weather_data = [
            Row(weather_lat=40.0, weather_lng=-75.0, geohash="dr5r", temperature=25.0),
            Row(weather_lat=43.6532, weather_lng=-79.3832, geohash="dpz8", temperature=20.0),
        ]
        weather_df = self.spark.createDataFrame(weather_data)

        joined_df = join_data(restaurants_df, weather_df)
        self.assertTrue("temperature" in joined_df.columns)

        # Check joined data
        results = joined_df.orderBy("id").collect()
        self.assertEqual(results[0]["temperature"], 25.0)  # Matches the first row
        self.assertEqual(results[1]["temperature"], 20.0)  # Matches the second row

    def test_save_results(self):
        
        #Test save_results() by writing a small DataFrame to a temp directory
        # Checking if partitions exist.
    
        df_data = [
            Row(id=1, geohash="aaaa", value="v1"),
            Row(id=2, geohash="bbbb", value="v2"),
        ]
        test_df = self.spark.createDataFrame(df_data)

        with tempfile.TemporaryDirectory() as tmp_dir:
            save_results(test_df, tmp_dir)

            # Expect partitions like geohash=aaaa, geohash=bbbb
            partition_aaaa = os.path.join(tmp_dir, "geohash=aaaa")
            partition_bbbb = os.path.join(tmp_dir, "geohash=bbbb")

            self.assertTrue(os.path.exists(partition_aaaa))
            self.assertTrue(os.path.exists(partition_bbbb))


if __name__ == '__main__':
    unittest.main()
