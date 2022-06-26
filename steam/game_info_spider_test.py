from unittest import TestCase


import sys
import os
sys.path.append(os.path.dirname(__file__))

from fake_response import fake_response_from_file
from spiders.gameinfo import GameinfoSpider




class SteamSpiderTest(TestCase):

    def setUp(self):
        """
        Create a spider to start the test.
        """
        self.spider = GameinfoSpider()


    def _test_item_field_is_not_none(self, results):
        """
        Validate item field.
        :param results List of scraped items.
        """
        
        for item in results:
            self.assertIsNotNone(item['name'])
            self.assertIsNotNone(item['link'])
            self.assertIsNotNone(item['release date'])
            self.assertIsNotNone(item['original price'])
            self.assertIsNotNone(item['discounted price'])
            self.assertTrue(item['offer ends'] is not None 
            or item['timestamp'] is not None)
        
            


    def _test_item_results(self, results, expected_length):
        """
        Validate number of scraped items.
        :param results List of scraped items.
        :param expected_length Expected number of items in the results list.
        """
        self.assertEqual(len(results), expected_length)


    def test_parse(self):
        """
        Put everything together.
        """
        results = self.spider.parse(fake_response_from_file("gameinfo.html"))
        self._test_item_results(list(results), 1)


        