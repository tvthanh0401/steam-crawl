from unittest import TestCase


from scrapy_splash import SplashRequest
from scrapy.http import Response, Request
import sys
import os
sys.path.append(os.path.dirname(__file__))

from fake_response import fake_response_from_file
from spiders.steamspider import SteamSpider
from spiders import crawl_config


EXPECTED_TOTAL_ITEM_PER_PAGE = 15

class SteamSpiderTest(TestCase):

    def setUp(self):
        """
        Create a spider to start the test.
        """
        self.spider = SteamSpider()

    def _test_item_field_is_not_none(self, results):
        """
        Validate item field.
        :param results List of scraped items.
        """
        boolean_values = [True, False]
        
        for item in results:
            self.assertIsNotNone(item['name'])
            self.assertIsNotNone(item['link'])
            self.assertIsNotNone(item['app id'])
            assert(item['support windows'] in boolean_values)
            assert(item['support mac'] in boolean_values)
            assert(item['support linux'] in boolean_values)
            assert(item['support vr'] in boolean_values)


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
        results = self.spider.parse(fake_response_from_file("test.html"))
        self._test_item_results(list(results), EXPECTED_TOTAL_ITEM_PER_PAGE)
        self._test_item_field_is_not_none(list(results))


        