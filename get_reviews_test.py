from unittest import TestCase
from get_reviews import get_reviews_for_app



class ReviewTest(TestCase):

    def setUp(self):
        self.app_id = '1222690'
    

    def _test_list_of_reviews_not_empty(self, results: list):
        """
        Check if the list of reviews is empty
        :param results: list of reviews for self.app_id
        """
        self.assertTrue(len(results) > 0)

    def _test_item_field_is_not_none(self, results: list):
        """
        Validate some important field are not None
        :param results: list of reviews
        """
        for item in results:
            self.assertIsNotNone(item['steamid'])
            self.assertIsNotNone(item['appid'])
            self.assertIsNotNone(item['content'])
            self.assertIsNotNone(item['recommended'])
    
    def _test_recommended_field_must_contain_boolean(self, results: list):
        """
        Validate type of recommended field of review
        :param results: list of reviews
        """
        possible_values = [True, False]

        for item in results:
            self.assertTrue(item['recommended'] in possible_values)
        

    def test_get_reviews(self):
        """
        Put everything together
        """
        results = get_reviews_for_app(self.app_id)
        self._test_list_of_reviews_not_empty(results)
        self._test_item_field_is_not_none(results)
        self._test_recommended_field_must_contain_boolean(results)
        


    


        