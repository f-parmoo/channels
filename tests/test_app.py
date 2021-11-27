from app import app
from unittest import TestCase
import json
import sys


def parse_response(response):
    return json.loads(response.get_data().decode(sys.getdefaultencoding()))


class TestApi(TestCase):
    def setUp(self):
        self.app = app.test_client()


    def show_channels(self, data, content_type):
        return self.app.post("/", data=json.dumps(data), content_type=content_type)

    def test_show_channels(self):
        json_body = {
            "metrics" :["impressions","clicks"],
            "breakdowns" : ["channel","country"],
            "sorting": [{"name":"clicks", "type":"desc"}],
            "filters":[{"name":"date_to","value":"2017-05-31"}]
        }
        response = parse_response(self.show_channels(data=json_body, content_type='application/json'))

        self.assertEqual(response[0]['channel'], 'adcolony')
        self.assertEqual(response[0]['country'], 'US')
        self.assertEqual(response[0]['impressions'], '498861.00')
        self.assertEqual(response[0]['clicks'],'12277.00')


