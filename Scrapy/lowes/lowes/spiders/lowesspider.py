import scrapy
import json
import numpy as np
import math
from lowes.items import LowesItem


class LowesspiderSpider(scrapy.Spider):
    name = "lowesspider"
    allowed_domains = ["lowes.com"]
    start_urls = ["https://www.lowes.com"]
    base_url = "https://www.lowes.com/pl/fall-decorations/fall-wreaths-garland/sullivans/1614047588"
    # base_url = 'https://www.lowes.com/pl/bathroom-accessories-hardware/shower-curtains-rods/4294639614'
    cookies = {}

    def parse(self, response):
        """
        Extract cookies and then initiate the first request to get the item count.
        """
        self.extract_cookies(response)

        yield scrapy.Request(self.base_url + "/products?offset=1&selectedStoreNumber=3284&ac=false&algoRulesAppliedInPageLoad=true", callback=self.total_item_count, cookies=self.cookies)

    def extract_cookies(self, response):
        """
        Extract cookies from the response headers and store them in self.cookies.
        """
        response_headers = response.headers.to_unicode_dict().get('Set-Cookie', '')
        for arr in response_headers.split(','):
            if '=' in arr.split(';')[0]:
                keyVal = arr.split(';')[0].split('=')
                self.cookies[keyVal[0]] = keyVal[1]

    def total_item_count(self, response):
        """
        Fetch total item count and create requests for each page.
        """
        jsonresponse = json.loads(response.text)
        itemCount = jsonresponse.get('itemCount', 0)
        total_items_per_offset = len(jsonresponse.get('itemList', itemCount))
        total_pages = math.ceil(itemCount / total_items_per_offset)
        for pageNo in range(total_pages):
            yield scrapy.Request(self.base_url + f"/products?offset={pageNo*total_items_per_offset}&selectedStoreNumber=3284&ac=false&algoRulesAppliedInPageLoad=true", callback=self.page_data_parse, cookies=self.cookies)

    def safe_value(self, dictionary, key_path, default_value='NA'):
        """
        Safely extracts a value from a nested dictionary using a list of keys.
        If any key is not found, return default_value.
        """
        try:
            for key in key_path:
                dictionary = dictionary[key]
            return dictionary
        except:
            return default_value

    def page_data_parse(self, response):
        """
        Parse the product data from the page response.
        """
        jsonresponse = json.loads(response.text)
        itemList = jsonresponse['itemList']
        lowes_item = LowesItem()
        for item in itemList:
            # yield {
            #     'Brand': self.safe_value(item, ['product', 'brand']),
            #     'ModelId': self.safe_value(item, ['product', 'modelId']),
            #     'Price': self.safe_value(item, ['location', 'price', 'sellingPrice'], self.safe_value(item, ['location', 'price', 'minPrice'])),
            #     'ProductURL': 'https://www.lowes.com' + self.safe_value(item, ['product', 'pdURL']),
            #     'Description': self.safe_value(item, ['product', 'description'])
            # }
            lowes_item['Brand'] = self.safe_value(item, ['product', 'brand']),
            lowes_item['ModelId'] = self.safe_value(item, ['product', 'modelId']),
            lowes_item['Price'] = self.safe_value(item, ['location', 'price', 'sellingPrice'], self.safe_value(item, ['location', 'price', 'minPrice'])),
            lowes_item['ProductURL'] = 'https://www.lowes.com' + self.safe_value(item, ['product', 'pdURL']),
            lowes_item['Description'] = self.safe_value(item, ['product', 'description'])

            yield lowes_item

        self.extract_cookies(response)

