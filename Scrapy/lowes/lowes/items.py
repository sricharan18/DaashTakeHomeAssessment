# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class LowesItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    Brand = scrapy.Field()
    ModelId = scrapy.Field()
    Price = scrapy.Field()
    ProductURL = scrapy.Field()
    Description = scrapy.Field()
