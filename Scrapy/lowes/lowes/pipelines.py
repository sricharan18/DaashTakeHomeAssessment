# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


class LowesPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        ## 
        value = adapter.get('Price')
        adapter['Price'] = value[0]

        return item
