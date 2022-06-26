# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from os import scandir
import scrapy


class SteamItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()

    def __init__(self):
        super().__init__()
        self.fields['link'] = scrapy.Field()
        self.fields['name'] = scrapy.Field()
        self.fields['app id'] = scrapy.Field()
        self.fields['support windows'] = scrapy.Field()
        self.fields['support mac'] = scrapy.Field()
        self.fields['support linux'] = scrapy.Field()
        self.fields['support vr'] = scrapy.Field()



class GameInfoItem(scrapy.Item):


    def __init__(self):
        super().__init__()
        self.fields['name'] = scrapy.Field()
        self.fields['link'] = scrapy.Field()
        self.fields['release date'] = scrapy.Field()
        self.fields['tag'] = scrapy.Field()
        self.fields['category'] = scrapy.Field()
        self.fields['developer'] = scrapy.Field()
        self.fields['review']  = scrapy.Field()
        self.fields['recent review'] = scrapy.Field()
        self.fields['original price'] = scrapy.Field()
        self.fields['discounted price']  = scrapy.Field()
        self.fields['offer ends']  = scrapy.Field()
        self.fields['timestamp'] = scrapy.Field()