from time import time
import scrapy
from scrapy_splash import SplashRequest
import re
import json
import steam.spiders.crawl_config as crawl_config

from steam.items import GameInfoItem

class GameinfoSpider(scrapy.Spider):
    """
    Crawl all information about game deal.
    """
    name = 'gameinfo'

    # Bypass Steam Age Check script.
    bypass_cookies_script = """
    function main(splash, args)
    assert(splash:go(args.url))
    assert(splash:wait(1))
    splash.images_enable = false
    assert(splash:runjs([[
        if(document.getElementById('ageYear') != null)
        {
            btn = document.getElementById('view_product_page_btn');
            document.getElementById('ageYear').value = 1970;
            btn.click();
        }
        ]]))
    assert(splash:wait(3))
    return {
        html = splash:html()
    }
    end
    """


    def start_requests(self):
        """
        Read link from crawl_config.LINK_FILE then send it to Splash.
        Splash will render the html and finally send it to scrapy by using
        callback argument.
        """
        link_file = open(crawl_config.LINK_FILE)
        links = json.load(link_file)
        for app in links:
            yield SplashRequest(url=app['link'], callback=self.parse, args={
                'lua_source': self.bypass_cookies_script
            }, endpoint='execute')

    
    
    def parse(self, response):
        """
        Extract information from html response from Splash.
        :param response: html response from Splash.
        :return: None.
        """
        script = response.xpath('//div[@class="game_area_purchase_game"]//script/text()').get() 
        if script:
            timestamp = re.findall(r'\b\d+\b', script)[0]
        else:
            timestamp = None
        app_name = response.xpath('//*[@id="appHubAppName"]/text()').get()
        if not app_name:
            app_name = response.xpath('//h2[@class="pageheader"]/text()').get()

        current_item = GameInfoItem()

        current_item['name'] = app_name
        current_item['link'] = response.url
        current_item['release date'] = response.xpath('//div[@class="release_date"]/div[2]/text()').get()
        current_item['tag'] = list(map(str.strip, response.xpath('//div[@class="glance_tags popular_tags"]/a/text()').extract()))
        current_item['category'] = response.xpath('//div[@class="game_area_features_list_ctn"]/a/div/text()').extract()
        current_item['developer'] = response.xpath('//div[@id="developers_list"]/a/text()').get()
        current_item['review'] = response.xpath('//*[@id="userReviews"]/div[2]/div[2]/span[1]/text()').get()
        current_item['recent review'] = response.xpath('//*[@id="userReviews"]/div[1]/div[2]/span[1]/text()').get()
        current_item['original price'] = response.xpath('//div[@class="discount_prices"]/div[1]/text()').get()
        current_item['discounted price'] = response.xpath('//div[@class="discount_prices"]/div[2]/text()').get()
        current_item['offer ends'] = response.xpath('//p[@class="game_purchase_discount_countdown"]/text()').get()
        current_item['timestamp'] = timestamp

        yield current_item

        # yield {
        #     # Game/App name
        #     'name': app_name,
        #     # Game/App link
        #     'link': response.url,
        #     # Game/App release date
        #     'release date': response.xpath('//div[@class="release_date"]/div[2]/text()').get(),
        #     # User tag
        #     'tag': list(map(str.strip, response.xpath('//div[@class="glance_tags popular_tags"]/a/text()').extract())),
        #     # Game feature
        #     'category': response.xpath('//div[@class="game_area_features_list_ctn"]/a/div/text()').extract(),
        #     # Developer
        #     'developer': response.xpath('//div[@id="developers_list"]/a/text()').get(),
        #     # Game rating based on all reviews
        #     'review': response.xpath('//*[@id="userReviews"]/div[2]/div[2]/span[1]/text()').get(),
        #     # Game rating based on recent reviews
        #     'recent review': response.xpath('//*[@id="userReviews"]/div[1]/div[2]/span[1]/text()').get(),
        #     # Original price of game
        #     'original price': response.xpath('//div[@class="discount_prices"]/div[1]/text()').get(),
        #     # Discounted price of game
        #     'discounted price': response.xpath('//div[@class="discount_prices"]/div[2]/text()').get(),
        #     # Discount expiration date
        #     'offer ends': response.xpath('//p[@class="game_purchase_discount_countdown"]/text()').get(),
        #     # Timestamp of discount expiration date in case the offer ends is null
        #     'timestamp': timestamp
        # }
