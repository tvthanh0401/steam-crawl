import scrapy 
from scrapy_splash import SplashRequest
import re
import steam.spiders.crawl_config as crawl_config
from steam.items import SteamItem

class SteamSpider(scrapy.Spider):
    """
    Extract information (link and some other information) of game deals
    """
    name = 'steam'


    def start_requests(self):
        """
        Start splash request to render the deals page.
        """
        url = crawl_config.BASE_URL
        yield SplashRequest(url=url, callback=self.parse_max_pages, args={'wait': 1}, dont_filter=True)


    def parse_max_pages(self, response):
        """
        Extract the total page of game deals, then render each page and send it to scrapy
        :param response: html response from Splash.
        :return: None.
        """
        pages_list =  response.xpath('//*[@id="TopSellers_links"]/span/text()').extract()
        pages_list = list(map(int, pages_list))
        max_page = max(pages_list)
        print(f'max page: {max_page}')
        base_url = crawl_config.BASE_FORMATTED_URL
        urls = []


        # Generate page link from base url
        for i in range(max_page):
            urls.append(base_url.format(i))

        
        # Start render each page and then send the rendered result to scrapy 
        for url in urls:
            yield SplashRequest(url=url, callback=self.parse, args={'wait': 1})
    
    def parse(self, response):
        """
        Extract information from html response from Splash.
        :param response: html response from Splash.
        :return: None.
        """

        # Get all games/apps in sales
        apps = response.xpath('//*[@id="TopSellersRows"]/a')


        for app in apps:
            current_item = SteamItem()
            supported = set(app.xpath('./div[3]/div[2]/span/@class').extract())

            current_item['link'] = app.xpath('./@href').get()
            current_item['name'] = app.xpath('./div[3]/div[1]/text()').get()
            current_item['app id'] = re.findall(r'\b\d+\b', app.xpath('./@href').get())[0]
            current_item['support windows'] = 'platform_img win' in supported
            current_item['support mac'] = 'platform_img mac' in supported
            current_item['support linux'] = 'platform_img linux' in supported
            current_item['support vr'] = 'vr_supported' in supported

            yield current_item
            # yield {
            #     # Game/App link
            #     'link': app.xpath('./@href').get(),
            #     # Game/App name
            #     'name': app.xpath('./div[3]/div[1]/text()').get(),
            #     # Game/App id
            #     'app id': re.findall(r'\b\d+\b', app.xpath('./@href').get())[0],
            #     # Game/App support windows or not
            #     'support windows': 'platform_img win' in supported,
            #     # Game/App support mac or not
            #     'support mac': 'platform_img mac' in supported,
            #     # Game/App support linux or not
            #     'support linux': 'platform_img linux' in supported,
            #     # Game/App support vr or not
            #     'support vr': 'vr_supported' in supported,
            # }