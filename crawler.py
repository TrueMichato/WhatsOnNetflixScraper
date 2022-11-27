import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
import scrapy_useragents
import pandas as pd
import csv

# scrapy runspider src/data_enrichment/utils/test_crawler.py


class HtmlSpider(scrapy.Spider):
    """
    """
    def __init__(self, *args, **kwargs):
        super(HtmlSpider, self).__init__(*args, **kwargs)
        self.start_urls = kwargs.get('start_urls', [])
        self.output_callback = kwargs.get('callback', [])
        self.result = []
    name = "Crawl me a river"

    def start_requests(self):
        down_mids = {*self.settings.getdict("DOWNLOADER_MIDDLEWARES")}
        for url in self.start_urls:
            if 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware' in down_mids:
                request = scrapy.Request(url=url,
                                         callback=self.parse)
            else:
                request = scrapy.Request(url=url, callback=self.parse)
            yield request

    def parse(self, response, **kwargs):
        address = response.url
        result = {"url": address, "status": response.status, "html": response.body}
        self.result.append(result)
        return result

    def close(self, spider, reason):
        self.output_callback(self.result)

#
# process = CrawlerProcess(settings={
#     "DOWNLOADER_MIDDLEWARES": {
#         'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
#         'scrapy_useragents.downloadermiddlewares.useragents.UserAgentsMiddleware': 500, },
#     "USER_AGENTS": [
#         ('Mozilla/5.0 (X11; Linux x86_64) '
#          'AppleWebKit/537.36 (KHTML, like Gecko) '
#          'Chrome/57.0.2987.110 '
#          'Safari/537.36'),  # chrome
#         ('Mozilla/5.0 (X11; Linux x86_64) '
#          'AppleWebKit/537.36 (KHTML, like Gecko) '
#          'Chrome/61.0.3163.79 '
#          'Safari/537.36'),  # chrome
#         ('Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) '
#          'Gecko/20100101 '
#          'Firefox/55.0'),  # firefox
#         ('Mozilla/5.0 (X11; Linux x86_64) '
#          'AppleWebKit/537.36 (KHTML, like Gecko) '
#          'Chrome/61.0.3163.91 '
#          'Safari/537.36'),  # chrome
#         ('Mozilla/5.0 (X11; Linux x86_64) '
#          'AppleWebKit/537.36 (KHTML, like Gecko) '
#          'Chrome/62.0.3202.89 '
#          'Safari/537.36'),  # chrome
#         ('Mozilla/5.0 (X11; Linux x86_64) '
#          'AppleWebKit/537.36 (KHTML, like Gecko) '
#          'Chrome/63.0.3239.108 '
#          'Safari/537.36'),  # chrome
#      ],
#     'DOWNLOAD_DELAY': 3,
# })
#
# if __name__ == '__main__':
# process.crawl(HtmlSpider, start_urls=['https://www.marksandspencer.com/webapp/wcs/stores/servlet/MSAgeCaptureCmd',
#                                           'https://www.marksandspencer.com/stores/woodley-reading-1126',
#                                           'https://www.marksandspencer.com/stores/newport-retail-park-1191',
#                                           'https://www.marksandspencer.com/webapp/wcs/stores/servlet/Logoff',
#                                           'https://www.marksandspencer.com/webapp/wcs/stores/servlet/OrderProcess',
#                                           'https://www.marksandspencer.com/webapp/wcs/stores/servlet/MSOrderTrackingFormCmd',
#                                           'https://www.marksandspencer.com/stores/cheltenham-2121',
#                                           'https://www.marksandspencer.com/MSSecureBasketDisplay',
#                                           'https://www.marksandspencer.com/webapp/wcs/stores/servlet/MSOrderDetailsDisplayCmd'])
#     process.start()  # the script will block here until the crawling is finished
