import scrapy


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
