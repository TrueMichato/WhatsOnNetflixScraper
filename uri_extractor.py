from scrapy.crawler import CrawlerProcess
from crawler import HtmlSpider
import sys
from datetime import datetime, date
import logging


settings = {
    "USER_AGENTS": [
        ('Mozilla/5.0 (X11; Linux x86_64) '
         'AppleWebKit/537.36 (KHTML, like Gecko) '
         'Chrome/57.0.2987.110 '
         'Safari/537.36'),  # chrome
        ('Mozilla/5.0 (X11; Linux x86_64) '
         'AppleWebKit/537.36 (KHTML, like Gecko) '
         'Chrome/61.0.3163.79 '
         'Safari/537.36'),  # chrome
        ('Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) '
         'Gecko/20100101 '
         'Firefox/55.0'),  # firefox
        ('Mozilla/5.0 (X11; Linux x86_64) '
         'AppleWebKit/537.36 (KHTML, like Gecko) '
         'Chrome/61.0.3163.91 '
         'Safari/537.36'),  # chrome
        ('Mozilla/5.0 (X11; Linux x86_64) '
         'AppleWebKit/537.36 (KHTML, like Gecko) '
         'Chrome/62.0.3202.89 '
         'Safari/537.36'),  # chrome
        ('Mozilla/5.0 (X11; Linux x86_64) '
         'AppleWebKit/537.36 (KHTML, like Gecko) '
         'Chrome/90.0.3239.108 '
         'Safari/537.36'),  # chrome
     ],
    'DOWNLOAD_DELAY': 0.2,
    'CONCURRENT_REQUESTS': 200,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 200,
    'CONCURRENT_ITEMS': 200,
    'DOWNLOAD_TIMEOUT': 60,
    'LOG_LEVEL': 'INFO',
    'RETRY_ENABLED': False,
    'REACTOR_THREADPOOL_MAXSIZE': 30,
    'HTTPERROR_ALLOWED_CODES': [305, 400, 401, 403, 404, 405, 406, 407]
}


class CustomCrawler:

    def __init__(self, **kwargs):
        self.output = None
        self.process = CrawlerProcess(settings=settings)
        self.start_urls = kwargs.get('start_urls', [])

    def yield_output(self, data):
        self.output = data

    def crawl(self, cls, urls):
        self.process.crawl(cls, callback=self.yield_output, start_urls=urls)
        self.process.start()


class UriExtractor:
    """
    """

    def __init__(self, urls, *args, **kwargs):
        logging.basicConfig(filename="log.txt", filemode='w', level=logging.DEBUG)
        self.urls = urls
        self.results = {}

    def run(self, **kwargs):
        try:
            run_start = datetime.utcnow()
            logging.info(f"Run began at {run_start}")
            logging.info(f"Crawler initiated with {len(self.urls)} to crawl")
            start_crawl = datetime.utcnow()
            results = self.crawl_static(HtmlSpider, self.urls)
            failed_responses = [result['status'] for result in results if result['status'] in settings['HTTPERROR_ALLOWED_CODES']]
            successful_responses = [result['status'] for result in results if result['status'] not in settings['HTTPERROR_ALLOWED_CODES']]
            if len(failed_responses) >= len(successful_responses):
                logging.warning(f"Crawling got {len(failed_responses)} fails out of "
                                f"{len(failed_responses) + len(successful_responses)} responses.")
            end_crawl = datetime.utcnow()
            crawler_time = end_crawl - start_crawl
            logging.info(f"Finished crawling {len(results)} urls with {len(successful_responses)} results, crawling "
                         f"took {crawler_time} seconds")
            for result in results:
                if not result['status'] in settings['HTTPERROR_ALLOWED_CODES']:
                    self.results[(result['url'], self.get_date_from_url(result['url']))] = result['html']
            return self.results
        except Exception as e:
            t, v, tb = sys.exc_info()
            logging.warning(f"Crawling got following exception and failed:\n {e}\n traceback: \n {t(v).with_traceback(tb)}")
            raise t(v).with_traceback(tb)

    @staticmethod
    def crawl_static(cls, urls):
        crawler = CustomCrawler()
        crawler.crawl(cls, urls=urls)
        return crawler.output

    @staticmethod
    def get_date_from_url(url):
        date_parts = url.split("=")[1]
        return date(int(date_parts.split("%2F")[2]), int(date_parts.split("%2F")[0]), int(date_parts.split("%2F")[1]))

    def handle_error(self, error, **kwargs):
        pass


if __name__ == '__main__':
    UriExtractor(["https://www.whats-on-netflix.com/most-popular/?dateselect=23%2F11%2F2022"]).run()
