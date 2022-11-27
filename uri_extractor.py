from scrapy.crawler import CrawlerProcess
from crawler import HtmlSpider
from hashlib import md5
from io import BytesIO
import gzip
import pandas as pd
import re
import traceback
import sys
from datetime import datetime, date
import logging


settings = {
    # "DOWNLOADER_MIDDLEWARES": {
    #     'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    #     'scrapy_useragents.downloadermiddlewares.useragents.UserAgentsMiddleware': 500,
    # },
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
        # super(UriExtractor, self).__init__(*args, **kwargs)
        logging.basicConfig(filename="log.txt", filemode='w', level=logging.DEBUG)
        self.urls = urls
        self.results = {}

    def run(self, **kwargs):
        try:
            run_start = datetime.utcnow()
            logging.info(f"Run began at {run_start}")
            # settings['DOWNLOAD_DELAY'] = 1
            # settings['USER_AGENTS'] = self.get_user_agents()
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
            logging.info(f"Finished crawling {len(results)} urls with {len(successful_responses)} results")
            for result in results:
                if not result['status'] in settings['HTTPERROR_ALLOWED_CODES']:
                    self.results[(result['url'], self.get_date_from_url(result['url']))] = result['html']
            return self.results
            # # returns a list of dicts, each dict with a url and html content
            # start_upload = datetime.utcnow()
            # requests_size = 0
            # for result in results:
            #     if not result['status'] in settings['HTTPERROR_ALLOWED_CODES']:
            #         href = re.sub('/$', '', result['url'])
            #         uri = md5(href.lower().encode()).hexdigest()
            #         try:
            #             html = result['html'].decode()
            #         except UnicodeDecodeError as e:
            #             try:
            #                 html = result['html'].decode('latin-1')
            #             except UnicodeDecodeError as e:
            #                 self.logger.info(run_id=run_id, tag=tag, domain=domain, chunk_num=chunk_name.split('_')[2],
            #                                  traceback=traceback.print_exc(),
            #                                  message=f"Encountered {str(e)} error decoding {result['url']}")
            #                 html = result['html']
            #         self.zip_to_s3(path, uri, html)
            #         requests_size += len(href) + len(html)
            # end_upload = datetime.utcnow()
            # upload_time = end_upload - start_upload
            # self.logger.info(run_id=run_id, tag=tag, domain=domain, chunk_num=chunk_name.split('_')[2],
            #                  message=f"Finished inserting {len(results)} urls to S3")
            # # os.system(f'say "finished crawling {tag} {domain}, crawled {len(results)} urls"')
            # requested_urls = set(urls)
            # crawled_urls = set([d['url'] for d in results])
            # total_run_time = end_upload - run_start
            # # all_path = path + '/' + 'all_hrefs_array' + '_' + run_id + '.gzip'
            # self.log_crawl(tag, domain, run_id, chunk_name.split('_')[2], crawler_time, upload_time, total_run_time,
            #                len(requested_urls), len(crawled_urls), use_proxy, requests_size)
            # self.log_chunk(tag, domain, run_id, path)
        except Exception as e:
            t, v, tb = sys.exc_info()
            logging.warning(f"Crawling got following exception and failed:\n {e}\n traceback: \n {t(v).with_traceback(tb)}")
            raise t(v).with_traceback(tb)

    @staticmethod
    def crawl_static(cls, urls):
        crawler = CustomCrawler()
        crawler.crawl(cls, urls=urls)
        return crawler.output

    # TODO - rewrite with static examples
    def get_user_agents(self):
        # user_agents = self.rds.execute_to_flat_list(f"""
        # SELECT DISTINCT ua
        # FROM category_crawler_user_agents
        # """)
        ua = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36"]
        return ua

    @staticmethod
    def get_date_from_url(url):
        date_parts = url.split("=")[1]
        return date(int(date_parts.split("%2F")[2]), int(date_parts.split("%2F")[0]), int(date_parts.split("%2F")[1]))

    # def zip_to_s3(self, path, name, content, run_id=None):
    #     if run_id:
    #         new_path = path + '/' + name + '_' + run_id + '.gzip'
    #     else:
    #         new_path = path + '/' + name + '.gzip'
    #     out_file = BytesIO()
    #     gzip_file = gzip.GzipFile(fileobj=out_file, mode='wb')
    #     gzip_file.write(content.encode('utf-8'))
    #     gzip_file.close()
    #     content = out_file.getvalue()
    #     out_file.close()
    #     self.s3.write_file(
    #         s3_path=new_path,
    #         content=content)

    # def load_href_array_from_s3(self, path):
    #     array_file = self.s3.get_object(path)
    #     file_content = array_file['Body'].read()
    #     file_content = BytesIO(file_content)
    #     gzipfile = gzip.GzipFile(fileobj=file_content)
    #     hrefs = gzipfile.read()
    #     return hrefs

    # def log_crawl(self, tag, domain, run_id, chunk_name, crawler_time, upload_time, total_run_time,
    #               requested_urls, crawled_urls, used_proxy, requests_size):
    #     insert_df = pd.DataFrame([[tag, domain, run_id, chunk_name, crawler_time.seconds, upload_time.seconds,
    #                                total_run_time.seconds, requested_urls, crawled_urls, used_proxy, requests_size/1000]],
    #                              columns=['tag', 'domain', 'run_id', 'chunk_num', 'crawled_time',
    #                                       'upload_time', 'total_run_time', 'num_requested_urls', 'num_crawled_urls',
    #                                       'used_proxy', 'requests_size'])
    #     self.rds.insert_dataframe_to_table('category_mapping_crawler_log', insert_df)

    # def log_chunk(self, tag, domain, run_id, path_name):
    #     query = f"""UPDATE category_mapping_chunks_log
    #     SET current_chunks = current_chunks + 1 , map_status = CASE
    #                                                             WHEN current_chunks < max_chunks THEN 'Running'
    #                                                             ELSE 'Run_Done'
    #                                                             END
    #     WHERE tag = '{tag}'
    #     AND domain = '{domain}'
    #     AND run_id = '{run_id}'
    #     AND path_name = '{path_name}'"""
    #     self.rds.execute_update(query=query)

    def handle_error(self, error, **kwargs):
        pass

    # def perform_test_job(self):
    #     self.process_message(tag='BU0N42E0G',
    #                          domain="build.com",
    #                          run_id="try_test",
    #                          down_delay=5.6499999999999995,
    #                          path='mapping-automation/BU0N42E0G/build.com',
    #                          chunk_name='hrefs_array_0')

    # def insert_df_to_collection_works(self, collection_name, insert_df):
    #     if insert_df.empty:
    #         return
    #     # normalize the insert dataframe
    #     insert_df = insert_df.replace([pd.np.inf, -pd.np.inf], pd.np.nan)
    #     data = insert_df.to_dict(orient='records')  # Here's our added param..
    #     collection = self.mongo.connection[collection_name]
    #     collection.insert_many(data)
    #     return
    #
    # def update_to_collection(self, collection_name, tag, domain, uri, html):
    #     """Insert data into Mongo collection, creates a new collection if collection doesn't exist.
    #     Returns:
    #     An instance of :class:`~pymongo.results.UpdateResult`. """
    #
    #     collection = self.mongo.connection[collection_name]
    #     result = collection.update_one({"_id": tag,
    #                                     tag: {"$exists": "true"},
    #                                     },
    #                                    {'$set': {f"{domain}.{uri}": html}}, upsert=True)
    #     return result


if __name__ == '__main__':
    UriExtractor(force_queue_creation=True,
                 slack_channel='auto_mapping_crawler',
                 always_delete_message=False).perform_job()
