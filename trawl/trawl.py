import scrapy
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings

from trawl import BookSpider, ShelfSpider, ReaderSpider

settings = get_project_settings()
configure_logging(settings)
runner = CrawlerRunner(settings)


@defer.inlineCallbacks
def trawl():
    # this should need passed a book_id (or list) and return list of reader_ids
    reader_spider = ReaderSpider

    # this should need list of reader_ids and return list of book_ids with text reviews
    # should this be the serializer
    shelf_spider = ShelfSpider

    # this should need list of books_ids with reviews
    book_spider = BookSpider

    for spider in [reader_spider, shelf_spider, book_spider]:
        runner.crawl(spider)


if __name__ == "__main__":
    trawl()
    reactor.run()  # the script will block here until the last crawl call is finished
