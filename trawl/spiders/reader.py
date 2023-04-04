import locale
import logging

from trawl.spiders.spider import TrawlSpider

logger = logging.getLogger('shelf_logger')
logger.STDOUT = True
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')  # this handles commas in string numbers


class ReaderSpider(TrawlSpider):
    name = "reader"
    shelf_names = ["favorite", "reread", "must", "best"]
    shelf_names_re = "|".join(shelf_names)

    start_urls = [
        "https://www.goodreads.com/review/list/40648422"
    ]

    def parse(self, response):
        shelf_urls = response.xpath(
                f"//*[@id='paginatedShelfList']//*[re:test(@title, '{self.shelf_names_re}')]/@href"
            )
        yield from response.follow_all(shelf_urls, self.parse_shelf)