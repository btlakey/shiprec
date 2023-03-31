import nest_asyncio
from toolz import curry
from .spider import TrawlSpider
from scrapy.exceptions import CloseSpider
import scrapy
import logging

logger = logging.getLogger('shelf_logger')
logger.STDOUT = True
nest_asyncio.apply()


class CombinedSpider(TrawlSpider):
    name = "combined"

    start_urls = ["https://www.goodreads.com/review/list/40648422"]
    shelf_names = ["favorite", "reread", "must", "best"]
    shelf_names_re = "|".join(shelf_names)

    next_page = True
    current_page = 1
    books_got = 0

    def page_request(self, url):
        url_page = url + f"&page={self.current_page}"
        return scrapy.Request(url=url_page, callback=self.parse)

    def set_status(self, response):
        try:
            status = self.response_get(response, "//*[@id='infiniteStatus']//text()").split()
            got_page, total = (int(status[i]) for i in [0, 2])
            self.books_got += got_page
            self.current_page += 1
            self.next_page = self.books_got <= total
        except AttributeError or NameError:  # accidentally got extra page
            CloseSpider("no more books on shelf")

    def get_shelf_urls(self, response):
        return response.xpath(
            f"//*[@id='paginatedShelfList']//*[re:test(@title, '{self.shelf_names_re}')]/@href"
        ).getall()

    def parse(self, response):
        for shelf_url in self.get_shelf_urls(response):
            next_page = response.urljoin(shelf_url)
            yield scrapy.Request(next_page, callback=self.parse_shelf)

    def parse_shelf(self, response_shelf):
        for book in response_shelf.xpath(
            "//*[@id='booksBody']//*[@class='bookalike review']"
        ):
            yield self.parse_book(book)

        self.set_status(response_shelf)
        print(f"fetched {self.books_got}")
        if self.next_page:
            yield self.page_request()

    def parse_book(self, response_book):
        # book_xpath = lambda x: response_book.xpath(x).get()
        book_xpath = curry(self.response_get)(response_book)

        return {
            "title": book_xpath(
                "//*[@class='field title']//@title"
            ),
            "isbn13": book_xpath(
                "//*[@class='field isbn13']//div//text()", is_int=True
            ),
            "author": book_xpath(
                "//*[@class='field author']//a//text()"
            ),
            "date_pub": book_xpath(
                "//*[@class='field date_pub']//div//text()"
            ),
            "mean_rating": book_xpath(
                "//*[@class='field avg_rating']//div//text()", is_float=True
            ),
            "num_rating": book_xpath(
                "//*[@class='field num_ratings']//div//text()", is_int=True
            ),
            "user_rating": book_xpath(
                "//*[@class='field rating']//*[@class=' staticStars notranslate']/@title"
            ),
            "date_read": book_xpath(
                "//*[@class='field date_read']//*[@class='date_read_value']/text()"
            ),
            "date_added": book_xpath(
                "//*[@class='field date_added']//div//@title"
            )
        }

