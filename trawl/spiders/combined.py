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

    def __init__(self):
        self.reset_status()

    def page_request(self, url):
        url_page = url.split("&page=")[0] + f"&page={self.current_page}"
        print(f"page_request.url_page: {url_page}")
        return scrapy.Request(url=url_page, callback=self.parse_shelf)

    def set_status(self, response):
        try:
            status = self.response_get(response, "//*[@id='infiniteStatus']//text()").split()
            got_page, total = (int(status[i]) for i in [0, 2])
            print(f"got_page: {got_page}\ntotal:{total}")
            self.books_got += got_page
            self.current_page += 1
            self.next_page = self.books_got < total
            print(f"self.next_page: {self.next_page}")
        except AttributeError or NameError:  # accidentally got extra page
            CloseSpider("no more books on shelf")

    def reset_status(self):
        self.books_got = 0
        self.current_page = 1
        self.next_page = True

    def get_shelf_urls(self, response):
        return response.xpath(
            f"//*[@id='paginatedShelfList']//*[re:test(@title, '{self.shelf_names_re}')]/@href"
        ).getall()

    def parse(self, response):
        print(f"get_shelf_urls: {self.get_shelf_urls(response)}")
        for shelf_url in self.get_shelf_urls(response):
            print(f"\n\nshelf_url: {shelf_url}")
            next_page = response.urljoin(shelf_url)
            print(f"next_page url: {next_page}")
            yield scrapy.Request(next_page, callback=self.parse_shelf)

    def parse_shelf(self, response_shelf):
        print(f"response_shelf.url: {response_shelf.url}")
        for book in response_shelf.xpath(
            "//*[@id='booksBody']//*[@class='bookalike review']"
        ):
            yield self.parse_book(book, response_shelf.url)

        self.set_status(response_shelf)
        print(f"fetched {self.books_got}")
        if self.next_page:
            yield self.page_request(response_shelf.url)
        else:
            self.reset_status()

    def parse_book(self, response_book, shelf_url):
        book_xpath = curry(self.response_get)(response_book)

        # # is there review text?
        # if not response_book.xpath(
        #     ".//*[@class='field actions']//*[text()='view (with text)']/@href"
        # ):
        #     review = {}
        # else:
        #     review = {
        #         "review_text": self.response_get(
        #             response_review,
        #             "//*[@itemprop='reviewBody']//text()"
        #         )
        #     }


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
            ),
            "review_text": book_xpath(
                "//*[re:test(@id, 'freeTextreview*')]//text()"
            ),
            "shelf_url": shelf_url
        }

