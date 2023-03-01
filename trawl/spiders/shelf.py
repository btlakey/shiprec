from toolz import curry
import locale
import logging


from .spider import TrawlSpider

logger = logging.getLogger('shelf_logger')
logger.STDOUT = True
locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')  # this handles commas in string numbers


class ShelfSpider(TrawlSpider):
    name = "shelf"
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

    def parse_shelf(self, response_shelf):
        for book in response_shelf.xpath(
            "//*[@id='booksBody']//*[@class='bookalike review']"
        ):
            yield self.parse_book(book)

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
