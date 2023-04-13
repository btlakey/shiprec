from toolz import curry
from .spider import TrawlSpider
from scrapy.exceptions import CloseSpider
import scrapy
import logging

logger = logging.getLogger('logger')
logger.STDOUT = True


class ReaderSpider(TrawlSpider):
    name = "reader"
    start_urls = ["https://www.goodreads.com/review/list/40648422"]

    def __init__(self):
        self.reset_status()

    def reset_status(self):
        """ Reset shelf counters (for each shelf crawled)
        :return: None
        """
        self.books_got = 0
        self.current_page = 1
        self.next_page = True

    def set_status(self, response):
        """ Stateful tracker of how many books have been crawled per shelf
        Will set self.next_page to False when shelf is exhausted,
          increments self.books_got, self.current_page
        Called for every shelf page

        :param response: scrapy.Response, where url=shelf_url

        :return: None
            updates self.books_got, self.current_page, self.next_page according
        """
        try:
            status = self.response_get(response, "//*[@id='infiniteStatus']//text()").split()
            got_page, total = (int(status[i]) for i in [0, 2])
            print(f"got_page: {got_page}\ntotal:{total}")
            self.books_got += got_page
            self.current_page += 1
            self.next_page = self.books_got < total
        except AttributeError or NameError:  # accidentally got extra page
            CloseSpider("no more books on shelf")

    @staticmethod
    def get_shelf_urls(response):
        """ Return a list of only the best/favorite shelves, according to some regex logic

        :param response: scrapy.Response, where url=reader_url

        :return: list(str)
            list of every shelf url that meets regex criteria
        """
        shelf_names = ["favorite", "reread", "must", "best"]
        shelf_names_re = "|".join(shelf_names)

        return response.xpath(
            f"//*[@id='paginatedShelfList']//*[re:test(@title, '{shelf_names_re}')]/@href"
        ).getall()

    def page_request(self, url):
        """ Paginate the shelf url

        :param url: current shelf url

        :return: scrapy.Request
            new Request object for next page of books on shelf
            parser = ReaderSpider.parse_shelf
        """
        # strip any &page=n that may have come from previous Reqeust
        url_page = url.split("&page=")[0] + f"&page={self.current_page}"
        return scrapy.Request(url=url_page, callback=self.parse_shelf)

    def parse(self, response):
        """ Main parse invocation; for a given reader, parse each specified shelf (self.parse_shelf)
        and return a list of book attributes (parse_book) for serialization

        :param response: scrapy.Response, where url=reader_url

        :return: Iterator(scrapy.Request)
            where url = shelf url
        """
        for shelf_url in self.get_shelf_urls(response):
            # print(f"\n\nshelf_url: {shelf_url}")
            next_page = response.urljoin(shelf_url)
            yield scrapy.Request(next_page, callback=self.parse_shelf)

    def parse_shelf(self, response_shelf):
        """ for a given shelf Response, paginate through each dynamically-loaded page and
        return all scraped books (per self.parse_book)

        :param response_shelf: scrapy.Response, where url=shelf_url

        :return: Iterator(self.parse_book, self.reset_status)
            for each book, yield return from self.parse_book, and generate next_page url until
            stopping criteria is met
        """
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
        """ For each book, return dict of book elements

        :param response_book: scrapy.Response, where element=book
        :param shelf_url: str, parent shelf_url

        :return: dict
            dictionary of scraped book elements
        """
        book_xpath = curry(self.response_get)(response_book)

        return {
            "title": book_xpath(
                "//*[@class='field title']//@title"
            ),
            "author": book_xpath(
                "//*[@class='field author']//a//text()"
            ),
            "date_pub": book_xpath(
                "//*[@class='field date_pub']//div//text()"
            ),
            "isbn13": book_xpath(
                "//*[@class='field isbn13']//div//text()", is_int=True
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
