from toolz import curry, compose
from .spider import TrawlSpider


class BookSpider(TrawlSpider):
    name = "book"
    start_urls = ["https://www.goodreads.com/book/show/17317376-the-theft-of-sunlight"]

    def parse_rating(self, response):
        xpath_rating = lambda xpath_str: "//*[@class='RatingStatistics']" + xpath_str
        get_rating = compose(
            curry(self.response_get)(response),
            xpath_rating
        )

        def review_star_count(self, response):
            stars = response.xpath(
                "//*[@class='RatingsHistogram__labelTotal']//text()"
            ).getall()  # returns five star rating counts (list)

            # a star rating count is stored as "1,456 (51%)", descending from 5 stars to 1
            # reversed passes from 0-4, but we want it as 1-5
            star_count = {}
            for rating, count in enumerate(reversed(stars)):
                star_count[f"count_{rating+1}stars"] = self.strip_convert(
                    count.split(" ")[0]  # discard proportion
                )

            return star_count

        return {
            "mean_rating": get_rating(
                "//*[@class='RatingStatistics__rating']//text()"
            ),
            "num_rating": get_rating(
                "//*[@data-testid='ratingsCount']//text()"
            ),
            "num_review": get_rating(
                "//*[@data-testid='reviewsCount']//text()"
            )
        }

    def parse(self, response, shelf_url):

        book_xpath = curry(self.response_get)(response)

        return {
            **{
                "title": book_xpath(
                    "//*[@class='BookPageTitleSection']//div//text()"
                ),
                "author": book_xpath(
                    "//*[@class='BookPageMetadataSection__contributor']//*[@class='ContributorLink__name']//text()"
                ),
                # TODO: this one needs lstrip of "First published "
                "date_pub": book_xpath(
                    "//*[@data-testid='publicationInfo']//text()"
                ),
                ## these requires a POST cURL call to Amazon for all editions
                # "original_title",  "publisher"
                "isbn13": book_xpath(

                ),
                # TODO: needs rstrip
                "page_count": book_xpath(
                    "//*[@data-testid='pagesFormat']//text()"
                ),
                ## TODO: remove list elements "...more"
                "shelves": book_xpath(
                    "//*[@class='Button Button--tag-inline Button--small']//text()"
                )
            },
            **self.review_star_count(response)
        }
