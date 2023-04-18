from toolz import curry
from .spider import TrawlSpider


class BookSpider(TrawlSpider):
    name = "book"
    start_urls = ["https://www.goodreads.com/book/show/17317376-the-theft-of-sunlight"]

    def _review_star_count(self, response):
        stars = response.xpath(
            "//*[@class='RatingsHistogram__labelTotal']//text()"
        ).getall()  # returns five star rating counts (list)

        # a star rating count is stored as "1,456 (51%)", descending from 5 stars to 1
        # reversed passes from 0-4, but we want it as 1-5
        star_count = {}
        for rating, count in enumerate(reversed(stars)):
            star_count[f"count_{rating + 1}star"] = self.strip_convert(
                count.split(" ")[0], is_int=True  # discard proportion
            )

        return star_count

    def parse_rating(self, response):
        def _get_rating(xpath_str, **kwargs):
            xpath = "//*[@class='RatingStatistics']" + xpath_str
            return self.response_get(response, xpath, **kwargs)

        return {
            **{
                "mean_rating": _get_rating(
                    "//*[@class='RatingStatistics__rating']//text()", is_int=True
                ),
                "num_rating": _get_rating(
                    "//*[@data-testid='ratingsCount']//text()", is_int=True
                ),
                "num_review": _get_rating(
                    "//*[@data-testid='reviewsCount']//text()", is_float=True
                )
            },
            **self._review_star_count(response)
        }

    def parse(self, response):
        book_xpath = curry(self.response_get)(response)

        def _get_summary(xpath_str):
            xpath = xpath_str + "//*[@class='DetailsLayoutRightParagraph__widthConstrained']//text()"
            return book_xpath(xpath, getall=True, concat=True)

        return {
            **{
                "title": book_xpath(
                    "//*[@class='BookPageTitleSection']//div//text()", getall=True
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
                # TODO: needs rstrip
                "page_count": book_xpath(
                    "//*[@data-testid='pagesFormat']//text()"
                ),
                ## TODO: remove list elements "...more"
                "shelves": book_xpath(
                    "//*[@class='Button Button--tag-inline Button--small']//text()"
                ),
                "summary_book": _get_summary(
                    "//*[@class='BookPageMetadataSection__description']"
                ),
                "summary_author": _get_summary(
                    "//*[@class='PageSection']"
                )
            },
            **self.parse_rating(response)
        }
