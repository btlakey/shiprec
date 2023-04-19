import scrapy
from toolz import curry
from .spider import TrawlSpider


class FavoriteSpider(TrawlSpider):
    name = "favorite"
    start_urls = [
        "https://www.goodreads.com/book/show/112077.The_Game_of_Kings",
        "https://www.goodreads.com/book/show/112080.Queens_Play",
        # "https://www.goodreads.com/book/show/351211.The_Disorderly_Knights",
        # "https://www.goodreads.com/book/show/360455.Pawn_in_Frankincense",
        # "https://www.goodreads.com/book/show/351198.The_Ringed_Castle",
        # "https://www.goodreads.com/book/show/112074.Checkmate",
        # "https://www.goodreads.com/book/show/210834.Kim",
        # "https://www.goodreads.com/book/show/234225.Dune",
        # "https://www.goodreads.com/book/show/865.The_Alchemist",
        # "https://www.goodreads.com/book/show/119073.The_Name_of_the_Rose",
        # "https://www.goodreads.com/book/show/77430.Master_Commander",
        # "https://www.goodreads.com/book/show/77427.H_M_S_Surprise",
        # "https://www.goodreads.com/book/show/55021.Heaven_s_Prisoners",
        # "https://www.goodreads.com/book/show/10050.Youth_in_Revolt",
        # "https://www.goodreads.com/book/show/10996342-the-art-of-fielding",
        # "https://www.goodreads.com/book/show/4395.The_Grapes_of_Wrath",
        # "https://www.goodreads.com/book/show/153747.Moby_Dick_or_the_Whale",
        # "https://www.goodreads.com/book/show/34.The_Fellowship_of_the_Ring",
        # "https://www.goodreads.com/book/show/15241.The_Two_Towers",
        # "https://www.goodreads.com/book/show/18512.The_Return_of_the_King"
    ]

    def start_url(self, url):
        return url + '/reviews?reviewFilters={"ratingMin":5,"ratingMax":5}'

    def parse(self, response):
        yield scrapy.Request(
            url=self.start_url(response.url),
            callback=self.parse_readers
        )

    def parse_readers(self, response):
        reader_xpath = curry(self.response_get)(response)
        return {
            "bookid": reader_xpath(
                "//*[@class='Text H1Title']//@href"
            ).split("/")[-1],
            "title": reader_xpath(
                "//*[@class='Text H1Title']//text()"
            ),
            "author": reader_xpath(
                "//*[@class='ContributorLink__name']//text()"
            ),
            "readers": reader_xpath(
                "//*[@class='ReviewerProfile__name']//@href", getall=True
            )
        }