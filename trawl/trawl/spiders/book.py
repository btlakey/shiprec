import scrapy


class BookSpider(scrapy.Spider):
    name = "book"

    def get_review(self, response):
        review_url = response.xpath(
            ".//*[@class='field actions']//*[text()='view (with text)']/@href"
        )
        yield response.follow(review_url, self.parse_review)

    def parse(self, response):
        pass

    def parse_review(self, response_review):
        return self.response_get(
            response_review,
            "//*[@itemprop='reviewBody']//text()"
        )

    def get_shelves(self, response):
        return None

    def get_shelf_name(self, reponse_shelf):
        names = reponse_shelf.xpath(
            "//*[@id='header']//*[@class='h1Shelf']//text()"
        ).getall()

        # for some reason, there are multiple returns for the shelf header name
        # but they're all empty strings
        name = [n.strip() for n in names if not str.isspace(n)]
        assert len(name) == 1

        # there are some zero-space unicode characters
        return name[0].encode("ascii", "ignore").decode()

