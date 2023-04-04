import scrapy
from .spider import TrawlSpider


class BookSpider(TrawlSpider):
    name = "review"
    start_urls = ["https://www.goodreads.com/review/list/40648422-samuraikitty?shelf=favorites"]

    def parse(self, response):
        for review_url in response.xpath(
            ".//*[@class='field actions']//*[text()='view (with text)']/@href"
        ).getall():
            review_path = response.urljoin(review_url)
            print(f"review_path: {review_path}")
            yield scrapy.Request(review_path, callback=self.parse_review)

    def parse_review(self, response_review):
        return {
            "review_text": self.response_get(
                response_review,
                "//*[@itemprop='reviewBody']//text()"
            )
        }
