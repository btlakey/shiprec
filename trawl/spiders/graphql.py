import scrapy
from scrapy.spiders import CrawlSpider, Rule
import json
import logging

logger = logging.getLogger('logger')
logger.STDOUT = True


class GraphSpider(CrawlSpider):
    name = 'graph'
    allowed_domains = ['goodreads.com']
    start_urls = [
        'https://www.goodreads.com/book/show/112077.The_Game_of_Kings/reviews?reviewFilters={"ratingMin":5,"ratingMax":5}'
    ]
    rules = (

    )

    def start_requests(self):

        url = 'https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql'
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://www.goodreads.com/",
            "content-type": "application/json; charset=UTF-8",
            "x-amz-date": "20230420T150307Z",
            "X-Amz-Security-Token": "IQoJb3JpZ2luX2VjEJf//////////wEaCXVzLWVhc3QtMSJHMEUCIQDv+nhUmebvxYyWjA/Z6hF0kRyd6kbRIiZ3ZwmAM5nfPgIgLCy+R/NeqlpXVsWR5ixOz8WsC7e9FLUVwkPwXQpj8goqkAYIkP//////////ARACGgw1MTgwODM5MjIxMjciDKMdm9w5KTzCR8LwhSrkBXYTh2c1nWAhA4Fd70nPtt4Tn3k8wLnMRBQ3tFebHcOhQ7jsrwIZVpiN1rjVxWPg3G0hsySLfjlLoTFDbV69hfgw5XrvNrX+WUGBr5hlJnFelcHy8ssVo3Vtc3r0qR6xht65jlgbD8xqJvHG7PAXdNFmW+Ut5eNR20qMcsqy++7FiQFRyi7HzJfzTyG74YNrb6tMyeNf52xkQsW6rHqqSU2M2WHiTJEGYxBKzyYrmqcXo0ttYQdw5DKWwvUB5LPJr8F/NfDgVVnBWRMF16WDW294x18n+yxjNWwUqPunr4oB6cT+Te/qjQzNQZ+2WSt9zskDipgcxKnQp1mMQbNv58bZwNWim6kztluWsl1gxnTY/SoDYYX6UdxUGIZYdMKkeRpJk7+F9wv3eWGHXrMBOfTrKOrSkQTUknjglaLgclPoJy87wDjO+tjbn5CeqpQKKuiqyV4sLgp3gVqXYldXnJbesOeJI1L4dUFzDagp++attCqa3VKA597dxf2Nj0UueKOBU3Hu4d5GBHI5w1NB010396sS6QFIlANNYwUT7Or7Dv+vlAnd+3VScmo2MQYtKsyZxyG5Je8SPV4eyW0DX8nN5KV5IujHbt96UxSFCAfhYqyfZuhyjHENrxjx9Eo/Fo3pjCbkoP1QBE7yb9MObGFWmMdKx8F/XurIa3r3sWrLYziOXyImQnW25k9WdENDweLKDf9zGBnBM9tuPzmTVShpMc8Ybm4rBfq3xa5Js6icyzm0P+pGu7Vj6IvWZQBIWmEQDCqyWGeIZWqW8MbYFvqPxn0oiEhE/aAu4YcJcCjPGTxpz4S+Hhxdued1nqyl7rlxyyFgPi03PCJTwakS2NCp1OQ7V14g67TdPO6vmY6KTOkvUTTs8p1Yte/eEpRM8CBcGnhsMr9ehZpoO209NpjKYXBaA7MGOBEShLogOCUjWxWJ8VHG248c1KS/fH5IWSJgfruqmh+rGQ0FLJriaa0zdQ/vMKuohaIGOocCkk9mM5L4GbJT2hurZzG8k2dU78rtywLaywV864+2OYgwU5CZQD7pCtxTVxv/VpC/Ne6HUdvVaeABpnqeHrcBm3194790fCAd/JnbTirJiJNPdOd29migvIqXxtQIFmr33daE3/DdlV2IRwkBTrUYQZ2Jp79Z1WO3mdCoF2IIivZDCHJbgV5eqTCI579k/h6GL3z0BV3l4xj6JwPLvc6jRfjcIbY8Vdv1Of4NVcV4acI8aRQZUBi5Hf7sn99hg5n1JIb+zjTj7cCGn3SXxA2rSqokqjPbVEM9onhR4/OvImh8vuS+ifGuaTqB25IwAil3AxXKz06uWe7Js3p7j3zJbAL5/QBUL4o=",
            "Authorization": "AWS4-HMAC-SHA256 Credential=ASIAXRIBVJDHZMNYSAXM/20230420/us-east-1/appsync/aws4_request, SignedHeaders=accept;content-type;host;x-amz-date;x-amz-security-token, Signature=f410f6f33cfc4bcea4f4c49cd999344d7fc38113d734e8afe8d05f91da47330e",
            "x-amz-user-agent": "aws-amplify/2.0.2",
            "Origin": "https://www.goodreads.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "cross-site"
        }
        body = '{"operationName":"getReviews","variables":{"filters":{"resourceType":"WORK","resourceId":"kca://work/amzn1.gr.work.v1.jY9lyLQNtOx-St1AGeHgBA","ratingMin":5,"ratingMax":5},"pagination":{"limit":30}},"query":"query getReviews($filters: BookReviewsFilterInput!, $pagination: PaginationInput) {\n  getReviews(filters: $filters, pagination: $pagination) {\n    ...BookReviewsFragment\n    __typename\n  }\n}\n\nfragment BookReviewsFragment on BookReviewsConnection {\n  totalCount\n  edges {\n    node {\n      ...ReviewCardFragment\n      __typename\n    }\n    __typename\n  }\n  pageInfo {\n    prevPageToken\n    nextPageToken\n    __typename\n  }\n  __typename\n}\n\nfragment ReviewCardFragment on Review {\n  __typename\n  id\n  creator {\n    ...ReviewerProfileFragment\n    __typename\n  }\n  recommendFor\n  updatedAt\n  createdAt\n  spoilerStatus\n  lastRevisionAt\n  text\n  rating\n  shelving {\n    shelf {\n      name\n      webUrl\n      __typename\n    }\n    taggings {\n      tag {\n        name\n        webUrl\n        __typename\n      }\n      __typename\n    }\n    webUrl\n    __typename\n  }\n  likeCount\n  viewerHasLiked\n  commentCount\n}\n\nfragment ReviewerProfileFragment on User {\n  id: legacyId\n  imageUrlSquare\n  isAuthor\n  ...SocialUserFragment\n  textReviewsCount\n  viewerRelationshipStatus {\n    isBlockedByViewer\n    __typename\n  }\n  name\n  webUrl\n  contributor {\n    id\n    works {\n      totalCount\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment SocialUserFragment on User {\n  viewerRelationshipStatus {\n    isFollowing\n    isFriend\n    __typename\n  }\n  followersCount\n  __typename\n}\n"}'

        yield scrapy.Request(
            url='https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql',
            method="POST",
            body=json.dumps(body),
            headers=headers,
            callback=self.parse,
            # dont_filter=True,
        )

        def parse_item(self, response):
            yield json.loads(response.text)

