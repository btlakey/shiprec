import logging

from trawl.spiders import GraphQLSpider
logger = logging.getLogger('logger')
logger.STDOUT = True


class ReviewSpider(GraphQLSpider):
    name = "review"
    start_urls = ["https://www.goodreads.com/review/list/40648422"]

    def make_review_payload(self):
        # not worth prettifying this string, but it does set review_num pagination
        return "{\"operationName\":\"getReviews\",\"variables\":{\"filters\":{\"resourceType\":\"WORK\",\"resourceId\":\"kca://work/amzn1.gr.work.v1.jY9lyLQNtOx-St1AGeHgBA\"},\"pagination\":{\"limit\":" + str(self.record_ct) + "}},\"query\":\"query getReviews($filters: BookReviewsFilterInput!, $pagination: PaginationInput) {\\n  getReviews(filters: $filters, pagination: $pagination) {\\n    ...BookReviewsFragment\\n    __typename\\n  }\\n}\\n\\nfragment BookReviewsFragment on BookReviewsConnection {\\n  totalCount\\n  edges {\\n    node {\\n      ...ReviewCardFragment\\n      __typename\\n    }\\n    __typename\\n  }\\n  pageInfo {\\n    prevPageToken\\n    nextPageToken\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment ReviewCardFragment on Review {\\n  __typename\\n  id\\n  creator {\\n    ...ReviewerProfileFragment\\n    __typename\\n  }\\n  recommendFor\\n  updatedAt\\n  createdAt\\n  spoilerStatus\\n  lastRevisionAt\\n  text\\n  rating\\n  shelving {\\n    shelf {\\n      name\\n      webUrl\\n      __typename\\n    }\\n    taggings {\\n      tag {\\n        name\\n        webUrl\\n        __typename\\n      }\\n      __typename\\n    }\\n    webUrl\\n    __typename\\n  }\\n  likeCount\\n  viewerHasLiked\\n  commentCount\\n}\\n\\nfragment ReviewerProfileFragment on User {\\n  id: legacyId\\n  imageUrlSquare\\n  isAuthor\\n  ...SocialUserFragment\\n  textReviewsCount\\n  viewerRelationshipStatus {\\n    isBlockedByViewer\\n    __typename\\n  }\\n  name\\n  webUrl\\n  contributor {\\n    id\\n    works {\\n      totalCount\\n      __typename\\n    }\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment SocialUserFragment on User {\\n  viewerRelationshipStatus {\\n    isFollowing\\n    isFriend\\n    __typename\\n  }\\n  followersCount\\n  __typename\\n}\\n\"}"

    def parse(self, response):
        review_payload = self.make_review_payload()
        data = self.graphql_request(review_payload)
        yield from data["data"]["getReviews"]["edges"]
