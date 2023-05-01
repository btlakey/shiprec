import scrapy
import logging
import json
import requests
from datetime import datetime, timedelta
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import os

logger = logging.getLogger('logger')
logger.STDOUT = True


class ReviewSpider(scrapy.Spider):
    name = "review"
    start_urls = ["https://www.goodreads.com/review/list/40648422"]

    auth_headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.goodreads.com/',
        'X-Amz-User-Agent': 'aws-sdk-js/2.711.0 callback',
        'Content-Type': 'application/x-amz-json-1.1',
        'X-Amz-Target': 'AWSCognitoIdentityService.GetCredentialsForIdentity',
        'X-Amz-Content-Sha256': '6030ca7c1f3393246848c0323a5e26fea5b3f98543b92a2e409fdf030aba5245',
        'Origin': 'https://www.goodreads.com',
        'Connection': 'keep-alive',
    }
    auth_url = "https://cognito-identity.us-east-1.amazonaws.com/"
    auth_payload = "{\"IdentityId\":\"us-east-1:5cb98804-4a0e-4004-aeae-f8feb41b7be5\"}"

    method = "POST"
    host = "kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com"
    region = "us-east-1"
    service = "appsync"
    url = "https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql"

    def get_auth(self):
        # get credentials
        self.utcnow = datetime.utcnow()
        self.amz_date = self.utcnow.strftime("%Y%m%d")
        self.amz_time = self.utcnow.strftime("%H%M%S")
        # now = datetime.datetime.now(pytz.timezone('US/Eastern'))

        response = requests.request(
            self.method,
            self.auth_url,
            headers=self.auth_headers,
            data=self.auth_payload
        )
        self.auth = json.loads(response.text)["Credentials"]

        # write to credentials, which is read by default for env variables
        with open(os.path.expanduser("~/.aws/credentials"), "w") as f_out:
            f_out.write(
                "[default]"
                f"\naws_access_key_id={self.auth['AccessKeyId']}"
                f"\naws_secret_access_key={self.auth['SecretKey']}"
                f"\naws_session_token={self.auth['SessionToken']}"
            )

    def check_auth(self):
        try:
            if (datetime.utcnow() - self.utcnow) >= timedelta(seconds=300):
                self.get_auth()
        except AttributeError:
            self.get_auth()

    def make_review_payload(self, review_num=64):
        self.review_num = review_num
        # not worth prettifying this string, but it does set review_num pagination
        self.reviews_payload = "{\"operationName\":\"getReviews\",\"variables\":{\"filters\":{\"resourceType\":\"WORK\",\"resourceId\":\"kca://work/amzn1.gr.work.v1.jY9lyLQNtOx-St1AGeHgBA\"},\"pagination\":{\"limit\":" + str(self.review_num) + "}},\"query\":\"query getReviews($filters: BookReviewsFilterInput!, $pagination: PaginationInput) {\\n  getReviews(filters: $filters, pagination: $pagination) {\\n    ...BookReviewsFragment\\n    __typename\\n  }\\n}\\n\\nfragment BookReviewsFragment on BookReviewsConnection {\\n  totalCount\\n  edges {\\n    node {\\n      ...ReviewCardFragment\\n      __typename\\n    }\\n    __typename\\n  }\\n  pageInfo {\\n    prevPageToken\\n    nextPageToken\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment ReviewCardFragment on Review {\\n  __typename\\n  id\\n  creator {\\n    ...ReviewerProfileFragment\\n    __typename\\n  }\\n  recommendFor\\n  updatedAt\\n  createdAt\\n  spoilerStatus\\n  lastRevisionAt\\n  text\\n  rating\\n  shelving {\\n    shelf {\\n      name\\n      webUrl\\n      __typename\\n    }\\n    taggings {\\n      tag {\\n        name\\n        webUrl\\n        __typename\\n      }\\n      __typename\\n    }\\n    webUrl\\n    __typename\\n  }\\n  likeCount\\n  viewerHasLiked\\n  commentCount\\n}\\n\\nfragment ReviewerProfileFragment on User {\\n  id: legacyId\\n  imageUrlSquare\\n  isAuthor\\n  ...SocialUserFragment\\n  textReviewsCount\\n  viewerRelationshipStatus {\\n    isBlockedByViewer\\n    __typename\\n  }\\n  name\\n  webUrl\\n  contributor {\\n    id\\n    works {\\n      totalCount\\n      __typename\\n    }\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment SocialUserFragment on User {\\n  viewerRelationshipStatus {\\n    isFollowing\\n    isFriend\\n    __typename\\n  }\\n  followersCount\\n  __typename\\n}\\n\"}"

    def parse(self, response):
        self.make_review_payload()
        self.check_auth()

        auth = BotoAWSRequestsAuth(
            aws_host=self.host,
            aws_region=self.region,
            aws_service=self.service
        )
        response = requests.request(
            self.method,
            url=self.url,
            data=self.reviews_payload,
            auth=auth
        )
        data = json.loads(response.text)

        yield from data["data"]["getReviews"]["edges"]
