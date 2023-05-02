import scrapy
import locale
import json
import requests
from datetime import datetime, timedelta
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth
import os

from utils import not_none

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')  # this handles commas in string numbers


class TrawlSpider(scrapy.Spider):

    @staticmethod
    def strip_convert(val, **kwargs):
        ## should this be in the pipeline adapter?
        # various fields sometimes return None, empty string, or only whitespace
        try:
            val = val.strip()
        except AttributeError:
            pass

        if not val:
            return None
        else:
            for type_check, convert in zip(["is_int", "is_float"], [int, float]):
                if kwargs.get(type_check, False):
                    val = convert(locale.atof(val))  # remove any commas that might be there; atof=float
            return val

    def response_get(self, response, xpath_query, **kwargs):
        # the . prevents the xpath query from going all the way back to the root node
        val = response.xpath("." + xpath_query)

        # is there a list of elements that are returned
        if kwargs.get("getall"):
            val_return = [self.strip_convert(v, **kwargs) for v in val.getall()]
            val_return = list(filter(not_none, val_return))  # remove any None elements from list
        else:
            val_return = self.strip_convert(val.get(), **kwargs)

        return "\n".join(val_return) if kwargs.get("concat") else val_return


class GraphQLSpider(TrawlSpider):

    def __init__(self, **kwargs):
        self.auth_headers = {
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
        self.auth_url = "https://cognito-identity.us-east-1.amazonaws.com/"
        self.auth_payload = "{\"IdentityId\":\"us-east-1:5cb98804-4a0e-4004-aeae-f8feb41b7be5\"}"
        self.auth, self.utcnow = None, None

        self.method = "POST"
        self.region = "us-east-1"
        self.service = "appsync"
        self.host = "kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com"
        self.url = "https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql"
        self.record_ct = kwargs.get("record_ct", 64)

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
        except TypeError:
            self.get_auth()

    def graphql_request(self, request_payload):
        self.check_auth()

        auth = BotoAWSRequestsAuth(
            aws_host=self.host,
            aws_region=self.region,
            aws_service=self.service
        )
        # TODO: make this a scrapy request object
        response = requests.request(
            self.method,
            url=self.url,
            data=request_payload,
            auth=auth
        )

        return json.loads(response.text)
