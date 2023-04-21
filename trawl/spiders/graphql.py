import requests

url = "https://kxbwmqov6jgg3daaamb744ycu4.appsync-api.us-east-1.amazonaws.com/graphql"

payload = "{\"operationName\":\"getReviews\",\"variables\":{\"filters\":{\"resourceType\":\"WORK\",\"resourceId\":\"kca://work/amzn1.gr.work.v1.jY9lyLQNtOx-St1AGeHgBA\"},\"pagination\":{\"limit\":30}},\"query\":\"query getReviews($filters: BookReviewsFilterInput!, $pagination: PaginationInput) {\\n  getReviews(filters: $filters, pagination: $pagination) {\\n    ...BookReviewsFragment\\n    __typename\\n  }\\n}\\n\\nfragment BookReviewsFragment on BookReviewsConnection {\\n  totalCount\\n  edges {\\n    node {\\n      ...ReviewCardFragment\\n      __typename\\n    }\\n    __typename\\n  }\\n  pageInfo {\\n    prevPageToken\\n    nextPageToken\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment ReviewCardFragment on Review {\\n  __typename\\n  id\\n  creator {\\n    ...ReviewerProfileFragment\\n    __typename\\n  }\\n  recommendFor\\n  updatedAt\\n  createdAt\\n  spoilerStatus\\n  lastRevisionAt\\n  text\\n  rating\\n  shelving {\\n    shelf {\\n      name\\n      webUrl\\n      __typename\\n    }\\n    taggings {\\n      tag {\\n        name\\n        webUrl\\n        __typename\\n      }\\n      __typename\\n    }\\n    webUrl\\n    __typename\\n  }\\n  likeCount\\n  viewerHasLiked\\n  commentCount\\n}\\n\\nfragment ReviewerProfileFragment on User {\\n  id: legacyId\\n  imageUrlSquare\\n  isAuthor\\n  ...SocialUserFragment\\n  textReviewsCount\\n  viewerRelationshipStatus {\\n    isBlockedByViewer\\n    __typename\\n  }\\n  name\\n  webUrl\\n  contributor {\\n    id\\n    works {\\n      totalCount\\n      __typename\\n    }\\n    __typename\\n  }\\n  __typename\\n}\\n\\nfragment SocialUserFragment on User {\\n  viewerRelationshipStatus {\\n    isFollowing\\n    isFriend\\n    __typename\\n  }\\n  followersCount\\n  __typename\\n}\\n\"}"
headers = {
  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0',
  'Accept': '*/*',
  'Accept-Language': 'en-US,en;q=0.5',
  'Accept-Encoding': 'gzip, deflate, br',
  'Referer': 'https://www.goodreads.com/',
  'content-type': 'application/json; charset=UTF-8',
  'x-amz-date': '20230421T144518Z',
  'X-Amz-Security-Token': 'IQoJb3JpZ2luX2VjEK///////////wEaCXVzLWVhc3QtMSJHMEUCIGJPuNzGX8T0LheDJaYHIK1uTByEtPhFh5LQNdjx2PWmAiEAhLlNFCRqQKlUoO/l3vpdUTn92TiiPVGZ5bpdYoLZ1VYqkAYIqP//////////ARACGgw1MTgwODM5MjIxMjciDCKZpP4ErCBz6KtcQCrkBVFOzy5Zu/rx2hXkOYzen74/VOb0rca745GRtdYtKil51kERUfPKrAEVwNVpzfi9ELnWEEf7kQ0Wv64l7fvVgiSXAFaaCbyB8jCqvx2gcd1RMtJAEAqnQ1jHxPJnXGlzre2i/eCzo2I/YHBYIt0oSNK+zm1OzeXQpO9LURZ/JCB8ZPdZQNRZYyb2h6e2rRmB5kDYuWpslxHegf/AikIgdrl/WwSa/97OhpZJaDqnQr8j5ivRlCgLqJ/UCewaao8Lt2dScwxrGf7yxVECwwx2ZaFauKkThDtM+m7hYX67IJzExuMD5LmGRmKKoYr7Uy1Y0rcQTSC4gXh1Dhu91W17Q/m49unSQsbAPV+Xj5cmwSa6jPZrAYD4rMuJJFQnuVWZ0htglQocxRKy6RZ6K0Qin6kVKQ9OSY2coY63XXHs8wacV85Wng2IDIkBjBCD19wOLLxRVhXiajA70UvEAtiD7jcAT6dk0IbbWFV+pANuLYK3dqMT5FACpV9q74pzWsOYbhE1DtrBUwnyisTpxpHH81jwYQVZCuvzniuZ+2tZIP+6ebDAdcq92t3jCaa2Kev7NrpQk+ueLyD3WULhuSiKvd+J1Q3oU6kSt4aLoFA5C3fz9hKytNT2sJJY0/MgLhIChyTTXlmcB7S2vzPW/2SSJi3xvSoWVzsBEvxnXrrTfgA9qiNBZJzivG2CJHz4xxoLchx9CH9KOXQTjyKxwrZwZrgUdPv1ZnJ88A//zzpI+/g2M3wdP/eAtb6cnNLVJ0+ky7HlGi3lQFJnzbWr3DgsABzO/U6rOA9rKK2wMdJpGTqxLScpDE/0m1XhYH9APbs51E9KjyyeIJqE+xRtzsGdCS+kYjVH+u29PBFe13JUOwUe0TTCS+dghA22QoOsrhQF3ZM994jxERndZlMVRrDbaK0pB52ObHfZSiVEe0cf7Ogpwzdt0eEd2mYgrOnOYPq1nFASKNGq/tcrU/tR4Rf+Y5a3SbSgMP7CiqIGOocC5Uo24YAxHoM1BLr7gG7JXMYBUBYH3gXn4UOnXuPe4RDFBWLmVuwrIJzotS6OyMKMRnkwPQcofWxghP9yT8QPFqxdsa8Oufa0QeH50Vpf00hcwXXS8dkO14uvBS6oJyL3NDdhfZi7kMnefDmbhnNZb4tfuSC8V/mkusGijb7uBmCwoQXGRfFM9xvE1YnocVAs9rz8Rqxx8FWMPcCaicprdDQ4rAQrE+TMPw44agMOKe2f9+EREZnL8sk3XbgVyVic5qHCkzs6/5D/ZWPI9md0uK++vkt+1pmQQPhqcgDXSauSwGtUL7tbCVM7/WLHrnHYI5C4ETZfzc4EJfhzlPsK2KTRLcADnOI=',
  'Authorization': 'AWS4-HMAC-SHA256 Credential=ASIAXRIBVJDHV5OAUTVB/20230421/us-east-1/appsync/aws4_request, SignedHeaders=accept;content-type;host;x-amz-date;x-amz-security-token, Signature=4ca2e3d7d3516cda7622c45918e1ec7cf688d746d0edc9f433128422d6c51e18',
  'x-amz-user-agent': 'aws-amplify/2.0.2',
  'Origin': 'https://www.goodreads.com',
  'Connection': 'keep-alive',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'cross-site',
  'TE': 'trailers'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
