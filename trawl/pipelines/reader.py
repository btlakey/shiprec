from itemadapter import ItemAdapter
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import random
import logging

from utils.utils import get_project_root
from trawl.pipelines import TrawlPipeline

logger = logging.getLogger('logger')
logger.STDOUT = True


class ReaderPipeline(TrawlPipeline):

    def __init__(self):
        """ Processing and serialization pipeline for Reader spider
        Make sure that Pipelines are appropriately specified (including processing order)
          in trawl.settings.ITEM_PIPELINES
        """
        super().__init__()
        self.custom_settings = {
            "ITEM_PIPELINES": {
                "trawl.pipelines.reader.ReaderPipeline": 2
            }
        }

        # TODO: do all stripping and type conversions in pipeline
        self.items_conv = (
            (int, ["isbn13", "num_rating", "user_rating"]),
            (float, ["mean_rating"]),
            (datetime, ["date_pub", "date_read", "date_added"]),
            (str, ["title", "author"])
        )
        self.items = []
        self.record_ct = 0

        # define schema here, otherwise some nullable columns will be inferred incorrectly
        self.schema = pa.schema([
            ("title", pa.string()),
            ("author", pa.string()),
            ("date_pub", pa.string()),
            ("isbn13", pa.int64()),
            ("user_rating", pa.int64()),
            ("date_read", pa.string()),
            ("date_added", pa.string()),
            ("review_text", pa.string()),
            ("shelf_url", pa.string()),
        ])
        self.subdir = "reader"

    def process_item(self, item, spider):
        """ For each scraped reader shelf dict, do any necessary post-processing

        :param item: scrapy.Item, a wrapper class for dictonary of scraped items
        :param spider: scrapy.Spider, required arg

        :return: itemadapter.ItemAdapter
            processed item, returned with a different adapter dict wrapper
            (scrapy, I think you might have overdone the wrappers)
        """
        adapter = ItemAdapter(item)

        if adapter.get("user_rating", None):
            adapter["user_rating"] = self.convert_rating(adapter["user_rating"])
        else:
            raise Warning(f"missing user_rating in {item}")

        self.batch_items(adapter)
        return adapter

    @staticmethod
    def convert_rating(rating: str):
        """ Convert a string representation of user rating to numerical

        :param rating: str, rating text from goodreads.com
        :return: int, between 1 and 5 according to map
        """
        rating_map = {
            "it was amazing": 5,
            "really liked it": 4,
            "liked it": 3,
            "it was OK": 2,
            "didn't like it": 1
        }
        for k, v in rating_map.items():
            if rating == k:
                return v