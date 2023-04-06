from itemadapter import ItemAdapter
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import random
import logging

from utils.utils import get_project_root

logger = logging.getLogger('logger')
logger.STDOUT = True


class ReaderPipeline:

    def __init__(self):
        """ Processing and serialization pipeline for Reader spider
        Make sure that Pipelines are appropriately specified (including processing order)
          in trawl.settings.ITEM_PIPELINES
        """

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
            ("isbn13", pa.int64()),
            ("author", pa.string()),
            ("date_pub", pa.string()),
            ("mean_rating", pa.float64()),
            ("num_rating", pa.int64()),
            ("user_rating", pa.int64()),
            ("date_read", pa.string()),
            ("date_added", pa.string()),
            ("review_text", pa.string()),
            ("shelf_url", pa.string()),
        ])

    def assert_schema(self):
        """ Double check that every key in the crawled item is present in the schema
        and vice versa
        """
        item = random.sample(self.items, 1)[0]
        assert len(
            set(item.keys()).symmetric_difference(set(self.schema.names))
        ) == 0

    def batch_items(self, adapter):
        """ Every n=64 scraped items, batch and serialize as parquet

        :param adapter: scrapy.ItemAdapter
            scrapy wrapper for a dict: items scraped from reader shelf
        :return: None
            assert schema and then serialize to parquet
        """
        self.items += [adapter.asdict()]
        if len(self.items) >= 64:
            self.assert_schema()
            self.export_items()

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

    def convert_to_pyarrow(self, items):
        """ Given a list of dictionaries, format into pyArrow table for parquet serialization
        Apply self.schema to prevent nullable column inference

        :param items: list(dict), list of adapter-processed items
        :return: pyarrow.Table
            formatted pyarrow Table for parquet serialization
        """
        return pa.Table.from_pylist(items, schema=self.schema)

    def export_items(self):
        """ Serialize self.items batch with a UUID filename, and then reset batch

        :return: None
            serialize to parquet, reset batch
        """
        filename = f"{get_project_root('data')}/{uuid.uuid4()}"
        print(f"filename: {filename}\n")
        pq.write_table(
            self.convert_to_pyarrow(self.items),
            filename + ".parquet"
        )
        self.items = []

    def close_spider(self, spider):
        """ Built-in method for final processing step when spider is closed
        Serialize any remaining items in self.items batch

        :param spider: scrapy.Spider, required arg
        :return: None
            call to self.export_items()
        """
        print(f"dumping last items: {len(self.items)}")
        self.export_items()

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