from itemadapter import ItemAdapter
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import random

from utils.utils import get_project_root


class TrawlPipeline:
    def __init__(self):
        self.items = []
        self.record_ct = 0

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

    def process_item(self, item, spider):
        pass
