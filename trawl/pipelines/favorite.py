from itemadapter import ItemAdapter
import pyarrow as pa

from trawl.pipelines import TrawlPipeline


class FavoritePipeline(TrawlPipeline):

    def __init__(self):
        super().__init__()
        self.custom_settings = {
            "ITEM_PIPELINES": {
                "trawl.pipelines.reader.FavoritePipeline": 1
            }
        }

        # define schema here, otherwise some nullable columns will be inferred incorrectly
        self.schema = pa.schema([
            ("bookid", pa.string()),
            ("bookid_int", pa.int64()),
            ("title", pa.string()),
            ("author", pa.string()),
            ("readers", pa.list_(pa.string()))
        ])
        self.subdir = "favorite"

    def process_item(self, item, spider):
        """ For each scraped reader shelf dict, do any necessary post-processing

        :param item: scrapy.Item, a wrapper class for dictonary of scraped items
        :param spider: scrapy.Spider, required arg

        :return: itemadapter.ItemAdapter
            processed item, returned with a different adapter dict wrapper
            (scrapy, I think you might have overdone the wrappers)
        """
        adapter = ItemAdapter(item)
        adapter["bookid_int"] = int(adapter["bookid"].split(".")[0])
        self.batch_items(adapter)
        return adapter
