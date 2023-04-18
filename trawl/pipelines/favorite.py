from itemadapter import ItemAdapter
import pyarrow as pa

from trawl.pipelines import TrawlPipeline


class ReaderPipeline(TrawlPipeline):

    def __init__(self):
        super().__init__()

        # define schema here, otherwise some nullable columns will be inferred incorrectly
        self.schema = pa.schema([
            ("bookid", pa.int64()),
            ("title", pa.int64()),
            ("author", pa.string()),
            ("readers", pa.StringArray())
        ])

    def process_item(self, item, spider):
        """ For each scraped reader shelf dict, do any necessary post-processing

        :param item: scrapy.Item, a wrapper class for dictonary of scraped items
        :param spider: scrapy.Spider, required arg

        :return: itemadapter.ItemAdapter
            processed item, returned with a different adapter dict wrapper
            (scrapy, I think you might have overdone the wrappers)
        """
        adapter = ItemAdapter(item)
        adapter["bookid"] = int(adapter["bookid"])
        self.batch_items(adapter)
        return adapter
