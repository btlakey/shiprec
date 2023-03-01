# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

from itemadapter import ItemAdapter
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq


class TrawlPipeline:

    # TODO: do all stripping and type conversions in pipeline
    items_conv = (
        (int, ["isbn13", "num_rating", "user_rating"]),
        (float, ["mean_rating"]),
        (datetime, ["date_pub", "date_read", "date_added"]),
        (str, ["title", "author"])
    )
    record_ct = 0

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        if adapter.get("user_rating", None):
            adapter["user_rating"] = self.convert_rating(adapter["user_rating"])
        else:
            raise Warning(f"missing user_rating in {item}")

        # TODO: add some userid (or shelfid?) string to filenames
        # filename = f"{self.record_ct:05d}_{adapter.get('user_id')}"
        filename = f"{self.record_ct:05d}"
        self.export_item(item, filename)
        return item

    def convert_to_pyarrow(self, item: dict):
        # TODO: handle list vs dict (should pass in list)
        return pa.Table.from_pylist([item])

    def export_item(self, item, filename):
        item_dict = ItemAdapter(item).asdict()
        pq.write_table(
            self.convert_to_pyarrow(item_dict),
            filename + ".parquet"
        )

    def convert_rating(self, rating: str):
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


#~#~#~#~#~#~#~#~#~#~#~#~#
#
# from itemadapter import ItemAdapter
# from datetime import datetime
# import pyarrow as pa
# import pyarrow.parquet as pq
#
#
# class ShelfPipeline:
#
#     # TODO: do all stripping and type conversions in pipeline
#     items_conv = (
#         (int, ["isbn13", "num_rating", "user_rating"]),
#         (float, ["mean_rating"]),
#         (datetime, ["date_pub", "date_read", "date_added"]),
#         (str, ["title", "author"])
#     )
#     record_ct = 0
#
#     def process_item(self, item, spider):
#         adapter = ItemAdapter(item)
#
#         if adapter.get("user_rating", None):
#             adapter["user_rating"] = self.convert_rating(adapter["user_rating"])
#         else:
#             raise Warning(f"missing user_rating in {item}")
#
#         # TODO: add some userid (or shelfid?) string to filenames
#         # filename = f"{self.record_ct:05d}_{adapter.get('user_id')}"
#         filename = f"{self.record_ct:05d}"
#         self.export_item(item, filename)
#         return item
#
#     def convert_to_pyarrow(self, item: dict):
#         # TODO: handle list vs dict (should pass in list)
#         return pa.Table.from_pylist([item])
#
#     def export_item(self, item, filename):
#         item_dict = ItemAdapter(item).asdict()
#         pq.write_table(
#             self.convert_to_pyarrow(item_dict),
#             filename + ".parquet"
#         )
#
#     def convert_rating(self, rating: str):
#         rating_map = {
#             "it was amazing": 5,
#             "really liked it": 4,
#             "liked it": 3,
#             "it was OK": 2,
#             "didn't like it": 1
#         }
#         for k, v in rating_map.items():
#             if rating == k:
#                 return v
