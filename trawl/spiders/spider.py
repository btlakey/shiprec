import scrapy
import locale

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


