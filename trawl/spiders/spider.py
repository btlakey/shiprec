import scrapy
import locale

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')  # this handles commas in string numbers


class TrawlSpider(scrapy.Spider):

    @staticmethod
    def response_get(response, xpath_query, **kwargs):
        # the . prevents the xpath query from going all the way back to the root node
        val = response.xpath("." + xpath_query).get()

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
