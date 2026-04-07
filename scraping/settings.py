BOT_NAME = "scraping"


SPIDER_MODULES = [
    "scraping.spiders",
    "scraping.spiders.makro",
]
NEWSPIDER_MODULE = "scraping.spiders"


ROBOTSTXT_OBEY = False


ZYTE_API_KEY = "8ec0288a2dd0494c9d6a8e458a56f8bf"
ZYTE_API_TRANSPARENT_MODE = True


DOWNLOADER_MIDDLEWARES = {
    "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 1000,
}


REQUEST_FINGERPRINTER_CLASS = "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter"


TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"


FEED_EXPORT_ENCODING = "utf-8"