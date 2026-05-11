BOT_NAME = "scraping"


SPIDER_MODULES = [
    "scraping.spiders",
    "scraping.spiders.makro",
    "scraping.spiders.minorista",
]


NEWSPIDER_MODULE = "scraping.spiders"


ROBOTSTXT_OBEY = False


# =========================
# ZYTE
# =========================
ZYTE_API_KEY = "8ec0288a2dd0494c9d6a8e458a56f8bf"
ZYTE_API_ENABLED = True

# obrigatório para que requests http/https passem pela Zyte API
DOWNLOAD_HANDLERS = {
    "http": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
    "https": "scrapy_zyte_api.ScrapyZyteAPIDownloadHandler",
}


# ❌ REMOVIDO (importante!)
# ZYTE_API_TRANSPARENT_MODE = True


DOWNLOADER_MIDDLEWARES = {
    "scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 610,
}


REQUEST_FINGERPRINTER_CLASS = "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter"
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"  # necessário pro Scrapy 2.7+ com scrapy-zyte-api[web:162]


# =========================
# ASYNC / SCRAPY
# =========================
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"


REFERER_ENABLED = True


# =========================
# EXPORTAÇÃO
# =========================
FEED_EXPORT_ENCODING = "utf-8"


# =========================
# AJUSTES DE CRAWL
# =========================
DOWNLOAD_DELAY = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 2
RETRY_TIMES = 2
DOWNLOAD_TIMEOUT = 60


# =========================
# LOG
# =========================
LOG_LEVEL = "INFO"