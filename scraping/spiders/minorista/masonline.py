import re
from urllib.parse import quote

import scrapy


class MasonlineSpider(scrapy.Spider):
    name = "masonline"
    allowed_domains = ["masonline.com.ar"]

    custom_settings = {
        "LOG_LEVEL": "INFO",
        "DOWNLOAD_DELAY": 0.2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_TIMEOUT": 60,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self, ean=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ean = str(ean).strip() if ean else None
        if not self.ean:
            raise ValueError("Informe o parâmetro -a ean=...")

    async def start(self):
        url = f"https://www.masonline.com.ar/busqueda?q={quote(self.ean)}"
        self.logger.info(f"Agendando busca | ean={self.ean} | url={url}")
        yield scrapy.Request(
            url=url,
            callback=self.parse_search,
            meta={"ean_buscado": self.ean},
        )

    def parse_search(self, response):
        ean_buscado = response.meta["ean_buscado"]

        self.logger.info(f"EAN: {ean_buscado} | URL: {response.url}")

        links = self._extract_product_links(response)
        self.logger.info(f"Links de produto encontrados: {len(links)}")

        if not links:
            yield {
                "ean": ean_buscado,
                "ean_buscado": ean_buscado,
                "nome": None,
                "preco": None,
                "descricao": None,
                "url": None,
            }
            return

        for url_produto in links:
            yield scrapy.Request(
                url=url_produto,
                callback=self.parse_product,
                meta={
                    "ean_buscado": ean_buscado,
                    "url_produto": url_produto,
                },
            )

    def parse_product(self, response):
        ean_buscado = response.meta["ean_buscado"]
        url_produto = response.meta.get("url_produto") or response.url
        html = response.text

        nome = (
            response.css("h1::text").get()
            or response.css('meta[property="og:title"]::attr(content)').get()
            or response.css('meta[name="twitter:title"]::attr(content)').get()
            or self._extract_json_field(html, ["productName", "name"])
        )
        nome = self._clean(nome)

        descricao = (
            response.css('meta[name="description"]::attr(content)').get()
            or response.css('meta[property="og:description"]::attr(content)').get()
            or response.css('meta[name="twitter:description"]::attr(content)').get()
            or self._extract_json_field(html, ["description"])
        )
        descricao = self._clean(descricao)

        preco = self._extract_price_from_html(html)
        ean_extraido = self._extract_ean_from_html(html) or ean_buscado

        url_canonica = (
            response.css('link[rel="canonical"]::attr(href)').get()
            or response.css('meta[property="og:url"]::attr(content)').get()
            or url_produto
        )
        url_canonica = self._clean(url_canonica)

        yield {
            "ean": ean_extraido,
            "ean_buscado": ean_buscado,
            "nome": nome,
            "preco": preco,
            "descricao": descricao,
            "url": url_canonica,
        }

    def _extract_product_links(self, response):
        hrefs = response.css("a::attr(href)").getall()
        links = []
        vistos = set()

        for href in hrefs:
            if not href:
                continue

            href = href.strip()

            if href.startswith("#"):
                continue
            if href.startswith("javascript:"):
                continue
            if "mailto:" in href:
                continue

            full_url = response.urljoin(href.split("?")[0])

            if "masonline.com.ar" not in full_url:
                continue

            if not self._looks_like_product_url(full_url):
                continue

            if full_url in vistos:
                continue

            vistos.add(full_url)
            links.append(full_url)

        return links

    def _looks_like_product_url(self, url):
        url = url.lower()

        bad_parts = [
            "/busqueda",
            "/login",
            "/checkout",
            "/cart",
            "/account",
            "/institucional",
            "/_secure",
            "/api/",
        ]
        if any(part in url for part in bad_parts):
            return False

        good_rules = [
            url.endswith("/p"),
            "/p/" in url,
            re.search(r"/[a-z0-9\-]+/p$", url),
            re.search(r"/[a-z0-9\-]+-?\d*$", url),
        ]

        return any(good_rules)

    def _extract_ean_from_html(self, html):
        if not html:
            return None

        patterns = [
            r'"ean"\s*:\s*"(\d{8,14})"',
            r'"gtin13"\s*:\s*"(\d{8,14})"',
            r'"gtin"\s*:\s*"(\d{8,14})"',
            r'"product[Ee]an"\s*:\s*"(\d{8,14})"',
            r'"Ean"\s*:\s*"(\d{8,14})"',
            r'\b(\d{13})\b',
        ]

        for pattern in patterns:
            m = re.search(pattern, html)
            if m:
                return m.group(1)

        return None

    def _extract_price_from_html(self, html):
        if not html:
            return None

        patterns = [
            r'"sellingPrice"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'"price"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'"highPrice"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'"lowPrice"\s*:\s*([0-9]+(?:\.[0-9]+)?)',
            r'"bestPrice"\s*:\s*"?(?P<v>[0-9]+(?:[.,][0-9]+)?)"?',
            r'"spotPrice"\s*:\s*"?(?P<v>[0-9]+(?:[.,][0-9]+)?)"?',
        ]

        for pattern in patterns:
            m = re.search(pattern, html)
            if m:
                value = m.groupdict().get("v") if m.groupdict() else m.group(1)
                try:
                    return float(str(value).replace(",", "."))
                except Exception:
                    pass

        price_text = re.search(
            r'\$\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})|[0-9]+(?:,[0-9]{2})?)',
            html,
        )
        if price_text:
            raw = price_text.group(1).replace(".", "").replace(",", ".")
            try:
                return float(raw)
            except Exception:
                return None

        return None

    def _extract_json_field(self, html, field_names):
        if not html:
            return None

        for field in field_names:
            pattern = rf'"{re.escape(field)}"\s*:\s*"([^"]+)"'
            m = re.search(pattern, html)
            if m:
                return m.group(1)

        return None

    def _clean(self, value):
        if value is None:
            return None
        value = re.sub(r"\s+", " ", str(value)).strip()
        return value or None