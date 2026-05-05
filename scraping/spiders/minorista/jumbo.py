import csv
import json
import os
import re
from urllib.parse import quote

import scrapy
import openpyxl


class JumboSearchSpider(scrapy.Spider):
    name = "jumbo_search"
    allowed_domains = ["jumbo.com.ar"]

    custom_settings = {
        "DOWNLOAD_DELAY": 0.8,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_TIMEOUT": 60,
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_ENCODING": "utf-8",
        "FEED_EXPORT_FIELDS": [
            "ean",
            "sku",
            "nome",
            "marca",
            "precoDe",
            "precoPor",
            "porcentagem",
            "oferta",
            "loja",
            "link",
        ],
    }

    def __init__(self, ean=None, ean_file=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.eans = []

        if ean:
            self.eans.append(self.clean_ean(ean))

        if ean_file:
            caminho = os.path.expanduser(str(ean_file).strip())
            if not os.path.isfile(caminho):
                raise ValueError(f"Arquivo não encontrado: {caminho}")

            self.eans.extend(self.load_eans_from_file(caminho))

        self.eans = self.unique_preserve_order([x for x in self.eans if x])

        if not self.eans:
            raise ValueError("Passe -a ean=... ou -a ean_file=... com uma planilha/arquivo válido")

        self.logger.info(f"Total de EANs carregados: {len(self.eans)}")

    def clean_ean(self, value):
        if value is None:
            return None

        s = str(value).strip()
        if not s:
            return None

        if re.fullmatch(r"\d+(\.0+)?", s):
            s = s.split(".")[0]

        s = re.sub(r"[^\d]", "", s)

        return s or None

    def normalize_header(self, value):
        if value is None:
            return ""
        s = str(value).strip().upper()
        s = s.replace("\ufeff", "")
        s = re.sub(r"\s+", " ", s)
        return s

    def unique_preserve_order(self, values):
        seen = set()
        result = []
        for v in values:
            if v in seen:
                continue
            seen.add(v)
            result.append(v)
        return result

    def load_eans_from_file(self, caminho):
        ext = os.path.splitext(caminho)[1].lower()

        if ext in [".xlsx", ".xlsm", ".xltx", ".xltm"]:
            return self.load_eans_from_excel(caminho)

        if ext in [".csv", ".txt"]:
            return self.load_eans_from_text(caminho)

        raise ValueError(f"Extensão não suportada: {ext}. Use XLSX, CSV ou TXT")

    def load_eans_from_excel(self, caminho):
        wb = openpyxl.load_workbook(caminho, read_only=True, data_only=True)
        ws = wb.active

        header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
        if not header_row:
            wb.close()
            raise ValueError("Planilha vazia")

        ean_col_index = None
        for idx, cell_value in enumerate(header_row):
            h = self.normalize_header(cell_value)
            if h == "EAN":
                ean_col_index = idx
                break

        if ean_col_index is None:
            wb.close()
            raise ValueError("Não encontrei a coluna 'EAN' no cabeçalho da planilha")

        eans = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            if ean_col_index >= len(row):
                continue
            ean = self.clean_ean(row[ean_col_index])
            if ean:
                eans.append(ean)

        wb.close()
        return eans

    def load_eans_from_text(self, caminho):
        eans = []

        with open(caminho, "r", encoding="utf-8-sig", newline="") as f:
            amostra = f.read(2048)
            f.seek(0)

            delimiter = ";" if amostra.count(";") > amostra.count(",") else ","

            if "EAN" in self.normalize_header(amostra):
                reader = csv.DictReader(f, delimiter=delimiter)
                for row in reader:
                    ean = self.clean_ean(row.get("EAN"))
                    if ean:
                        eans.append(ean)
            else:
                for line in f:
                    ean = self.clean_ean(line)
                    if ean:
                        eans.append(ean)

        return eans

    def start_requests(self):
        for ean in self.eans:
            url = (
                "https://www.jumbo.com.ar/api/catalog_system/pub/products/search/"
                f"?fq=alternateIds_Ean:{quote(ean)}"
            )
            yield scrapy.Request(
                url=url,
                callback=self.parse_search,
                errback=self.errback_search,
                dont_filter=True,
                meta={
                    "ean": ean,
                    "handle_httpstatus_all": True,
                },
            )

    def errback_search(self, failure):
        ean = failure.request.meta.get("ean")
        self.logger.info(f"Erro buscando EAN {ean}: {repr(failure.value)}")
        yield self.empty_item(ean)

    def parse_search(self, response):
        ean = response.meta["ean"]

        if response.status != 200:
            self.logger.info(f"Resposta HTTP {response.status} para EAN {ean}")
            yield self.empty_item(ean)
            return

        try:
            data = json.loads(response.text)
        except Exception:
            self.logger.info(f"JSON inválido para EAN {ean}")
            yield self.empty_item(ean)
            return

        if not isinstance(data, list) or not data:
            self.logger.info(f"Nenhum produto encontrado para EAN {ean}")
            yield self.empty_item(ean)
            return

        found_product = None
        found_item = None

        for product in data:
            for item in product.get("items", []) or []:
                item_ean = self.clean_ean(item.get("ean"))
                if item_ean == ean:
                    found_product = product
                    found_item = item
                    break
            if found_item:
                break

        if not found_item:
            self.logger.info(f"Nenhum SKU encontrado para EAN {ean}")
            yield self.empty_item(ean)
            return

        sellers = found_item.get("sellers") or []
        seller = sellers[0] if sellers else {}
        offer = seller.get("commertialOffer") or {}

        price = self.normalize_price(offer.get("Price"))
        list_price = self.normalize_price(offer.get("ListPrice"))

        promo_texts = self.collect_promo_texts(found_product, found_item, offer)
        desconto_medio = self.extract_second_unit_average_discount(promo_texts)
        has_promo = self.has_promotion(found_product, found_item, offer, promo_texts)

        preco_de = None
        preco_por = None
        porcentagem = None

        if has_promo:
            if desconto_medio is not None and price is not None:
                preco_de = price
                preco_por = round(preco_de * (1 - desconto_medio / 100), 2)
                porcentagem = desconto_medio
            else:
                porcentagem = self.extract_discount_dynamic(found_product, found_item, offer)

                if porcentagem is not None and price is not None:
                    preco_de = price
                    preco_por = round(preco_de * (1 - porcentagem / 100), 2)
                else:
                    if list_price is not None and price is not None and list_price > price:
                        preco_de = list_price
                        preco_por = price
                        porcentagem = self.calc_discount(preco_de, preco_por)

        else:
            preco_de = price
            preco_por = price

        oferta = "X" if has_promo and porcentagem else None

        sku = (
            found_item.get("itemId")
            or found_product.get("productReference")
            or found_product.get("productReferenceCode")
        )

        link = found_product.get("link")
        if link and link.startswith("/"):
            link = f"https://www.jumbo.com.ar{link}"

        item = {
            "ean": ean,
            "sku": str(sku) if sku is not None else None,
            "nome": found_product.get("productName") or found_product.get("productTitle"),
            "marca": found_product.get("brand"),
            "precoDe": preco_de,
            "precoPor": preco_por,
            "porcentagem": porcentagem,
            "oferta": oferta,
            "loja": "Jumbo",
            "link": link,
        }

        self.logger.info(f"Encontrado SKU {item['sku']} para EAN {ean}")
        self.logger.info(f"Promo textos encontrados: {promo_texts}")
        self.logger.info(f"Has promo: {has_promo}")
        self.logger.info(f"Item final: {item}")

        yield item

    def normalize_price(self, value):
        if value is None:
            return None

        if isinstance(value, str):
            value = value.strip().replace(",", ".")
            try:
                value = float(value)
            except Exception:
                return None

        if isinstance(value, int):
            value = float(value)

        if not isinstance(value, float):
            return None

        if value >= 100000:
            value = value / 100

        return round(value, 2)

    def collect_promo_texts(self, product, item, offer):
        possible_values = [
            offer.get("discountHighlights"),
            offer.get("DiscountHighLight"),
            offer.get("teasers"),
            offer.get("Teasers"),
            product.get("clusterHighlights"),
            product.get("productClusters"),
            product.get("properties"),
            item.get("referenceId"),
        ]

        texts = []
        for value in possible_values:
            texts.extend(self.extract_texts_from_any(value))

        cleaned = []
        seen = set()

        for text in texts:
            s = re.sub(r"\s+", " ", str(text)).strip()
            if not s:
                continue
            if s.lower() in seen:
                continue
            seen.add(s.lower())
            cleaned.append(s)

        return cleaned

    def extract_texts_from_any(self, value):
        if value is None:
            return []

        if isinstance(value, str):
            return [value]

        texts = []

        if isinstance(value, dict):
            for v in value.values():
                texts.extend(self.extract_texts_from_any(v))
            return texts

        if isinstance(value, list):
            for v in value:
                texts.extend(self.extract_texts_from_any(v))
            return texts

        return []

    def extract_second_unit_average_discount(self, texts):
        if not texts:
            return None

        for text in texts:
            s = str(text).strip()

            match = re.search(r"2do\s+al\s+(\d{1,3})\s*%", s, flags=re.IGNORECASE)
            if not match:
                match = re.search(r"2do\s+al\s+(\d{1,3})\b", s, flags=re.IGNORECASE)

            if match:
                try:
                    second_unit_discount = int(match.group(1))
                    if 0 < second_unit_discount <= 100:
                        return round(second_unit_discount / 2)
                except Exception:
                    return None

        return None

    def has_promotion(self, product, item, offer, promo_texts=None):
        if promo_texts:
            for text in promo_texts:
                s = str(text).strip()
                if not s:
                    continue

                if re.search(r"\d{1,3}\s*%", s):
                    return True

                if re.search(r"\b(2do|segunda|segundo)\b", s, flags=re.IGNORECASE):
                    return True

        promo_sources = [
            offer.get("discountHighlights"),
            offer.get("DiscountHighLight"),
            offer.get("teasers"),
            offer.get("Teasers"),
            product.get("clusterHighlights"),
            product.get("productClusters"),
            product.get("properties"),
            item.get("referenceId"),
        ]

        for value in promo_sources:
            if self.value_has_meaningful_content(value):
                if self.parse_percentage_from_any(value) is not None:
                    return True

                texts = self.extract_texts_from_any(value)
                for text in texts:
                    s = str(text).strip()
                    if not s:
                        continue
                    if re.search(r"\b(2do|segunda|segundo)\b", s, flags=re.IGNORECASE):
                        return True

        return False

    def value_has_meaningful_content(self, value):
        if value is None:
            return False

        if isinstance(value, str):
            return bool(value.strip())

        if isinstance(value, dict):
            return any(self.value_has_meaningful_content(v) for v in value.values())

        if isinstance(value, list):
            return any(self.value_has_meaningful_content(v) for v in value)

        return False

    def extract_discount_dynamic(self, product, item, offer):
        possible_values = [
            offer.get("discountHighlights"),
            offer.get("DiscountHighLight"),
            offer.get("teasers"),
            offer.get("Teasers"),
            product.get("clusterHighlights"),
            product.get("productClusters"),
            product.get("properties"),
            item.get("referenceId"),
        ]

        for value in possible_values:
            pct = self.parse_percentage_from_any(value)
            if pct is not None:
                return pct

        return None

    def parse_percentage_from_any(self, value):
        if value is None:
            return None

        if isinstance(value, str):
            return self.parse_percentage(value)

        if isinstance(value, dict):
            for v in value.values():
                pct = self.parse_percentage_from_any(v)
                if pct is not None:
                    return pct

        if isinstance(value, list):
            for v in value:
                pct = self.parse_percentage_from_any(v)
                if pct is not None:
                    return pct

        return None

    def parse_percentage(self, value):
        if not value:
            return None

        s = str(value).strip()
        match = re.search(r"(\d{1,3})\s*%", s)
        if match:
            try:
                pct = int(match.group(1))
                if 0 < pct <= 100:
                    return pct
            except Exception:
                return None

        return None

    def calc_discount(self, preco_de, preco_por):
        if preco_de is None or preco_por is None:
            return None
        if preco_de <= preco_por:
            return None
        return round(((preco_de - preco_por) / preco_de) * 100)

    def empty_item(self, ean):
        return {
            "ean": ean,
            "sku": None,
            "nome": None,
            "marca": None,
            "precoDe": None,
            "precoPor": None,
            "porcentagem": None,
            "oferta": None,
            "loja": "Jumbo",
            "link": None,
        }