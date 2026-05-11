import csv
import json
import re
from pathlib import Path
from urllib.parse import quote, urljoin

import scrapy
from scrapy import Request

try:
    import openpyxl
except ImportError:
    openpyxl = None

try:
    from scrapy_playwright.page import PageMethod
except ImportError:
    PageMethod = None


class CarrefourPrecoSpider(scrapy.Spider):
    name = "carrefour_preco"
    allowed_domains = ["www.carrefour.com.ar", "carrefour.com.ar"]

    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 0.4,
        "DOWNLOAD_TIMEOUT": 60,
        "LOG_LEVEL": "INFO",
        "COOKIES_ENABLED": True,
    }

    PASTA_LEITURA = Path(r"C:\Users\Felipe Braga\Desktop\trabalho\WebScraping\scraping\Leitura")
    ARQUIVO_PADRAO = PASTA_LEITURA / "BASE RETAIL MINORISTA 23_02_2026.xlsx"

    PALAVRAS_COMBO = [
        "3x2", "2x1", "4x3", "5x4",
        "3 x 2", "2 x 1", "4 x 3", "5 x 4",
        "2do al", "segunda unidad", "segundo al",
        "combinable", "combinables", "max ", "máx",
        "unidades", "unidad", "c/u", "cada uno",
        "llevando", "lleva", "pack",
    ]

    def __init__(self, arquivo_entrada=None, ean=None, usar_playwright="0", *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ean = ean
        self.usar_playwright = str(usar_playwright).strip().lower() in ("1", "true", "yes", "sim")
        self.arquivo_entrada = Path(arquivo_entrada) if arquivo_entrada else self.ARQUIVO_PADRAO

    # ---------------- start ----------------

    def start_requests(self):
        eans = []

        if self.ean:
            ean = self.sanitize_ean(self.ean)
            if self.ean_valido(ean):
                eans.append(ean)
        elif self.arquivo_entrada:
            eans.extend(self.ler_eans_arquivo(self.arquivo_entrada))

        eans = list(dict.fromkeys([e for e in eans if self.ean_valido(e)]))

        for ean in eans:
            api_url = self.build_api_ean_url(ean)
            yield Request(
                api_url,
                callback=self.parse_api,
                meta={"ean": ean, "api_kind": "ean"},
                dont_filter=True,
            )

    # ---------------- leitura ----------------

    def resolver_caminho_arquivo(self, arquivo_entrada):
        caminho = Path(arquivo_entrada)

        if caminho.exists():
            return caminho

        if not caminho.is_absolute():
            candidato = self.PASTA_LEITURA / caminho.name
            if candidato.exists():
                return candidato

        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

    def ler_eans_csv(self, caminho: Path):
        eans = []

        with caminho.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            if not reader.fieldnames:
                raise ValueError("CSV sem cabeçalho.")

            nomes = {c.lower().strip(): c for c in reader.fieldnames}
            coluna_ean = (
                nomes.get("ean")
                or nomes.get("código ean")
                or nomes.get("codigo ean")
                or nomes.get("codigo_ean")
                or nomes.get("codigoean")
                or nomes.get("ean 13")
                or nomes.get("cod ean")
                or nomes.get("cod_ean")
            )

            if not coluna_ean:
                raise ValueError(f"Não encontrei coluna EAN no CSV. Cabeçalho: {reader.fieldnames}")

            for row in reader:
                valor = (row.get(coluna_ean) or "").strip()
                if valor:
                    eans.append(self.sanitize_ean(valor))

        return list(dict.fromkeys(eans))

    def ler_eans_xlsx(self, caminho: Path):
        if openpyxl is None:
            raise RuntimeError("openpyxl não instalado. Rode: pip install openpyxl")

        wb = openpyxl.load_workbook(str(caminho), read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]
        rows = list(ws.iter_rows(values_only=True))
        wb.close()

        if not rows:
            raise ValueError("Planilha vazia.")

        header_row_idx = None
        for i, row in enumerate(rows):
            if not row:
                continue
            vals = [str(c).strip().lower() if c is not None else "" for c in row]
            if any("ean" in v for v in vals):
                header_row_idx = i
                break

        if header_row_idx is None:
            raise ValueError("Não encontrei cabeçalho com EAN.")

        header = [str(h).strip() if h is not None else "" for h in rows[header_row_idx]]
        header_norm = [h.lower() for h in header]
        nomes = {c: idx for idx, c in enumerate(header_norm)}

        idx_ean = None
        for chave in [
            "ean", "código ean", "codigo ean", "codigo_ean",
            "codigoean", "ean 13", "cod ean", "cod_ean"
        ]:
            if chave in nomes:
                idx_ean = nomes[chave]
                break

        if idx_ean is None:
            for nome_coluna, idx in nomes.items():
                if "ean" in nome_coluna:
                    idx_ean = idx
                    break

        if idx_ean is None:
            raise ValueError(f"Não encontrei coluna EAN no XLSX. Cabeçalho: {header}")

        eans = []
        for row in rows[header_row_idx + 1:]:
            if row is None or idx_ean >= len(row):
                continue
            valor = row[idx_ean]
            if valor is None:
                continue
            valor = self.sanitize_ean(valor)
            if valor:
                eans.append(valor)

        return list(dict.fromkeys(eans))

    def ler_eans_arquivo(self, arquivo_entrada):
        caminho = self.resolver_caminho_arquivo(arquivo_entrada)
        self.logger.info("Arquivo de entrada localizado em: %s", caminho)

        if caminho.suffix.lower() == ".csv":
            return self.ler_eans_csv(caminho)
        if caminho.suffix.lower() == ".xlsx":
            return self.ler_eans_xlsx(caminho)

        raise ValueError("Use arquivo .csv ou .xlsx")

    # ---------------- utils ----------------

    def sanitize_ean(self, value):
        return re.sub(r"\D", "", str(value or ""))

    def ean_valido(self, ean):
        return bool(ean) and len(ean) in (8, 12, 13, 14)

    def normalize_text(self, value):
        if value is None:
            return None
        value = re.sub(r"\s+", " ", str(value)).strip()
        return value or None

    def parse_price(self, value):
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        texto = str(value)
        texto = texto.replace("\xa0", " ")
        texto = re.sub(r"\s+", "", texto)
        texto = texto.replace("$", "")

        m = re.search(r"(\d{1,3}(?:\.\d{3})+,\d{2}|\d{1,3}(?:\.\d{3})+|\d+,\d{2}|\d+)", texto)
        if not m:
            return None

        texto = m.group(1)

        if "," in texto:
            texto = texto.replace(".", "").replace(",", ".")
        else:
            # Inteiro estilo 4.008 -> 4008
            texto = texto.replace(".", "")

        try:
            return float(texto)
        except Exception:
            return None

    def build_api_ean_url(self, ean):
        fq = quote(f"alternateIds_Ean:{ean}", safe="")
        return f"https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq={fq}"

    def build_api_ft_url(self, ean):
        return f"https://www.carrefour.com.ar/api/catalog_system/pub/products/search?ft={quote(ean, safe='')}"

    def build_search_url(self, ean):
        return f"https://www.carrefour.com.ar/{quote(ean)}?_q={quote(ean)}&map=ft"

    def is_pdp_url(self, url):
        if not url:
            return False
        base = url.split("?")[0]
        return base.endswith("/p") or "/p/" in base

    def abs_url(self, url):
        if not url:
            return None
        return urljoin("https://www.carrefour.com.ar", url)

    def get_default_seller(self, product):
        sellers = product.get("items", [{}])[0].get("sellers", [])
        if not sellers:
            return {}
        return next((s for s in sellers if s.get("sellerDefault")), sellers[0])

    def get_offer(self, product):
        seller = self.get_default_seller(product)
        return seller.get("commertialOffer") or {}

    def extract_json_ld_blocks(self, response):
        blocos = []
        for raw in response.css('script[type="application/ld+json"]::text').getall():
            raw = raw.strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
                blocos.append(data)
            except Exception:
                continue
        return blocos

    def find_offer_in_jsonld(self, data):
        if isinstance(data, dict):
            if data.get("@type") == "Offer":
                return data
            for v in data.values():
                achou = self.find_offer_in_jsonld(v)
                if achou:
                    return achou
        elif isinstance(data, list):
            for item in data:
                achou = self.find_offer_in_jsonld(item)
                if achou:
                    return achou
        return None

    def extract_price_candidates_from_text(self, text, skip_combo_promos=True):
        if not text:
            return []

        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)

        padrao = r"\$\s*\d[\d\s\.\,]*\d"
        candidatos = []

        for match in re.finditer(padrao, text):
            trecho = match.group(0)

            if skip_combo_promos:
                inicio = max(0, match.start() - 90)
                fim = min(len(text), match.end() + 90)
                contexto = text[inicio:fim].lower()
                if any(p in contexto for p in self.PALAVRAS_COMBO):
                    continue

            preco = self.parse_price(trecho)
            if preco is not None:
                candidatos.append(preco)

        unicos = []
        for v in candidatos:
            if v not in unicos:
                unicos.append(v)
        return unicos

    def extract_combo_promotion(self, text):
        if not text:
            return None

        texto = re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()
        lower = texto.lower()

        m = re.search(r"(\d+)\s*[xX]\s*(\d+)", texto)
        if m:
            qtd = int(m.group(1))
            paga = int(m.group(2))
            precos = self.extract_price_candidates_from_text(texto, skip_combo_promos=False)
            return {
                "tipo": f"{qtd}x{paga}",
                "texto": texto[:250],
                "preco_unitario": precos[0] if precos else None,
                "preco_total": precos[1] if len(precos) > 1 else None,
            }

        m2 = re.search(r"2do\s+al\s+(\d+)%", lower)
        if m2:
            perc = int(m2.group(1))
            precos = self.extract_price_candidates_from_text(texto, skip_combo_promos=False)
            return {
                "tipo": f"2do_al_{perc}",
                "texto": texto[:250],
                "preco_unitario": precos[0] if precos else None,
                "preco_total": precos[1] if len(precos) > 1 else None,
            }

        return None

    def extract_prices_from_dom(self, response):
        seletores = [
            ".vtex-product-price-1-x-sellingPriceValue *::text",
            ".vtex-product-price-1-x-currencyContainer *::text",
            ".vtex-product-price-1-x-listPriceValue *::text",
            ".vtex-store-components-3-x-productPriceContainer *::text",
            ".price-box *::text",
            ".product-price *::text",
            ".pdp-price *::text",
            "main *::text",
        ]

        textos = []
        for sel in seletores:
            vals = response.css(sel).getall()
            if vals:
                textos.extend(vals)

        texto_unico = " ".join([t.strip() for t in textos if t and t.strip()])
        texto_unico = re.sub(r"\s+", " ", texto_unico).strip()

        precos_sem_combo = self.extract_price_candidates_from_text(texto_unico, skip_combo_promos=True)
        combo = self.extract_combo_promotion(texto_unico)

        preco_por = precos_sem_combo[0] if precos_sem_combo else None
        preco_de = None

        if len(precos_sem_combo) >= 2:
            maior = max(precos_sem_combo[:3])
            menor = min(precos_sem_combo[:3])
            if maior > menor:
                preco_de = maior
                preco_por = menor

        return {
            "texto": texto_unico,
            "precoPor": preco_por,
            "precoDe": preco_de,
            "combo": combo,
        }

    def pick_product_from_api(self, products, ean):
        if not isinstance(products, list):
            return None

        for product in products:
            for item in product.get("items", []) or []:
                eans = [
                    self.sanitize_ean(item.get("ean")),
                    self.sanitize_ean(product.get("productReference")),
                    self.sanitize_ean(product.get("productReferenceCode")),
                ]
                if ean in eans:
                    return product

        return products[0] if products else None

    def extract_product_fields_from_api(self, product):
        if not product:
            return {}

        item0 = (product.get("items") or [{}])[0]
        offer = self.get_offer(product)

        link = (
            product.get("link")
            or product.get("linkText")
            or product.get("detailUrl")
        )
        if link and not str(link).startswith("http"):
            if not str(link).startswith("/"):
                link = "/" + str(link)
            link = self.abs_url(link)

        out = {
            "nome": self.normalize_text(product.get("productName") or product.get("name")),
            "marca": self.normalize_text(product.get("brand")),
            "sku": item0.get("itemId"),
            "link": link,
            "price_raw": offer.get("Price") or offer.get("spotPrice"),
            "list_price_raw": offer.get("ListPrice"),
            "price_without_discount_raw": offer.get("PriceWithoutDiscount") or offer.get("priceWithoutDiscount"),
        }
        return out

    def normalize_prices(self, item):
        preco = self.parse_price(item.get("preco") or item.get("precoPor"))
        preco_por = self.parse_price(item.get("precoPor") or item.get("preco"))
        preco_de = self.parse_price(item.get("precoDe"))

        promo_tipo = item.get("promo_tipo")
        promo_texto = item.get("promo_texto")
        promo_preco_unitario = self.parse_price(item.get("promo_preco_unitario"))
        promo_preco_total = self.parse_price(item.get("promo_preco_total"))

        if preco is None and preco_por is not None:
            preco = preco_por
        if preco_por is None and preco is not None:
            preco_por = preco

        if preco_de is not None and preco_por is not None and preco_de <= preco_por:
            preco_de = None

        oferta = None
        desconto_percentual = None

        if preco_de is not None and preco_por is not None and preco_de > preco_por:
            oferta = "x"
            try:
                desconto_percentual = round((1 - (preco_por / preco_de)) * 100, 2)
            except Exception:
                desconto_percentual = None

        # Promo de combo não vira preço riscado clássico
        if promo_tipo and not oferta:
            oferta = None

        item["preco"] = preco
        item["precoPor"] = preco_por
        item["precoDe"] = preco_de
        item["oferta"] = oferta
        item["desconto_percentual"] = desconto_percentual

        item["promo_tipo"] = promo_tipo
        item["promo_texto"] = promo_texto
        item["promo_preco_unitario"] = promo_preco_unitario
        item["promo_preco_total"] = promo_preco_total

        return item

    # ---------------- parse api ----------------

    def parse_api(self, response):
        ean = response.meta["ean"]

        try:
            data = json.loads(response.text)
        except Exception:
            data = None

        product = self.pick_product_from_api(data, ean) if data else None

        if not product:
            ft_url = self.build_api_ft_url(ean)
            yield Request(
                ft_url,
                callback=self.parse_api_ft,
                meta={"ean": ean},
                dont_filter=True,
            )
            return

        base = self.extract_product_fields_from_api(product)

        item = {
            "loja": "Carrefour",
            "ean": ean,
            "sku": base.get("sku"),
            "nome": base.get("nome"),
            "marca": base.get("marca"),
            "preco": base.get("price_raw"),
            "precoPor": base.get("price_raw"),
            "precoDe": base.get("list_price_raw") or base.get("price_without_discount_raw"),
            "oferta": None,
            "desconto_percentual": None,
            "promo_tipo": None,
            "promo_texto": None,
            "promo_preco_unitario": None,
            "promo_preco_total": None,
            "price_raw": base.get("price_raw"),
            "list_price_raw": base.get("list_price_raw"),
            "price_without_discount_raw": base.get("price_without_discount_raw"),
            "link": base.get("link"),
        }

        item = self.normalize_prices(item)

        pdp_url = item.get("link") or self.build_search_url(ean)

        if self.usar_playwright and PageMethod is not None:
            yield Request(
                pdp_url,
                callback=self.parse_pdp_playwright,
                meta={
                    "ean": ean,
                    "item_base": item,
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                    ],
                },
                errback=self.errback_close_page,
                dont_filter=True,
            )
        else:
            yield Request(
                pdp_url,
                callback=self.parse_pdp,
                meta={"ean": ean, "item_base": item},
                dont_filter=True,
            )

    def parse_api_ft(self, response):
        ean = response.meta["ean"]

        try:
            data = json.loads(response.text)
        except Exception:
            data = None

        product = self.pick_product_from_api(data, ean) if data else None

        if not product:
            yield {
                "loja": "Carrefour",
                "ean": ean,
                "sku": None,
                "nome": None,
                "marca": None,
                "preco": None,
                "precoPor": None,
                "precoDe": None,
                "oferta": None,
                "desconto_percentual": None,
                "promo_tipo": None,
                "promo_texto": None,
                "promo_preco_unitario": None,
                "promo_preco_total": None,
                "price_raw": None,
                "list_price_raw": None,
                "price_without_discount_raw": None,
                "link": self.build_search_url(ean),
            }
            return

        base = self.extract_product_fields_from_api(product)

        item = {
            "loja": "Carrefour",
            "ean": ean,
            "sku": base.get("sku"),
            "nome": base.get("nome"),
            "marca": base.get("marca"),
            "preco": base.get("price_raw"),
            "precoPor": base.get("price_raw"),
            "precoDe": base.get("list_price_raw") or base.get("price_without_discount_raw"),
            "oferta": None,
            "desconto_percentual": None,
            "promo_tipo": None,
            "promo_texto": None,
            "promo_preco_unitario": None,
            "promo_preco_total": None,
            "price_raw": base.get("price_raw"),
            "list_price_raw": base.get("list_price_raw"),
            "price_without_discount_raw": base.get("price_without_discount_raw"),
            "link": base.get("link"),
        }

        item = self.normalize_prices(item)

        pdp_url = item.get("link") or self.build_search_url(ean)

        yield Request(
            pdp_url,
            callback=self.parse_pdp,
            meta={"ean": ean, "item_base": item},
            dont_filter=True,
        )

    # ---------------- parse pdp ----------------

    def merge_pdp_data(self, response, item):
        dom = self.extract_prices_from_dom(response)
        texto_dom = dom.get("texto")
        combo = dom.get("combo")

        if dom.get("precoPor") is not None:
            item["preco"] = dom.get("precoPor")
            item["precoPor"] = dom.get("precoPor")

        if dom.get("precoDe") is not None:
            item["precoDe"] = dom.get("precoDe")

        if combo:
            item["promo_tipo"] = combo.get("tipo")
            item["promo_texto"] = combo.get("texto")
            item["promo_preco_unitario"] = combo.get("preco_unitario")
            item["promo_preco_total"] = combo.get("preco_total")

        # JSON-LD fallback
        for bloco in self.extract_json_ld_blocks(response):
            offer = self.find_offer_in_jsonld(bloco)
            if not offer:
                continue

            # price atual
            jsonld_preco = offer.get("price")
            if jsonld_preco is None and isinstance(offer.get("priceSpecification"), dict):
                jsonld_preco = offer["priceSpecification"].get("price")

            if item.get("precoPor") is None and jsonld_preco is not None:
                item["preco"] = jsonld_preco
                item["precoPor"] = jsonld_preco

            # preço original/strikethrough
            ps = offer.get("priceSpecification")
            preco_de_jsonld = None

            if isinstance(ps, dict):
                price_type = str(ps.get("priceType") or "")
                if "StrikethroughPrice" in price_type:
                    preco_de_jsonld = ps.get("price")
            elif isinstance(ps, list):
                for p in ps:
                    if not isinstance(p, dict):
                        continue
                    price_type = str(p.get("priceType") or "")
                    if "StrikethroughPrice" in price_type:
                        preco_de_jsonld = p.get("price")
                        break

            if item.get("precoDe") is None and preco_de_jsonld is not None:
                item["precoDe"] = preco_de_jsonld

        # nome fallback
        if not item.get("nome"):
            nome = response.css("h1 *::text").getall()
            if nome:
                item["nome"] = self.normalize_text(" ".join(nome))

        item["link"] = response.url
        item["debug_texto_preco"] = texto_dom[:500] if texto_dom else None

        return self.normalize_prices(item)

    def parse_pdp(self, response):
        item = dict(response.meta["item_base"])
        item = self.merge_pdp_data(response, item)
        yield item

    async def parse_pdp_playwright(self, response):
        item = dict(response.meta["item_base"])
        page = response.meta.get("playwright_page")

        try:
            item = self.merge_pdp_data(response, item)
            yield item
        finally:
            if page:
                await page.close()

    async def errback_close_page(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()