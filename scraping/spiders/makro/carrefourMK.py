import csv
import json
import re
from pathlib import Path
from urllib.parse import quote

from scrapy import Request, Spider

try:
    import openpyxl
except ImportError:
    openpyxl = None

try:
    from scrapy_playwright.page import PageMethod
except ImportError:
    PageMethod = None


class CarrefourMKSpider(Spider):
    name = "carrefour_mk"
    allowed_domains = ["www.carrefour.com.ar", "carrefour.com.ar"]

    custom_settings = {
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 0.3,
        "LOG_LEVEL": "INFO",
        "COOKIES_ENABLED": True,
    }

    PALAVRAS_RUINS = [
        "off", "descuento", "descuentos", "promocion", "promoción", "promociones",
        "oferta", "ofertas", "seleccionados", "categorias", "categorías",
        "semana", "billetera", "beneficio", "ahorro", "hasta ",
    ]

    def __init__(self, arquivo_entrada=None, ean=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not arquivo_entrada and not ean:
            raise ValueError("Passe arquivo_entrada ou ean")
        self.arquivo_entrada = arquivo_entrada
        self.ean = ean

    # ---------------- leitura ----------------

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
                    eans.append(valor)

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

        # achar linha de cabeçalho, ignorando linhas de título tipo "RELEVAMIENTO MENSUAL"
        header_row_idx = None
        for i, row in enumerate(rows):
            if not row:
                continue

            if any(c is not None and str(c).strip() for c in row):
                header_candidate = [str(c).strip().lower() if c is not None else "" for c in row]

                # se não tem "ean" em nenhuma célula, provavelmente é título -> continua procurando
                if not any("ean" in h for h in header_candidate):
                    continue

                header_row_idx = i
                break

        if header_row_idx is None:
            raise ValueError("Não encontrei linha de cabeçalho com coluna EAN na planilha.")

        header = [str(h).strip() if h is not None else "" for h in rows[header_row_idx]]
        header_normalizado = [h.lower() for h in header]
        nomes = {c: idx for idx, c in enumerate(header_normalizado)}

        # coluna EAN
        idx_ean = (
            nomes.get("ean")
            or nomes.get("código ean")
            or nomes.get("codigo ean")
            or nomes.get("codigo_ean")
            or nomes.get("codigoean")
            or nomes.get("ean 13")
            or nomes.get("cod ean")
            or nomes.get("cod_ean")
        )

        if idx_ean is None:
            for nome_coluna, idx in nomes.items():
                if "ean" in nome_coluna:
                    idx_ean = idx
                    break

        if idx_ean is None:
            raise ValueError(f"Não encontrei coluna EAN no XLSX. Cabeçalho: {header}")

        # tentar achar coluna do mercado/competidor
        idx_mercado = None
        candidatos_mercado = [
            "competidor",
            "competidor ",
            "supermercado",
            "mercado",
            "cliente",
        ]
        for nome_coluna, idx in nomes.items():
            if nome_coluna in candidatos_mercado:
                idx_mercado = idx
                break

        eans = []
        for row in rows[header_row_idx + 1:]:
            if row is None or idx_ean >= len(row):
                continue

            # se tiver coluna de mercado, filtra só Carrefour
            if idx_mercado is not None and idx_mercado < len(row):
                mercado_val = row[idx_mercado]
                mercado_txt = str(mercado_val or "").lower()
                if "carrefour" not in mercado_txt:
                    continue

            valor = row[idx_ean]
            if valor is None:
                continue

            valor = str(valor).strip()
            if valor:
                eans.append(valor)

        return list(dict.fromkeys(eans))

    def ler_eans_arquivo(self, arquivo_entrada):
        caminho = Path(arquivo_entrada)

        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

        if caminho.suffix.lower() == ".csv":
            eans = self.ler_eans_csv(caminho)
        elif caminho.suffix.lower() == ".xlsx":
            eans = self.ler_eans_xlsx(caminho)
        else:
            raise ValueError("Use arquivo .csv ou .xlsx")

        return eans

    # ---------------- utils ----------------

    def sanitize_ean(self, value):
        return re.sub(r"\D", "", str(value or ""))

    def ean_valido(self, ean):
        return bool(ean) and len(ean) in (8, 12, 13, 14)

    def parse_price(self, value):
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        texto = str(value).strip()
        texto = texto.replace("$", "").replace("\xa0", " ")
        texto = texto.replace(".", "").replace(",", ".")
        texto = re.sub(r"[^\d.]", "", texto)

        try:
            return float(texto) if texto else None
        except Exception:
            return None

    def normalize_text(self, value):
        if value is None:
            return None
        value = re.sub(r"\s+", " ", str(value)).strip()
        return value or None

    def texto_ruim(self, texto):
        if not texto:
            return False
        t = texto.lower()
        return any(p in t for p in self.PALAVRAS_RUINS)

    def clean_nome(self, nome, ean):
        nome = self.normalize_text(nome)
        if not nome:
            return None

        nome_limpo = re.sub(r"\s*-\s*Carrefour\s*$", "", nome, flags=re.I).strip()

        if ean and re.sub(r"\D", "", nome_limpo) == str(ean):
            return None

        if ean and nome_limpo == f"{ean} - Carrefour":
            return None

        if self.texto_ruim(nome_limpo):
            return None

        return nome_limpo

    def pick_best_seller(self, sellers):
        if not sellers:
            return None

        melhor = None
        melhor_preco = None

        for seller in sellers:
            offer = seller.get("commertialOffer") or {}
            preco = offer.get("Price")
            if preco is None:
                preco = offer.get("spotPrice")
            if preco is None:
                preco = offer.get("ListPrice")

            if preco is None:
                continue

            if melhor is None or preco < melhor_preco:
                melhor = seller
                melhor_preco = preco

        return melhor or sellers[0]

    def build_search_url(self, ean):
        return f"https://www.carrefour.com.ar/{quote(ean)}?_q={quote(ean)}&map=ft"

    def build_api_ean_url(self, ean):
        fq = quote(f"alternateIds_Ean:{ean}", safe="")
        return f"https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq={fq}"

    def build_api_ft_url(self, ean):
        ft = quote(ean, safe="")
        return f"https://www.carrefour.com.ar/api/catalog_system/pub/products/search?ft={ft}"

    def is_pdp_url(self, url):
        if not url:
            return False
        base = url.split("?")[0]
        return base.endswith("/p") or "/p/" in base or base.endswith("/p")

    def extract_items_from_api_product(self, produto, ean, response):
        product_name = (
            produto.get("productName")
            or produto.get("productTitle")
            or produto.get("name")
        )

        brand = produto.get("brand")
        link = produto.get("link") or produto.get("url")
        if link and link.startswith("/"):
            link = response.urljoin(link)

        preco = None
        sku_ean = None

        items = produto.get("items") or []
        item_match = None

        for item in items:
            item_ean = self.sanitize_ean(item.get("ean"))
            ref_ids = item.get("referenceId") or []
            ref_texto = " ".join(str(x.get("Value") or "") for x in ref_ids if isinstance(x, dict))

            if item_ean == ean or ean in self.sanitize_ean(ref_texto):
                item_match = item
                sku_ean = item_ean or ean
                break

        if item_match is None and items:
            item_match = items[0]
            sku_ean = self.sanitize_ean(item_match.get("ean")) or ean

        if item_match:
            sellers = item_match.get("sellers") or []
            seller = self.pick_best_seller(sellers)
            if seller:
                offer = seller.get("commertialOffer") or {}
                preco = (
                    offer.get("Price")
                    or offer.get("spotPrice")
                    or offer.get("ListPrice")
                )

            item_link = item_match.get("detailUrl")
            if item_link and item_link.startswith("/"):
                item_link = response.urljoin(item_link)

            if item_link:
                link = item_link

        return {
            "ean": ean,
            "nome": self.clean_nome(product_name, ean),
            "marca": self.normalize_text(brand),
            "preco": self.parse_price(preco),
            "loja": "carrefour_ar",
            "link": link,
            "sku_ean_encontrado": sku_ean,
        }

    # ---------------- start ----------------

    async def start(self):
        if self.arquivo_entrada:
            eans_brutos = self.ler_eans_arquivo(self.arquivo_entrada)
        else:
            eans_brutos = [self.ean]

        for ean_bruto in eans_brutos:
            ean = self.sanitize_ean(ean_bruto)

            if not self.ean_valido(ean):
                self.logger.warning(
                    "EAN inválido ignorado | original=%s | sanitizado=%s",
                    ean_bruto,
                    ean,
                )
                yield {
                    "ean": ean,
                    "nome": None,
                    "marca": None,
                    "preco": None,
                    "loja": "carrefour_ar",
                    "link": None,
                }
                continue

            api_url = self.build_api_ean_url(ean)

            self.logger.info("API EAN | valor=%s | URL=%s", ean, api_url)

            yield Request(
                url=api_url,
                callback=self.parse_api_ean,
                dont_filter=True,
                meta={"ean_atual": ean},
                headers={"Accept": "application/json, text/plain, */*"},
            )

    # ---------------- tentativa 1 ----------------

    def parse_api_ean(self, response):
        ean = response.meta["ean_atual"]

        self.logger.info("API EAN RESPONSE | STATUS=%s | URL=%s", response.status, response.url)

        try:
            data = json.loads(response.text)
        except Exception:
            data = None

        if isinstance(data, list) and data:
            self.logger.info("API EAN RESPONSE | produtos=%d", len(data))
            item = self.extract_items_from_api_product(data[0], ean, response)

            self.logger.info(
                "API EAN FINAL | EAN=%s | nome=%s | preco=%s | link=%s",
                item["ean"], item["nome"], item["preco"], item["link"]
            )

            if item["link"] and self.is_pdp_url(item["link"]):
                yield Request(
                    url=item["link"],
                    callback=self.parse_produto,
                    dont_filter=True,
                    meta={
                        "ean_atual": ean,
                        "item_base": item,
                        "playwright": True,
                        "playwright_include_page": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_load_state", "networkidle"),
                            PageMethod("wait_for_timeout", 1500),
                        ] if PageMethod else [],
                    },
                )
                return

            item.pop("sku_ean_encontrado", None)
            yield item
            return

        self.logger.warning("API EAN sem resultados para EAN=%s", ean)

        api_ft_url = self.build_api_ft_url(ean)
        self.logger.info("API FT | valor=%s | URL=%s", ean, api_ft_url)

        yield Request(
            url=api_ft_url,
            callback=self.parse_api_ft,
            dont_filter=True,
            meta={"ean_atual": ean},
            headers={"Accept": "application/json, text/plain, */*"},
        )

    # ---------------- tentativa 2 ----------------

    def parse_api_ft(self, response):
        ean = response.meta["ean_atual"]

        self.logger.info("API FT RESPONSE | STATUS=%s | URL=%s", response.status, response.url)

        try:
            data = json.loads(response.text)
        except Exception:
            data = None

        if isinstance(data, list) and data:
            self.logger.info("API FT RESPONSE | produtos=%d", len(data))

            melhor = None

            for produto in data:
                extraido = self.extract_items_from_api_product(produto, ean, response)

                nome = extraido.get("nome") or ""
                sku_ean = str(extraido.get("sku_ean_encontrado") or "").strip()
                score = 0

                if sku_ean == ean:
                    score += 1000
                if extraido.get("preco") is not None:
                    score += 200
                if extraido.get("nome"):
                    score += 50
                if extraido.get("link") and self.is_pdp_url(extraido.get("link")):
                    score += 100
                if nome and not self.texto_ruim(nome):
                    score += 20

                extraido["_score"] = score

                if melhor is None or extraido["_score"] > melhor["_score"]:
                    melhor = extraido

            if melhor:
                self.logger.info(
                    "API FT FINAL | EAN=%s | nome=%s | preco=%s | link=%s",
                    melhor["ean"], melhor["nome"], melhor["preco"], melhor["link"]
                )

                melhor.pop("_score", None)
                melhor.pop("sku_ean_encontrado", None)

                if melhor["link"] and self.is_pdp_url(melhor["link"]):
                    yield Request(
                        url=melhor["link"],
                        callback=self.parse_produto,
                        dont_filter=True,
                        meta={
                            "ean_atual": ean,
                            "item_base": melhor,
                            "playwright": True,
                            "playwright_include_page": True,
                            "playwright_page_methods": [
                                PageMethod("wait_for_load_state", "networkidle"),
                                PageMethod("wait_for_timeout", 1500),
                            ] if PageMethod else [],
                        },
                    )
                    return

                yield melhor
                return

        self.logger.warning("API FT sem resultados para EAN=%s", ean)

        search_url = self.build_search_url(ean)
        self.logger.info("HTML BUSCA | valor=%s | URL=%s", ean, search_url)

        yield Request(
            url=search_url,
            callback=self.parse_busca_html,
            dont_filter=True,
            meta={"ean_atual": ean},
        )

    # ---------------- tentativa 3 ----------------

    def parse_busca_html(self, response):
        ean = response.meta["ean_atual"]

        self.logger.info("HTML RESPONSE | STATUS=%s | URL=%s", response.status, response.url)

        links = response.css('a[href*="/p"]::attr(href)').getall()
        links_validos = []

        for href in links:
            href_abs = response.urljoin(href)

            if "/promociones" in href_abs:
                continue

            if href_abs not in links_validos:
                links_validos.append(href_abs)

        self.logger.info("HTML BUSCA | links_pdp_validos=%d", len(links_validos))

        if links_validos:
            yield Request(
                url=links_validos[0],
                callback=self.parse_produto,
                dont_filter=True,
                meta={
                    "ean_atual": ean,
                    "item_base": {
                        "ean": ean,
                        "nome": None,
                        "marca": None,
                        "preco": None,
                        "loja": "carrefour_ar",
                        "link": links_validos[0],
                    },
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 1500),
                    ] if PageMethod else [],
                },
            )
            return

        self.logger.warning(
            "Busca sem PDP no HTML cru. Vou renderizar categoria e procurar EAN=%s dentro dela | URL=%s",
            ean,
            response.url,
        )

        yield Request(
            url=response.url,
            callback=self.parse_categoria_playwright,
            dont_filter=True,
            meta={
                "ean_atual": ean,
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                    PageMethod("wait_for_timeout", 3000),
                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 2000),
                ] if PageMethod else [],
            },
        )

    # ---------------- categoria renderizada ----------------

    async def parse_categoria_playwright(self, response):
        ean = response.meta["ean_atual"]
        page = response.meta.get("playwright_page")

        if page is None:
            self.logger.warning("Playwright page não veio no response para EAN=%s", ean)
            yield {
                "ean": ean,
                "nome": None,
                "marca": None,
                "preco": None,
                "loja": "carrefour_ar",
                "link": response.url,
            }
            return

        try:
            produto_info = await page.evaluate(
                """(ean) => {
                    const normalizar = (txt) => (txt || '').replace(/\\s+/g, ' ').trim();
                    const soDigitos = (txt) => (txt || '').replace(/\\D/g, '');

                    const candidatos = Array.from(document.querySelectorAll('a, article, section, div, li'));
                    const encontrados = [];

                    for (const el of candidatos) {
                        const texto = normalizar(el.textContent || '');
                        if (!texto) continue;

                        const textoDigitos = soDigitos(texto);
                        const hrefEl = el.matches('a[href*="/p"]') ? el : el.querySelector('a[href*="/p"]');
                        const href = hrefEl ? hrefEl.href : null;

                        const datasetStr = JSON.stringify(el.dataset || {});
                        const html = el.outerHTML || '';

                        const bateEan =
                            textoDigitos.includes(ean) ||
                            soDigitos(datasetStr).includes(ean) ||
                            soDigitos(html).includes(ean);

                        if (!bateEan) continue;

                        const nomeEl =
                            el.querySelector('h1, h2, h3, h4') ||
                            el.querySelector('[class*="name"]') ||
                            el.querySelector('[class*="productName"]') ||
                            el.querySelector('[class*="product-name"]') ||
                            el.querySelector('span');

                        const precoEl =
                            el.querySelector('.valtech-carrefourar-product-price-0-x-currencyContainer') ||
                            el.querySelector('[class*="price"]') ||
                            el.querySelector('[data-testid*="price"]');

                        encontrados.push({
                            nome: normalizar(nomeEl ? nomeEl.textContent : texto.slice(0, 180)),
                            preco: normalizar(precoEl ? precoEl.textContent : null),
                            link: href,
                            texto: texto.slice(0, 500),
                        });
                    }

                    const comPdp = encontrados.filter(x => x.link && x.link.includes('/p'));
                    if (comPdp.length) return comPdp[0];
                    if (encontrados.length) return encontrados[0];
                    return null;
                }""",
                ean,
            )
        except Exception as exc:
            self.logger.warning("Falha no evaluate da categoria | EAN=%s | erro=%s", ean, exc)
            produto_info = None

        await page.close()

        if not produto_info:
            self.logger.warning(
                "Não encontrei o EAN dentro da categoria renderizada | EAN=%s | URL=%s",
                ean,
                response.url,
            )
            yield {
                "ean": ean,
                "nome": None,
                "marca": None,
                "preco": None,
                "loja": "carrefour_ar",
                "link": response.url,
            }
            return

        link = produto_info.get("link")
        nome = self.clean_nome(produto_info.get("nome"), ean)
        preco = self.parse_price(produto_info.get("preco"))

        self.logger.info(
            "CATEGORIA PLAYWRIGHT | EAN=%s | nome=%s | preco=%s | link=%s",
            ean, nome, preco, link
        )

        if link and self.is_pdp_url(link):
            yield Request(
                url=link,
                callback=self.parse_produto,
                dont_filter=True,
                meta={
                    "ean_atual": ean,
                    "item_base": {
                        "ean": ean,
                        "nome": nome,
                        "marca": None,
                        "preco": preco,
                        "loja": "carrefour_ar",
                        "link": link,
                    },
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                        PageMethod("wait_for_timeout", 2000),
                    ] if PageMethod else [],
                },
            )
            return

        yield {
            "ean": ean,
            "nome": nome,
            "marca": None,
            "preco": preco,
            "loja": "carrefour_ar",
            "link": link or response.url,
        }

    # ---------------- PDP ----------------

    async def parse_produto(self, response):
        ean = response.meta["ean_atual"]
        base = response.meta.get("item_base") or {}
        page = response.meta.get("playwright_page")

        if "/promociones" in response.url:
            self.logger.warning("URL de promo genérica ignorada | EAN=%s | URL=%s", ean, response.url)
            if page:
                await page.close()
            yield {
                "ean": ean,
                "nome": base.get("nome"),
                "marca": base.get("marca"),
                "preco": base.get("preco"),
                "loja": "carrefour_ar",
                "link": response.url,
            }
            return

        nome = (
            response.css("h1::text").get()
            or response.css('[class*="productName"]::text').get()
            or response.css("title::text").get()
            or base.get("nome")
        )
        nome = self.clean_nome(nome, ean) or base.get("nome")

        marca = (
            response.css('[class*="productBrand"]::text').get()
            or response.css('[class*="brand"]::text').get()
            or base.get("marca")
        )
        marca = self.normalize_text(marca)

        preco = base.get("preco")

        partes_preco = response.css(
            ".valtech-carrefourar-product-price-0-x-currencyContainer *::text"
        ).getall()
        texto_preco = "".join(p.strip() for p in partes_preco if p.strip()) or None
        if texto_preco:
            preco = self.parse_price(texto_preco)

        if page and preco is None:
            try:
                texto_js = await page.evaluate(
                    """() => {
                        const el =
                            document.querySelector('.valtech-carrefourar-product-price-0-x-currencyContainer') ||
                            document.querySelector('[class*="price"]');
                        return el ? el.textContent.trim() : null;
                    }"""
                )
                if texto_js:
                    preco = self.parse_price(texto_js)
            except Exception as exc:
                self.logger.warning("Falha ao ler preço via Playwright | EAN=%s | erro=%s", ean, exc)

        if preco is None:
            for bloco in response.css('script[type="application/ld+json"]::text').getall():
                try:
                    data = json.loads(bloco)
                except Exception:
                    continue

                estruturas = data if isinstance(data, list) else [data]

                for obj in estruturas:
                    if not isinstance(obj, dict):
                        continue

                    if not nome:
                        nome = self.clean_nome(obj.get("name"), ean) or nome

                    brand = obj.get("brand")
                    if isinstance(brand, dict):
                        brand = brand.get("name")
                    if not marca:
                        marca = self.normalize_text(brand)

                    offers = obj.get("offers") or {}
                    if isinstance(offers, list):
                        offers = offers[0] if offers else {}

                    preco = (
                        preco
                        or obj.get("price")
                        or offers.get("price")
                        or offers.get("lowPrice")
                        or offers.get("highPrice")
                    )

                    if preco is not None:
                        break

                if preco is not None:
                    break

        if page:
            await page.close()

        item = {
            "ean": ean,
            "nome": nome,
            "marca": marca,
            "preco": self.parse_price(preco),
            "loja": "carrefour_ar",
            "link": response.url,
        }

        self.logger.info(
            "PDP FINAL | EAN=%s | nome=%s | preco=%s | link=%s",
            item["ean"], item["nome"], item["preco"], item["link"]
        )

        yield item