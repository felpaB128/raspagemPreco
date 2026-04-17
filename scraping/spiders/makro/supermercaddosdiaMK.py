import csv
import re
from pathlib import Path
from urllib.parse import quote

from scrapy import Spider, Request, Selector

try:
    import openpyxl
except ImportError:
    openpyxl = None


class SupermercadosDiaMKSpider(Spider):
    name = "supermercadosdia_mk"
    allowed_domains = ["diaonline.supermercadosdia.com.ar", "supermercadosdia.com.ar"]

    custom_settings = {
        "ZYTE_API_TRANSPARENT_MODE": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.2,
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self, ean=None, arquivo_entrada=None, termo=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not ean and not arquivo_entrada and not termo:
            raise ValueError(
                "Passe ean, termo ou arquivo_entrada. "
                "Ex.: -a ean=190679000019 | -a termo=celula | -a arquivo_entrada=arquivo.xlsx"
            )

        self.ean = ean
        self.termo = termo
        self.arquivo_entrada = arquivo_entrada

    # -----------------------------
    # resolução de caminho / leitura
    # -----------------------------
    def _resolver_caminho_arquivo(self, caminho_str: str) -> Path:
        caminho = Path(caminho_str).expanduser()

        if caminho.is_absolute():
            return caminho

        candidatos = [
            Path.cwd() / caminho,
            Path(__file__).resolve().parents[4] / caminho,
            Path(__file__).resolve().parents[3] / caminho,
        ]

        for candidato in candidatos:
            if candidato.exists():
                return candidato

        return candidatos[0]

    def _eh_competidor_dia(self, valor: str) -> bool:
        if not valor:
            return False
        v = valor.strip().lower()
        return "dia" in v

    def _ler_eans_csv(self, caminho: Path):
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
                raise ValueError(
                    f"Não encontrei coluna de EAN no CSV. Cabeçalho: {reader.fieldnames}"
                )

            coluna_competidor = (
                nomes.get("competidor")
                or nomes.get("competidor ")
                or nomes.get("concorrente")
            )

            for row in reader:
                if coluna_competidor:
                    valor_comp = row.get(coluna_competidor) or ""
                    if not self._eh_competidor_dia(valor_comp):
                        continue

                valor = (row.get(coluna_ean) or "").strip()
                if valor:
                    eans.append(valor)

        return list(dict.fromkeys(eans))

    def _ler_eans_xlsx(self, caminho: Path):
        if openpyxl is None:
            raise RuntimeError("openpyxl não está instalado. Rode: pip install openpyxl")

        wb = openpyxl.load_workbook(str(caminho), read_only=True, data_only=True)
        ws = wb[wb.sheetnames[0]]
        rows = list(ws.iter_rows(values_only=True))
        wb.close()

        if not rows:
            raise ValueError("Planilha vazia.")

        header_row_idx = None
        header = None

        for idx, row in enumerate(rows[:20]):
            nomes_linha = [str(h).strip().lower() if h is not None else "" for h in row]
            if any(
                n in (
                    "ean",
                    "código ean",
                    "codigo ean",
                    "codigo_ean",
                    "codigoean",
                    "ean 13",
                    "cod ean",
                    "cod_ean",
                )
                for n in nomes_linha
            ):
                header_row_idx = idx
                header = [str(h).strip() if h is not None else "" for h in row]
                break

        if header_row_idx is None:
            for idx, row in enumerate(rows):
                if any(c is not None and str(c).strip() for c in row):
                    header_row_idx = idx
                    header = [str(h).strip() if h is not None else "" for h in row]
                    break

        if header_row_idx is None or header is None:
            raise ValueError("Não encontrei nenhuma linha de cabeçalho na planilha.")

        header_norm = [h.lower().strip() for h in header]
        nomes = {c: idx for idx, c in enumerate(header_norm)}

        idx_ean = None
        for chave in (
            "ean",
            "código ean",
            "codigo ean",
            "codigo_ean",
            "codigoean",
            "ean 13",
            "cod ean",
            "cod_ean",
        ):
            if chave in nomes:
                idx_ean = nomes[chave]
                break

        idx_competidor = None
        for chave in ("competidor", "concorrente"):
            if chave in nomes:
                idx_competidor = nomes[chave]
                break

        if idx_ean is None:
            raise ValueError(
                f"Não encontrei coluna de EAN no XLSX. Cabeçalho detectado: {header}"
            )

        eans = []
        for row in rows[header_row_idx + 1:]:
            if not row:
                continue

            if idx_competidor is not None and idx_competidor < len(row):
                valor_comp = row[idx_competidor]
                if not self._eh_competidor_dia(str(valor_comp) if valor_comp is not None else ""):
                    continue

            if idx_ean >= len(row):
                continue

            valor = row[idx_ean]
            if valor is None:
                continue

            valor = str(valor).strip()
            if valor:
                eans.append(valor)

        return list(dict.fromkeys(eans))

    def _ler_eans_arquivo(self, caminho_str: str):
        caminho = self._resolver_caminho_arquivo(caminho_str)

        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

        if caminho.suffix.lower() == ".csv":
            return self._ler_eans_csv(caminho)
        elif caminho.suffix.lower() == ".xlsx":
            return self._ler_eans_xlsx(caminho)

        raise ValueError("Arquivo de entrada deve ser .csv ou .xlsx")

    # -----------------------------
    # util
    # -----------------------------
    def montar_url_busca(self, termo: str) -> str:
        termo = quote(str(termo).strip())
        return f"https://diaonline.supermercadosdia.com.ar/{termo}?_q={termo}&map=ft"

    def _get_selector(self, response):
        raw = getattr(response, "raw_api_response", None) or {}
        browser_html = raw.get("browserHtml")
        if browser_html:
            return Selector(text=browser_html)
        return response

    def _to_float(self, valor):
        if valor is None:
            return None

        if isinstance(valor, (int, float)):
            return float(valor)

        texto = str(valor).strip()
        if not texto:
            return None

        texto = texto.replace("$", "").replace("\xa0", " ")
        texto = texto.replace(".", "").replace(",", ".")
        texto = re.sub(r"[^\d.]", "", texto)

        try:
            return float(texto) if texto else None
        except Exception:
            return None

    def _price_to_str(self, valor):
        if valor is None:
            return None
        if float(valor).is_integer():
            return str(int(valor))
        return f"{valor:.2f}"

    def aplicar_oferta_flag(self, preco_por, preco_de):
        preco_por_f = self._to_float(preco_por)
        preco_de_f = self._to_float(preco_de)

        if preco_por_f is not None and preco_de_f is not None and preco_de_f > preco_por_f:
            return "x"
        return None

    def normalizar_precos(self, preco=None, preco_por=None, preco_de=None):
        preco_f = self._to_float(preco)
        preco_por_f = self._to_float(preco_por)
        preco_de_f = self._to_float(preco_de)

        if preco_por_f is None and preco_f is not None:
            preco_por_f = preco_f

        oferta = None

        if preco_por_f is not None and preco_de_f is not None and preco_de_f > preco_por_f:
            preco_final = preco_por_f
            oferta = "x"
        else:
            preco_final = preco_por_f if preco_por_f is not None else preco_de_f
            preco_por_f = None
            preco_de_f = None

        return {
            "preco": self._price_to_str(preco_final),
            "precoPor": self._price_to_str(preco_por_f),
            "precoDe": self._price_to_str(preco_de_f),
            "oferta": oferta,
        }

    def extrair_preco_regex(self, texto: str):
        if not texto:
            return None

        texto = " ".join(texto.split())
        padrao = r"\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?"

        m = re.search(padrao, texto, flags=re.IGNORECASE)
        if m:
            return m.group(0)
        return None

    def extrair_todos_precos_regex(self, texto: str):
        if not texto:
            return []

        texto = " ".join(texto.split())
        padrao = r"\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?"
        return re.findall(padrao, texto, flags=re.IGNORECASE)

    def extrair_precos_pdp(self, sel: Selector):
        preco_por = None
        preco_de = None

        seletores_preco_por = [
            ".vtex-product-price-1-x-sellingPriceValue::text",
            ".vtex-product-price-1-x-currencyContainer .vtex-product-price-1-x-sellingPriceValue::text",
            "[class*='sellingPriceValue']::text",
            "[data-testid='price']::text",
        ]

        seletores_preco_de = [
            ".vtex-product-price-1-x-listPriceValue::text",
            "[class*='listPriceValue']::text",
            "[class*='listPrice']::text",
        ]

        for s in seletores_preco_por:
            textos = sel.css(s).getall()
            textos = [" ".join(t.split()) for t in textos if t and t.strip()]
            for t in textos:
                preco = self.extrair_preco_regex(t)
                if preco:
                    preco_por = preco
                    break
            if preco_por:
                break

        for s in seletores_preco_de:
            textos = sel.css(s).getall()
            textos = [" ".join(t.split()) for t in textos if t and t.strip()]
            for t in textos:
                preco = self.extrair_preco_regex(t)
                if preco:
                    preco_de = preco
                    break
            if preco_de:
                break

        if not preco_por:
            textos = [t.strip() for t in sel.css("body ::text").getall() if t and t.strip()]
            bloco = " ".join(textos)
            precos = self.extrair_todos_precos_regex(bloco)
            if precos:
                preco_por = precos[0]
                if len(precos) > 1:
                    preco_de = preco_de or precos[1]

        return self.normalizar_precos(preco=preco_por, preco_por=preco_por, preco_de=preco_de)

    def extrair_precos_card(self, card: Selector):
        preco_por = None
        preco_de = None

        seletores_preco_por = [
            ".vtex-product-price-1-x-sellingPriceValue::text",
            ".vtex-product-price-1-x-currencyContainer .vtex-product-price-1-x-sellingPriceValue::text",
            "[class*='sellingPriceValue']::text",
            "[data-testid='price']::text",
            ".vtex-product-price-1-x-currencyContainer::text",
            "[class*='currencyContainer']::text",
        ]

        seletores_preco_de = [
            ".vtex-product-price-1-x-listPriceValue::text",
            "[class*='listPriceValue']::text",
            "[class*='listPrice']::text",
        ]

        for s in seletores_preco_por:
            textos = card.css(s).getall()
            textos = [" ".join(t.split()) for t in textos if t and t.strip()]
            for t in textos:
                preco = self.extrair_preco_regex(t)
                if preco:
                    preco_por = preco
                    break
            if preco_por:
                break

        for s in seletores_preco_de:
            textos = card.css(s).getall()
            textos = [" ".join(t.split()) for t in textos if t and t.strip()]
            for t in textos:
                preco = self.extrair_preco_regex(t)
                if preco:
                    preco_de = preco
                    break
            if preco_de:
                break

        if not preco_por:
            textos = [t.strip() for t in card.css("::text").getall() if t and t.strip()]
            bloco = " ".join(textos)
            precos = self.extrair_todos_precos_regex(bloco)
            if precos:
                preco_por = precos[0]
                if len(precos) > 1:
                    preco_de = preco_de or precos[1]

        return self.normalizar_precos(preco=preco_por, preco_por=preco_por, preco_de=preco_de)

    def extrair_marca(self, nome):
        if not nome:
            return None
        partes = nome.split()
        return partes[0] if partes else None

    def extrair_nome_listagem(self, card):
        seletores = [
            "h2::text",
            "h3::text",
            ".vtex-product-summary-2-x-productBrand::text",
            ".vtex-product-summary-2-x-productName::text",
            "a::text",
            "span::text",
        ]
        for sel in seletores:
            textos = card.css(sel).getall()
            textos = [" ".join(t.split()) for t in textos if t and t.strip()]
            textos = [
                t
                for t in textos
                if len(t) > 2 and "A un clic de llevarte el producto" not in t
            ]
            if textos:
                return textos[0]
        return None

    # -----------------------------
    # start
    # -----------------------------
    async def start(self):
        if self.arquivo_entrada:
            caminho_resolvido = self._resolver_caminho_arquivo(self.arquivo_entrada)
            self.logger.info("Lendo arquivo de entrada: %s", caminho_resolvido)

            eans = self._ler_eans_arquivo(self.arquivo_entrada)
            self.logger.info("Processando %d EANs do arquivo (Competidor = DIA)", len(eans))

            for ean in eans:
                yield Request(
                    url=self.montar_url_busca(ean),
                    callback=self.parse_search,
                    dont_filter=True,
                    meta={
                        "busca_valor": str(ean),
                        "tipo_busca": "ean",
                        "zyte_api_automap": {
                            "browserHtml": True,
                            "actions": [{"action": "scrollBottom"}],
                        },
                    },
                )
            return

        valor_busca = self.termo or self.ean
        tipo_busca = "termo" if self.termo else "ean"

        yield Request(
            url=self.montar_url_busca(valor_busca),
            callback=self.parse_search,
            dont_filter=True,
            meta={
                "busca_valor": str(valor_busca),
                "tipo_busca": tipo_busca,
                "zyte_api_automap": {
                    "browserHtml": True,
                    "actions": [{"action": "scrollBottom"}],
                },
            },
        )

    # -----------------------------
    # parse busca -> encontrar link produto
    # -----------------------------
    def parse_search(self, response):
        busca_valor = response.meta.get("busca_valor")
        tipo_busca = response.meta.get("tipo_busca")
        sel = self._get_selector(response)

        self.logger.info(
            "Busca %s | valor=%s | URL=%s", tipo_busca, busca_valor, response.url
        )

        links = sel.css("a::attr(href)").getall()
        links_produto = []

        for href in links:
            if not href:
                continue
            href = href.strip()

            if href.endswith("/p") or "/p?" in href or "/product/" in href.lower():
                abs_url = response.urljoin(href)
                if abs_url not in links_produto:
                    links_produto.append(abs_url)

        if links_produto:
            self.logger.info(
                "Encontrados %d links de produto para %s. Entrando em cada PDP.",
                len(links_produto),
                busca_valor,
            )
            for link in links_produto:
                yield Request(
                    url=link,
                    callback=self.parse_produto,
                    dont_filter=True,
                    meta={
                        "busca_valor": busca_valor,
                        "tipo_busca": tipo_busca,
                        "link_produto": link,
                        "zyte_api_automap": {
                            "browserHtml": True,
                        },
                    },
                )
            return

        cards = sel.css("article, section, div")
        encontrou_algo = False

        for card in cards:
            html_card = card.get() or ""
            if "$" not in html_card:
                continue

            nome = self.extrair_nome_listagem(card)
            precos = self.extrair_precos_card(card)

            if nome or precos.get("preco"):
                encontrou_algo = True
                status = (
                    "resultado_em_listagem_com_preco"
                    if precos.get("preco")
                    else "resultado_em_listagem_sem_preco"
                )

                yield {
                    "loja": "dia_ar",
                    "tipo_busca": tipo_busca,
                    "busca_valor": busca_valor,
                    "ean": busca_valor if tipo_busca == "ean" else None,
                    "nome": nome,
                    "marca": self.extrair_marca(nome),
                    "preco": precos.get("preco"),
                    "precoPor": precos.get("precoPor"),
                    "precoDe": precos.get("precoDe"),
                    "oferta": precos.get("oferta"),
                    "link": response.url,
                    "status_busca": status,
                }

        if not encontrou_algo:
            yield {
                "loja": "dia_ar",
                "tipo_busca": tipo_busca,
                "busca_valor": busca_valor,
                "ean": busca_valor if tipo_busca == "ean" else None,
                "nome": None,
                "marca": None,
                "preco": None,
                "precoPor": None,
                "precoDe": None,
                "oferta": None,
                "link": response.url,
                "status_busca": "nao_indexado_na_busca",
            }

    # -----------------------------
    # parse produto (PDP)
    # -----------------------------
    def parse_produto(self, response):
        busca_valor = response.meta.get("busca_valor")
        tipo_busca = response.meta.get("tipo_busca")
        link = response.meta.get("link_produto")
        sel = self._get_selector(response)

        nome = sel.css("h1::text").get()
        if nome:
            nome = " ".join(nome.split())
        else:
            nome = sel.css(
                ".vtex-store-components-3-x-productNameContainer *::text, "
                ".vtex-product-name-1-x-productName::text"
            ).get()
            if nome:
                nome = " ".join(nome.split())

        precos = self.extrair_precos_pdp(sel)
        status = "encontrado_com_preco" if precos.get("preco") else "encontrado_sem_preco"

        yield {
            "loja": "dia_ar",
            "tipo_busca": tipo_busca,
            "busca_valor": busca_valor,
            "ean": busca_valor if tipo_busca == "ean" else None,
            "nome": nome,
            "marca": self.extrair_marca(nome),
            "preco": precos.get("preco"),
            "precoPor": precos.get("precoPor"),
            "precoDe": precos.get("precoDe"),
            "oferta": precos.get("oferta"),
            "link": link or response.url,
            "status_busca": status,
        }