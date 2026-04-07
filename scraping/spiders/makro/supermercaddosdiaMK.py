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

            for row in reader:
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

        # tenta achar linha de cabeçalho "real"
        for idx, row in enumerate(rows[:20]):
            nomes_linha = [str(h).strip().lower() if h is not None else "" for h in row]
            if any(
                n
                in (
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

        # fallback: primeira linha não vazia
        if header_row_idx is None:
            for idx, row in enumerate(rows):
                if any(c is not None and str(c).strip() for c in row):
                    header_row_idx = idx
                    header = [str(h).strip() if h is not None else "" for h in row]
                    break

        if header_row_idx is None or header is None:
            raise ValueError("Não encontrei nenhuma linha de cabeçalho na planilha.")

        header_norm = [h.lower() for h in header]
        nomes = {c: idx for idx, c in enumerate(header_norm)}

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

        # se ainda não encontrou, usa a primeira coluna não vazia
        if idx_ean is None:
            for i, h in enumerate(header):
                if h:
                    idx_ean = i
                    break

        if idx_ean is None:
            raise ValueError(
                f"Não encontrei coluna de EAN no XLSX. Cabeçalho detectado: {header}"
            )

        eans = []
        for row in rows[header_row_idx + 1 :]:
            if not row or idx_ean >= len(row):
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

    def extrair_preco(self, texto: str):
        if not texto:
            return None

        texto = " ".join(texto.split())
        padroes = [
            r"\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?",
        ]

        for padrao in padroes:
            m = re.search(padrao, texto, flags=re.IGNORECASE)
            if m:
                return m.group(0)

        return None

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

            eans = self._ler_eans_arquivo(self.arquivo_entrada)[:50]
            self.logger.info("Processando %d EANs do arquivo", len(eans))

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

        # 1) tentar achar links de produto na listagem
        links = sel.css("a::attr(href)").getall()
        links_produto = []

        for href in links:
            if not href:
                continue
            href = href.strip()

            # VTEX costuma usar /p no final para PDP
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

        # 2) fallback: tentar usar informação da própria listagem
        cards = sel.css("article, section, div")
        encontrou_algo = False

        for card in cards:
            html_card = card.get() or ""
            if "$" not in html_card:
                continue

            nome = self.extrair_nome_listagem(card)
            textos = [t.strip() for t in card.css("::text").getall() if t and t.strip()]
            bloco = " ".join(textos)
            preco = self.extrair_preco(bloco)

            if nome or preco:
                encontrou_algo = True
                yield {
                    "loja": "dia_ar",
                    "tipo_busca": tipo_busca,
                    "busca_valor": busca_valor,
                    "ean": busca_valor if tipo_busca == "ean" else None,
                    "nome": nome,
                    "marca": self.extrair_marca(nome),
                    "preco": preco,
                    "link": response.url,
                    "status_busca": "resultado_em_listagem",
                }

        if not encontrou_algo:
            # 3) nada encontrado
            yield {
                "loja": "dia_ar",
                "tipo_busca": tipo_busca,
                "busca_valor": busca_valor,
                "ean": busca_valor if tipo_busca == "ean" else None,
                "nome": None,
                "marca": None,
                "preco": None,
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

        # nome do produto: pega o primeiro h1 limpo da página
        nome = sel.css("h1::text").get()
        if nome:
            nome = " ".join(nome.split())
        else:
            # fallback: tenta seletores específicos da VTEX
            nome = sel.css(
                ".vtex-store-components-3-x-productNameContainer *::text, "
                ".vtex-product-name-1-x-productName::text"
            ).get()
            if nome:
                nome = " ".join(nome.split())

        # pegar todo o texto da página e extrair o primeiro preço
        textos = [t.strip() for t in sel.css("body ::text").getall() if t and t.strip()]
        bloco = " ".join(textos)
        preco = self.extrair_preco(bloco)

        yield {
            "loja": "dia_ar",
            "tipo_busca": tipo_busca,
            "busca_valor": busca_valor,
            "ean": busca_valor if tipo_busca == "ean" else None,
            "nome": nome,
            "marca": self.extrair_marca(nome),
            "preco": preco,
            "link": link or response.url,
            "status_busca": "encontrado",
        }