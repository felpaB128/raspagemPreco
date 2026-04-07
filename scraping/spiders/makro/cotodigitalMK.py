import csv
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import quote, urljoin

from scrapy import Spider, Request, Selector

try:
    import openpyxl
except ImportError:
    openpyxl = None


class CotoDigitalMKSpider(Spider):
    name = "cotodigital_mk"
    allowed_domains = [
        "www.cotodigital.com.ar",
        "cotodigital.com.ar",
        "api.cotodigital.com.ar",
    ]

    custom_settings = {
        "ZYTE_API_TRANSPARENT_MODE": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.2,
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    # -----------------------------
    # init
    # -----------------------------
    def __init__(
        self,
        ean=None,
        arquivo_entrada=None,
        termo=None,
        modo="ambos",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if not ean and not arquivo_entrada and not termo:
            raise ValueError(
                "Passe ean, termo ou arquivo_entrada.\n"
                "Ex.: -a ean=190679000019 | -a termo=cerveza | "
                "-a arquivo_entrada='produtos.xlsx'"
            )

        self.ean = ean
        self.termo = termo
        self.arquivo_entrada = arquivo_entrada
        self.modo = (modo or "ambos").strip().lower()

        if self.modo not in {"browser", "api", "ambos"}:
            raise ValueError("modo deve ser: browser, api ou ambos")

    # -----------------------------
    # resolução de caminho / leitura
    # -----------------------------
    def _resolver_caminho_arquivo(self, caminho_str: str) -> Path:
        caminho = Path(caminho_str).expanduser()

        if caminho.is_absolute():
            return caminho

        candidatos = [
            Path.cwd() / caminho,
            Path.cwd() / caminho.name,
        ]

        for cand in candidatos:
            if cand.exists():
                return cand

        return candidatos[0]

    def _normalizar_coluna(self, texto):
        if texto is None:
            return ""
        texto = str(texto).strip().lower()
        texto = unicodedata.normalize("NFKD", texto)
        texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
        texto = re.sub(r"[^a-z0-9]+", " ", texto)
        return " ".join(texto.split())

    def _valor_limpo(self, valor):
        if valor is None:
            return ""
        return str(valor).strip()

    def _mapear_colunas(self, fieldnames):
        nomes = {self._normalizar_coluna(c): c for c in fieldnames if c}

        def pick(*aliases):
            for alias in aliases:
                chave = self._normalizar_coluna(alias)
                if chave in nomes:
                    return nomes[chave]
            return None

        return {
            "ean": pick(
                "ean",
                "codigo ean",
                "código ean",
                "codigo_ean",
                "codigoean",
                "ean 13",
                "cod ean",
                "cod_ean",
            ),
            "nome": pick(
                "nome",
                "producto",
                "produto",
                "descripcion",
                "descrição",
                "descricao",
                "articulo",
            ),
            "marca": pick("marca", "brand"),
        }

    def _montar_produto_de_row(self, row, colunas):
        ean = (
            self._valor_limpo(row.get(colunas["ean"]))
            if colunas.get("ean")
            else ""
        )
        if not ean:
            return None

        produto = {
            "ean": ean,
            "nome": self._valor_limpo(row.get(colunas["nome"]))
            if colunas.get("nome")
            else "",
            "marca": self._valor_limpo(row.get(colunas["marca"]))
            if colunas.get("marca")
            else "",
        }
        return produto

    def _ler_produtos_csv(self, caminho: Path):
        produtos = []
        with caminho.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV sem cabeçalho.")

            colunas = self._mapear_colunas(reader.fieldnames)

            if not colunas["ean"]:
                raise ValueError(
                    f"Não encontrei coluna de EAN no CSV. Cabeçalho: {reader.fieldnames}"
                )

            for row in reader:
                produto = self._montar_produto_de_row(row, colunas)
                if produto:
                    produtos.append(produto)

        vistos = set()
        unicos = []
        for p in produtos:
            chave = (p["ean"], self._normalizar(p.get("nome")))
            if chave not in vistos:
                vistos.add(chave)
                unicos.append(p)
        return unicos

    def _ler_produtos_xlsx(self, caminho: Path):
        if openpyxl is None:
            raise RuntimeError(
                "openpyxl não está instalado. Rode: pip install openpyxl"
            )

        wb = openpyxl.load_workbook(
            str(caminho), read_only=True, data_only=True
        )
        ws = wb[wb.sheetnames[0]]
        rows = list(ws.iter_rows(values_only=True))
        wb.close()

        if not rows:
            raise ValueError("Planilha vazia.")

        header_row_idx = None
        header = None

        for idx, row in enumerate(rows[:40]):
            nomes_linha = [
                self._normalizar_coluna(h) if h is not None else ""
                for h in row
            ]
            if any(
                n
                in {
                    "ean",
                    "codigo ean",
                    "ean 13",
                    "cod ean",
                    "codigoean",
                }
                for n in nomes_linha
            ):
                header_row_idx = idx
                header = [
                    str(h).strip() if h is not None else "" for h in row
                ]
                break

        if header_row_idx is None or header is None:
            raise ValueError(
                "Não encontrei linha de cabeçalho com coluna de EAN no XLSX."
            )

        colunas_map = self._mapear_colunas(header)
        if not colunas_map["ean"]:
            raise ValueError(
                f"Não encontrei coluna de EAN no XLSX. Cabeçalho: {header}"
            )

        idx_por_nome = {nome: i for i, nome in enumerate(header)}

        produtos = []
        for row in rows[header_row_idx + 1 :]:
            if not row:
                continue

            row_dict = {}
            for nome_col, idx in idx_por_nome.items():
                row_dict[nome_col] = row[idx] if idx < len(row) else None

            produto = self._montar_produto_de_row(row_dict, colunas_map)
            if produto:
                produtos.append(produto)

        vistos = set()
        unicos = []
        for p in produtos:
            chave = (p["ean"], self._normalizar(p.get("nome")))
            if chave not in vistos:
                vistos.add(chave)
                unicos.append(p)
        return unicos

    def _ler_produtos_arquivo(self, caminho_str: str):
        caminho = self._resolver_caminho_arquivo(caminho_str)

        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

        if caminho.suffix.lower() == ".csv":
            return self._ler_produtos_csv(caminho)
        elif caminho.suffix.lower() == ".xlsx":
            return self._ler_produtos_xlsx(caminho)

        raise ValueError("Arquivo de entrada deve ser .csv ou .xlsx")

    # -----------------------------
    # urls
    # -----------------------------
    def montar_url_busca_browser(self, termo: str) -> str:
        termo = quote(str(termo).strip())
        return (
            "https://www.cotodigital.com.ar/sitios/cdigi/nuevositio?Ntt="
            f"{termo}"
        )

    def montar_urls_api(self, termo: str):
        termo = quote(str(termo).strip())
        return {
            (
                "https://www.cotodigital.com.ar/"
                "sitios/cdigi/categoria?_dyncharset=utf-8&Dy=1&Ntt="
                f"{termo}"
            ),
            (
                "https://www.cotodigital.com.ar/"
                "sitios/cdigi/browse/_/N-1z141we?Ntt="
                f"{termo}"
            ),
        }

    def _get_selector(self, response):
        raw = getattr(response, "raw_api_response", None) or {}
        browser_html = raw.get("browserHtml")
        if browser_html:
            return Selector(text=browser_html)
        return response

    # -----------------------------
    # util texto / matching
    # -----------------------------
    def limpar_texto(self, texto):
        if not texto:
            return None
        return " ".join(str(texto).split()).strip()

    def _strip_accents(self, txt: str):
        if not txt:
            return ""
        txt = unicodedata.normalize("NFKD", str(txt))
        return "".join(ch for ch in txt if not unicodedata.combining(ch))

    def _normalizar(self, txt: str):
        if not txt:
            return ""
        txt = self._strip_accents(str(txt)).lower()
        txt = re.sub(r"[^a-z0-9]+", " ", txt)
        return " ".join(txt.split())

    def _tokenizar(self, txt: str):
        txt = self._normalizar(txt)
        if not txt:
            return []
        return [t for t in txt.split() if t]

    def _palavras_relevantes(self, txt: str, limite=6):
        tokens = self._tokenizar(txt)
        stop = {
            "fid",
            "pq",
            "pqx",
            "x500g",
            "x1k",
            "x1kg",
            "de",
            "del",
            "la",
            "el",
            "con",
            "sin",
            "fort",
            "v",
            "zinc",
            "for",
            "n",
            "no",
        }
        saida = []
        for t in tokens:
            if t in stop:
                continue
            if re.fullmatch(r"\d+", t):
                continue
            saida.append(t)
        return saida[:limite]

    def _gerar_consultas_produto(self, produto: dict):
        """
        Ordem simples de prioridade para reduzir volume:
        1) EAN
        2) nome completo normalizado
        3) marca + palavras relevantes do nome
        """
        consultas = []

        ean = self._valor_limpo(produto.get("ean"))
        nome = self._valor_limpo(produto.get("nome"))
        marca = self._valor_limpo(produto.get("marca"))

        nome_norm = self._normalizar(nome)
        marca_norm = self._normalizar(marca)
        palavras_nome = self._palavras_relevantes(nome, limite=5)

        def add(tipo, valor, prioridade):
            valor = self.limpar_texto(valor)
            if valor:
                consultas.append((tipo, valor, prioridade))

        if ean:
            add("ean", ean, 1)

        if nome_norm:
            add("nome_completo", nome_norm, 2)

        if marca_norm and palavras_nome:
            add("marca_palavras", f"{marca_norm} {' '.join(palavras_nome)}", 3)

        vistos = set()
        saida = []
        for tipo, valor, prioridade in consultas:
            chave = self._normalizar(f"{tipo}|{valor}")
            if chave not in vistos:
                vistos.add(chave)
                saida.append((tipo, valor, prioridade))
        return saida

    # -----------------------------
    # preço / marca / sem resultado
    # -----------------------------
    def extrair_preco(self, texto: str):
        if not texto:
            return None

        texto = " ".join(texto.split())
        padroes = [
            r"\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?",
            r"\d{1,3}(?:\.\d{3})*,\d{2}",
        ]

        for padrao in padroes:
            m = re.search(padrao, texto, flags=re.IGNORECASE)
            if m:
                valor = m.group(0)
                if not valor.startswith("$"):
                    valor = f"$ {valor}"
                return valor

        return None

    def extrair_marca(self, nome):
        if not nome:
            return None
        partes = self.limpar_texto(nome).split()
        return partes[0] if partes else None

    def detectar_sem_resultado(self, texto):
        if not texto:
            return False
        t = texto.lower()
        sinais = [
            "no se encontraron resultados",
            "no encontramos productos",
            "sin resultados",
            "ningún resultado",
            "ningun resultado",
        ]
        return any(s in t for s in sinais)

    # -----------------------------
    # score de match (simples)
    # -----------------------------
    def _pontuar_match(
        self, produto_origem, nome_encontrado, busca_valor, tipo_busca
    ):
        score = 0
        if not produto_origem:
            return score

        nome_encontrado_norm = self._normalizar(nome_encontrado)
        nome_origem = self._normalizar(produto_origem.get("nome"))
        marca_origem = self._normalizar(produto_origem.get("marca"))
        ean_origem = self._valor_limpo(produto_origem.get("ean"))

        if tipo_busca == "ean" and busca_valor == ean_origem:
            score += 100

        if nome_origem and nome_origem in nome_encontrado_norm:
            score += 40

        tokens_nome = set(self._palavras_relevantes(nome_origem, limite=8))
        tokens_achado = set(self._tokenizar(nome_encontrado_norm))
        inter = tokens_nome & tokens_achado
        score += len(inter) * 8

        if marca_origem and marca_origem in nome_encontrado_norm:
            score += 15

        return score

    # -----------------------------
    # HTML → produto
    # -----------------------------
    def extrair_produto_html(self, sel: Selector):
        candidatos = []

        blocos = sel.css(
            "div.product, li.product, .product-item, .producto, "
            ".product-card, .search-results-item, .prod_details, "
            ".product_info_wrapper"
        )

        for bloco in blocos[:80]:
            nome = self.limpar_texto(
                bloco.css(
                    "h1::text, h2::text, h3::text, "
                    ".product-name::text, .productName::text, "
                    ".nombre-producto::text, .descrip_full::text, "
                    "a::attr(title), a::text"
                ).get()
            )
            textos = [
                self.limpar_texto(t)
                for t in bloco.css("::text").getall()
                if self.limpar_texto(t)
            ]
            bloco_texto = " ".join(textos)
            preco = self.extrair_preco(bloco_texto)

            link = bloco.css("a::attr(href)").get()
            if link:
                link = urljoin(
                    self.settings.get(
                        "START_URL", "https://www.cotodigital.com.ar"
                    ),
                    link,
                )

            if nome or preco:
                candidatos.append(
                    {
                        "nome": nome,
                        "preco": preco,
                        "link": link,
                        "texto": bloco_texto,
                    }
                )

        if candidatos:
            escolhido = max(
                candidatos,
                key=lambda x: (
                    1 if x.get("preco") else 0,
                    len(x.get("nome") or ""),
                ),
            )
            return {
                "nome": escolhido.get("nome"),
                "preco": escolhido.get("preco"),
                "link": escolhido.get("link"),
            }

        # Se não achei bloco de produto, não inventar usando <title>
        return None

    # -----------------------------
    # JSON → produto (fallback genérico)
    # -----------------------------
    def extrair_produto_json(self, data):
        if isinstance(data, dict):
            nome_keys = [
                "name",
                "displayName",
                "productDisplayName",
                "description",
            ]
            preco_keys = ["price", "listPrice", "salePrice", "precio"]
            link_keys = ["url", "link", "productUrl"]

            nome = None
            preco = None
            link = None

            for k in nome_keys:
                if data.get(k):
                    nome = self.limpar_texto(data.get(k))
                    break

            for k in preco_keys:
                v = data.get(k)
                if v is None:
                    continue
                if isinstance(v, (int, float)):
                    preco = (
                        f"$ {v:,.2f}"
                        .replace(",", "X")
                        .replace(".", ",")
                        .replace("X", ".")
                    )
                else:
                    preco = self.extrair_preco(str(v)) or self.limpar_texto(v)
                if preco:
                    break

            for k in link_keys:
                if data.get(k):
                    link = self.limpar_texto(data.get(k))
                    break

            if nome or preco:
                return {"nome": nome, "preco": preco, "link": link}

            for v in data.values():
                achou = self.extrair_produto_json(v)
                if achou:
                    return achou

        elif isinstance(data, list):
            for item in data:
                achou = self.extrair_produto_json(item)
                if achou:
                    return achou

        return None

    # -----------------------------
    # montar item
    # -----------------------------
    def montar_item(
        self,
        response,
        tipo_busca,
        busca_valor,
        prioridade_busca=None,
        nome=None,
        preco=None,
        link=None,
        origem="browser",
        produto_origem=None,
    ):
        nome = self.limpar_texto(nome)
        preco = self.limpar_texto(preco)

        status = (
            "resultado_busca" if (nome or preco) else "nao_indexado_na_busca"
        )

        ean_entrada = None
        nome_entrada = None
        marca_entrada = None
        score_match = 0

        if produto_origem:
            ean_entrada = produto_origem.get("ean")
            nome_entrada = produto_origem.get("nome")
            marca_entrada = produto_origem.get("marca")
            score_match = self._pontuar_match(
                produto_origem, nome, busca_valor, tipo_busca
            )

        return {
            "loja": "cotodigital_ar",
            "origem_extracao": origem,
            "tipo_busca": tipo_busca,
            "prioridade_busca": prioridade_busca,
            "busca_valor": busca_valor,
            "ean": ean_entrada
            if ean_entrada
            else (busca_valor if tipo_busca == "ean" else None),
            "nome": nome,
            "marca": self.extrair_marca(nome) or marca_entrada,
            "preco": preco,
            "link": link or response.url,
            "status_busca": status,
            "score_match": score_match,
            "ean_entrada": ean_entrada,
            "nome_entrada": nome_entrada,
            "marca_entrada": marca_entrada,
        }

    # -----------------------------
    # start
    # -----------------------------
    async def start(self):
        if self.arquivo_entrada:
            caminho_resolvido = self._resolver_caminho_arquivo(
                self.arquivo_entrada
            )
            self.logger.info(
                "Lendo arquivo de entrada: %s", caminho_resolvido
            )

            produtos = self._ler_produtos_arquivo(self.arquivo_entrada)
            self.logger.info(
                "Processando %d produtos do arquivo", len(produtos)
            )

            for produto in produtos:
                consultas = self._gerar_consultas_produto(produto)
                self.logger.info(
                    "Produto EAN=%s | nome=%s | consultas=%s",
                    produto.get("ean"),
                    produto.get("nome"),
                    [f"{tipo}:{valor}" for tipo, valor, _ in consultas],
                )

                for tipo_busca, valor_busca, prioridade in consultas:
                    for req in self._gerar_requests_busca(
                        valor_busca, tipo_busca, prioridade
                    ):
                        req.meta.setdefault("produto_origem", produto)
                        yield req
            return

        valor_busca = self.termo or self.ean
        tipo_busca = "termo" if self.termo else "ean"
        prioridade = 1

        for req in self._gerar_requests_busca(
            str(valor_busca), tipo_busca, prioridade
        ):
            yield req

    def _gerar_requests_busca(
        self, valor_busca, tipo_busca, prioridade_busca=1
    ):
        if self.modo in {"browser", "ambos"}:
            yield Request(
                url=self.montar_url_busca_browser(valor_busca),
                callback=self.parse_search_browser,
                dont_filter=True,
                meta={
                    "busca_valor": valor_busca,
                    "tipo_busca": tipo_busca,
                    "prioridade_busca": prioridade_busca,
                    "tentativa_origem": "browser",
                    "zyte_api_automap": {
                        "browserHtml": True,
                        "actions": [
                            {"action": "waitForTimeout", "timeout": 2},
                            {"action": "scrollBottom"},
                            {"action": "waitForTimeout", "timeout": 2},
                            {"action": "scrollBottom"},
                            {"action": "waitForTimeout", "timeout": 1},
                        ],
                    },
                },
            )

        if self.modo in {"api", "ambos"}:
            for url in self.montar_urls_api(valor_busca):
                yield Request(
                    url=url,
                    callback=self.parse_search_api,
                    dont_filter=True,
                    meta={
                        "busca_valor": valor_busca,
                        "tipo_busca": tipo_busca,
                        "prioridade_busca": prioridade_busca,
                        "tentativa_origem": "api",
                    },
                    headers={
                        "Accept": (
                            "text/html,application/json,application/"
                            "xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                        ),
                        "User-Agent": "Mozilla/5.0",
                    },
                )

    # -----------------------------
    # parse modo browser
    # -----------------------------
    def parse_search_browser(self, response):
        busca_valor = response.meta.get("busca_valor")
        tipo_busca = response.meta.get("tipo_busca")
        prioridade_busca = response.meta.get("prioridade_busca")
        produto_origem = response.meta.get("produto_origem")
        sel = self._get_selector(response)

        self.logger.info(
            "[BROWSER] Busca %s | prioridade=%s | valor=%s | URL=%s",
            tipo_busca,
            prioridade_busca,
            busca_valor,
            response.url,
        )

        textos = [
            self.limpar_texto(t)
            for t in sel.css("body ::text").getall()
            if self.limpar_texto(t)
        ]
        corpo = " ".join(textos)

        if self.detectar_sem_resultado(corpo):
            yield self.montar_item(
                response=response,
                tipo_busca=tipo_busca,
                busca_valor=busca_valor,
                prioridade_busca=prioridade_busca,
                nome=None,
                preco=None,
                origem="browser",
                produto_origem=produto_origem,
            )
            return

        produto = self.extrair_produto_html(sel) or {}

        link = produto.get("link")
        if link and link.startswith("/"):
            link = response.urljoin(link)

        yield self.montar_item(
            response=response,
            tipo_busca=tipo_busca,
            busca_valor=busca_valor,
            prioridade_busca=prioridade_busca,
            nome=produto.get("nome"),
            preco=produto.get("preco"),
            link=link,
            origem="browser",
            produto_origem=produto_origem,
        )

    # -----------------------------
    # parse modo api
    # -----------------------------
    def parse_search_api(self, response):
        busca_valor = response.meta.get("busca_valor")
        tipo_busca = response.meta.get("tipo_busca")
        prioridade_busca = response.meta.get("prioridade_busca")
        produto_origem = response.meta.get("produto_origem")

        self.logger.info(
            "[API] Busca %s | prioridade=%s | valor=%s | status=%s | URL=%s | content-type=%s",
            tipo_busca,
            prioridade_busca,
            busca_valor,
            response.status,
            response.url,
            response.headers.get("Content-Type", b"").decode(
                "utf-8", "ignore"
            ),
        )

        content_type = response.headers.get("Content-Type", b"").decode(
            "utf-8", "ignore"
        ).lower()

        nome = None
        preco = None
        link = None

        # 1) JSON
        if "json" in content_type:
            try:
                data = json.loads(response.text)
                produto = self.extrair_produto_json(data)
                if produto:
                    nome = produto.get("nome")
                    preco = produto.get("preco")
                    link = produto.get("link")
            except Exception as e:
                self.logger.debug(
                    "Falha ao interpretar JSON em %s: %s", response.url, e
                )

        # 2) HTML
        if not nome and not preco:
            sel = Selector(text=response.text)
            textos = [
                self.limpar_texto(t)
                for t in sel.css("body ::text").getall()
                if self.limpar_texto(t)
            ]
            corpo = " ".join(textos)

            if self.detectar_sem_resultado(corpo):
                yield self.montar_item(
                    response=response,
                    tipo_busca=tipo_busca,
                    busca_valor=busca_valor,
                    prioridade_busca=prioridade_busca,
                    nome=None,
                    preco=None,
                    origem="api",
                    produto_origem=produto_origem,
                )
                return

            produto = self.extrair_produto_html(sel)
            if produto:
                nome = produto.get("nome")
                preco = produto.get("preco")
                link = produto.get("link")

        if link and link.startswith("/"):
            link = response.urljoin(link)

        yield self.montar_item(
            response=response,
            tipo_busca=tipo_busca,
            busca_valor=busca_valor,
            prioridade_busca=prioridade_busca,
            nome=nome,
            preco=preco,
            link=link,
            origem="api",
            produto_origem=produto_origem,
        )