import base64
import csv
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import quote

from scrapy import Spider, Request, Selector

try:
    import openpyxl
except ImportError:
    openpyxl = None


SITES = {
    "jumbo_ar": {
        "base_url_busca": "https://www.jumbo.com.ar/busqueda?q={query}",
        "url_direta": "https://www.jumbo.com.ar/{query}?_q={query}&map=ft",
        "catalog_sku": "https://www.jumbo.com.ar/api/catalog_system/pub/products/search?fq=skuId:{sku}",
        "allowed_domains": [
            "www.jumbo.com.ar",
            "jumbo.com.ar",
        ],
    },
}


class ProdutoPorEanSpider(Spider):
    name = "produto_por_ean"

    custom_settings = {
        "ZYTE_API_TRANSPARENT_MODE": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0.2,
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_FIELDS": [
            "loja",
            "ean",
            "sku",
            "nome",
            "marca",
            "precoDe",
            "precoPor",
            "oferta",
            "desconto_percentual",
            "print_tela_path",
            "link",
        ],
    }

    def __init__(self, ean=None, arquivo_entrada=None, loja=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.loja = (loja or "").strip().lower()
        if self.loja not in SITES:
            raise ValueError(f"Loja '{self.loja}' não suportada. Use: {list(SITES.keys())}")

        if not ean and not arquivo_entrada:
            raise ValueError(
                "Passe ean ou arquivo_entrada. "
                "Ex.: -a ean=789... ou -a arquivo_entrada=C:\\arquivo.xlsx"
            )

        self.ean = str(ean).strip() if ean else None
        self.arquivo_entrada = arquivo_entrada
        self.site_cfg = SITES[self.loja]
        self.allowed_domains = self.site_cfg["allowed_domains"]

        self.prints_dir = Path("prints")
        self.prints_dir.mkdir(exist_ok=True)

        self.itens_emitidos = set()

    async def start(self):
        for req in self._build_start_requests():
            yield req

    def _build_start_requests(self):
        eans = [self.ean] if self.ean else self._ler_eans_arquivo(self.arquivo_entrada)

        for ean in eans:
            ean = self._normalizar_ean(ean)
            if not ean:
                continue

            query_enc = quote(ean)
            url_direta = self.site_cfg["url_direta"].format(query=query_enc)
            url_busca = self.site_cfg["base_url_busca"].format(query=query_enc)

            item_base = {
                "loja": self.loja,
                "ean": ean,
                "sku": None,
                "nome": None,
                "marca": None,
                "precoDe": None,
                "precoPor": None,
                "oferta": None,
                "desconto_percentual": None,
                "print_tela_path": None,
                "link": url_direta,
            }

            self.logger.info("Agendando URL direta para EAN=%s | %s", ean, url_direta)
            yield Request(
                url=url_direta,
                callback=self.parse_produto,
                dont_filter=True,
                meta={
                    "item_base": item_base,
                    "ean_atual": ean,
                    "via_url_direta": True,
                    "zyte_api_automap": {
                        "browserHtml": True,
                        "screenshot": True,
                    },
                },
            )

            self.logger.info("Agendando busca para EAN=%s | %s", ean, url_busca)
            yield Request(
                url=url_busca,
                callback=self.parse_search,
                dont_filter=True,
                meta={
                    "ean_atual": ean,
                    "zyte_api_automap": {
                        "browserHtml": True,
                    },
                },
            )

    def start_requests(self):
        yield from self._build_start_requests()

    # ---------------- Leitura de arquivo ----------------

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
        for i, row in enumerate(rows):
            if row is None:
                continue
            valores = [str(c).strip() for c in row if c is not None and str(c).strip()]
            if valores:
                header_row_idx = i
                break

        if header_row_idx is None:
            raise ValueError("Planilha sem dados.")

        idx_ean = 0
        eans = []
        for row in rows[header_row_idx + 1 :]:
            if row is None or all(c is None for c in row):
                continue
            if idx_ean >= len(row):
                continue
            valor = row[idx_ean]
            if valor is None:
                continue
            valor_str = str(valor).strip()
            if valor_str:
                eans.append(valor_str)

        return list(dict.fromkeys(eans))

    def _ler_eans_arquivo(self, caminho_str: str):
        caminho = Path(caminho_str)
        if not caminho.exists():
            raise FileNotFoundError(f"Arquivo de entrada não encontrado: {caminho}")

        sufixo = caminho.suffix.lower()
        if sufixo == ".csv":
            return self._ler_eans_csv(caminho)
        if sufixo == ".xlsx":
            return self._ler_eans_xlsx(caminho)
        raise ValueError(f"Extensão não suportada: {sufixo}. Use .csv ou .xlsx para arquivo_entrada.")

    # ---------------- Utilidades gerais ----------------

    def slugify(self, texto: str) -> str:
        texto = (texto or "").strip().lower()
        texto = re.sub(r"[^\w\s-]", "", texto, flags=re.UNICODE)
        texto = re.sub(r"[-\s]+", "-", texto)
        return (texto[:120] or "item").strip("-") or "item"

    def normalizar_texto(self, texto):
        if not texto:
            return ""
        texto = str(texto).strip().lower()
        texto = unicodedata.normalize("NFKD", texto)
        texto = "".join(c for c in texto if not unicodedata.combining(c))
        texto = re.sub(r"\s+", " ", texto)
        return texto.strip()

    def _normalizar_ean(self, valor):
        if valor is None:
            return None
        s = re.sub(r"\D", "", str(valor))
        return s or None

    def _normalizar_sku(self, valor):
        if valor is None:
            return None
        s = re.sub(r"\D", "", str(valor))
        return s or None

    def nome_parece_produto(self, texto, ean_atual=None):
        if not texto:
            return False
        txt = self.limpar_nome_candidato(texto)
        txt_norm = self.normalizar_texto(txt)

        if len(txt) < 5:
            return False

        if self.texto_parece_ean(txt):
            return False
        if ean_atual and txt == str(ean_atual):
            return False
        if re.fullmatch(r"[\d\W]+", txt):
            return False
        if self.texto_parece_banner(txt):
            return False
        if len(txt.split()) < 2:
            return False

        palavras_ruins = {
            "prime",
            "week",
            "exclusivo",
            "exclusiva",
            "oferta",
            "ofertas",
            "promo",
            "promocion",
            "promociones",
        }
        palavras = re.findall(r"[a-zA-ZÀ-ÿ0-9]+", txt_norm)
        if palavras and all(p in palavras_ruins for p in palavras):
            return False

        return True

    def preco_str_para_float(self, preco_str):
        if preco_str is None:
            return None
        if isinstance(preco_str, (int, float)):
            try:
                valor = float(preco_str)
                return valor if valor > 0 else None
            except Exception:
                return None

        s = str(preco_str).replace("\xa0", " ").strip()
        s = re.sub(r"[^\d\.,]", "", s)
        if not s or not re.search(r"\d", s):
            return None

        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            partes = s.split(".")
            if len(partes) > 2:
                s = s.replace(".", "")
            elif len(partes) == 2 and len(partes[1]) == 3:
                s = s.replace(".", "")

        try:
            valor = float(s)
        except Exception:
            return None

        if valor <= 0 or valor > 100000:
            return None
        return valor

    def float_para_preco_str(self, valor_float):
        if valor_float is None:
            return None
        try:
            valor_float = float(valor_float)
        except Exception:
            return None
        valor_formatado = f"$ {valor_float:,.2f}"
        valor_formatado = (
            valor_formatado.replace(",", "X").replace(".", ",").replace("X", ".")
        )
        return valor_formatado

    def limpar_nome_candidato(self, texto):
        if not texto:
            return ""
        txt = " ".join(str(texto).split()).strip()
        txt = re.sub(r"\s*\|\s*jumbo.*$", "", txt, flags=re.IGNORECASE)
        txt = re.sub(r"\s*-\s*jumbo.*$", "", txt, flags=re.IGNORECASE)
        txt = re.sub(r"\s*\|\s*cencosud.*$", "", txt, flags=re.IGNORECASE)
        txt = re.sub(r"\s*-\s*cencosud.*$", "", txt, flags=re.IGNORECASE)
        return txt.strip(" -|:")

    def salvar_screenshot(self, item, response):
        raw = getattr(response, "raw_api_response", None) or {}
        screenshot_b64 = raw.get("screenshot")
        if not screenshot_b64:
            return None
        nome_arquivo = f"{self.slugify(item.get('nome') or item.get('ean') or 'item')}.png"
        caminho_arquivo = self.prints_dir / nome_arquivo
        with open(caminho_arquivo, "wb") as f:
            f.write(base64.b64decode(screenshot_b64))
        return str(caminho_arquivo.resolve())

    def _get_html_selector(self, response):
        raw = getattr(response, "raw_api_response", None) or {}
        browser_html = raw.get("browserHtml")
        if browser_html:
            return Selector(text=browser_html)
        return Selector(text=response.text or "")

    def texto_parece_ean(self, texto):
        if not texto:
            return False
        texto_limpo = re.sub(r"\D", "", str(texto))
        return texto_limpo.isdigit() and 8 <= len(texto_limpo) <= 14

    def texto_parece_banner(self, texto):
        if not texto:
            return False
        t = self.normalizar_texto(texto)
        bloqueados = [
            "especial de la semana",
            "ofertas",
            "oferta",
            "promociones",
            "promocion",
            "catalogo",
            "resultados",
            "busqueda",
            "supermercado online",
            "aprovecha",
            "prime week",
            "exclusivo",
            "beneficio",
            "imperdible",
            "solo por hoy",
            "cyber",
            "hot sale",
            "black friday",
        ]
        if any(b in t for b in bloqueados):
            return True
        if re.search(r"\b\d{1,2}\s*cuotas\b", t):
            return True
        if re.search(r"\bahorra\b", t):
            return True
        return False

    def extrair_nome_produto_pagina(self, response_sel, ean_atual=None):
        seletores_nome = [
            "[class*='productName']::text",
            "[class*='product-name']::text",
            "[class*='ProductName']::text",
            "[class*='productNameContainer']::text",
            "[class*='productNameContainer'] *::text",
            "[class*='vtex-store-components-3-x-productNameContainer']::text",
            "[class*='vtex-store-components-3-x-productNameContainer'] *::text",
            "h1[class*='product']::text",
            "h1::text",
            "meta[property='og:title']::attr(content)",
            "title::text",
        ]

        candidatos = []
        for seletor in seletores_nome:
            textos = response_sel.css(seletor).getall()
            for txt in textos:
                txt = self.limpar_nome_candidato(txt)
                if txt and txt not in candidatos:
                    candidatos.append(txt)
        for txt in candidatos:
            if self.nome_parece_produto(txt, ean_atual=ean_atual):
                return txt
        return None

    def extrair_marca(self, nome_limpo: str):
        if not nome_limpo:
            return None
        nome = " ".join(str(nome_limpo).split()).strip()
        nome_norm = self.normalizar_texto(nome)

        marcas_conhecidas = [
            "nivea",
            "coca-cola",
            "coca cola",
            "pepsi",
            "nestle",
            "la serenisima",
            "serenisima",
            "arcor",
            "bagley",
            "knorr",
            "ala",
            "skip",
            "drive",
            "rexona",
            "sedal",
            "dove",
            "hellmanns",
            "red bull",
            "colgate",
        ]
        for marca in marcas_conhecidas:
            if marca in nome_norm:
                return marca.title()

        palavras = nome.split()
        if palavras:
            return palavras[0]
        return None

    def eh_url_de_produto(self, url):
        if not url:
            return False
        url = url.lower()
        if re.search(r"/p(?:\?|$)", url):
            return True
        if re.search(r"https?://[^/]+/\d{8,14}(?:\?|$|/)", url):
            return True
        if "/busqueda" in url:
            return False
        return True

    # ---------------- Extração de preços HTML ----------------

    def _normalizar_node(self, node):
        if node is None:
            return None
        if isinstance(node, list):
            return node[0] if node else None
        try:
            if node.__class__.__name__ == "SelectorList":
                return node[0] if len(node) else None
        except Exception:
            pass
        return node

    def _texto_ignorado_preco(self, texto):
        t = self.normalizar_texto(texto)
        bloqueados = [
            "precio regular x lt",
            "precio regular x kg",
            "precio regular x un",
            "precio regular x unidad",
            "precio sin impuestos nacionales",
            "sin impostos nacionales",
            "sin impostos",
            "sin impuestos",
            "x lt",
            "x kg",
            "x un",
            "por unidade",
            "por unidad",
        ]
        return any(b in t for b in bloqueados)

    def _node_tem_texto_ignorado(self, node):
        node = self._normalizar_node(node)
        if node is None:
            return False
        for txt in node.css("::text").getall():
            if self._texto_ignorado_preco(txt):
                return True
        return False

    def _extrair_valores_monetarios_do_node(self, node):
        node = self._normalizar_node(node)
        if node is None:
            return []

        if self._node_tem_texto_ignorado(node):
            return []

        texto = " ".join(
            t.strip() for t in node.css("::text").getall() if t and t.strip()
        )
        candidatos = []
        padroes = [
            r"\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})",
            r"\$\s*\d+(?:,\d{2})",
            r"\d{1,3}(?:\.\d{3})*(?:,\d{2})",
            r"\d{1,3}(?:\.\d{3})",
        ]
        for padrao in padroes:
            for m in re.findall(padrao, texto):
                vf = self.preco_str_para_float(m)
                if vf is not None:
                    candidatos.append(vf)

        unicos = []
        vistos = set()
        for v in candidatos:
            chave = round(v, 2)
            if chave not in vistos:
                vistos.add(chave)
                unicos.append(v)
        return unicos

    def _extrair_percentual_do_texto(self, texto):
        if not texto:
            return None
        t = self.normalizar_texto(texto)
        padroes = [
            r"-\s*(\d{1,3})\s*%",
            r"(\d{1,3})\s*%\s*off",
            r"off\s*(\d{1,3})\s*%",
            r"(\d{1,3})\s*%\s*descuento",
            r"descuento\s*de\s*(\d{1,3})\s*%",
            r"ahorra\s*(\d{1,3})\s*%",
        ]

        for padrao in padroes:
            m = re.search(padrao, t, re.I)
            if not m:
                continue
            try:
                valor = int(m.group(1))
            except Exception:
                continue
            if 0 < valor < 90:
                return valor

        return None

    def _extrair_desconto_percentual_do_node(self, node):
        node = self._normalizar_node(node)
        if node is None:
            return None

        if self._node_tem_texto_ignorado(node):
            return None

        seletores = [
            "[class*='discount']::text",
            "[class*='Discount']::text",
            "[class*='descuento']::text",
            "[class*='badge']::text",
            "[class*='flag']::text",
            "[class*='promotion']::text",
            "[class*='promo']::text",
            "span::text",
            "div::text",
        ]

        candidatos = []
        for sel in seletores:
            for txt in node.css(sel).getall():
                txt = (txt or "").strip()
                if not txt:
                    continue
                pct = self._extrair_percentual_do_texto(txt)
                if pct is not None:
                    candidatos.append(pct)

        if candidatos:
            return max(candidatos)

        texto_total = " ".join(
            t.strip() for t in node.css("::text").getall() if t and t.strip()
        )
        return self._extrair_percentual_do_texto(texto_total)

    def _tem_percentual_no_node(self, node):
        return self._extrair_desconto_percentual_do_node(node) is not None

    def _extrair_preco_riscado_do_node(self, node):
        node = self._normalizar_node(node)
        if node is None:
            return None

        if self._node_tem_texto_ignorado(node):
            return None

        seletores = [
            "[class*='price_listPrice']::text",
            "[class*='listPrice']::text",
            "[class*='price-list-price']::text",
            "[class*='priceListPrice']::text",
            "s::text",
            "del::text",
            "[style*='line-through']::text",
            "[class*='strik']::text",
            "[class*='cross']::text",
        ]

        candidatos = []
        for sel in seletores:
            for txt in node.css(sel).getall():
                txt = (txt or "").strip()
                if not txt or self._texto_ignorado_preco(txt):
                    continue
                vf = self.preco_str_para_float(txt)
                if vf is not None:
                    candidatos.append(vf)

        return max(candidatos) if candidatos else None

    def _extrair_preco_principal_do_node(self, node):
        node = self._normalizar_node(node)
        if node is None:
            return None

        if self._node_tem_texto_ignorado(node):
            return None

        seletores = [
            "[class*='price_sellingPrice']::text",
            "[class*='sellingPrice']::text",
            "[class*='spotPrice']::text",
            "[class*='bestPrice']::text",
            "[class*='price-selling-price']::text",
        ]

        candidatos = []
        for sel in seletores:
            for txt in node.css(sel).getall():
                txt = (txt or "").strip()
                if not txt or self._texto_ignorado_preco(txt):
                    continue
                vf = self.preco_str_para_float(txt)
                if vf is not None:
                    candidatos.append(vf)

        if candidatos:
            return min(candidatos)

        valores = self._extrair_valores_monetarios_do_node(node)
        if valores:
            return min(valores)

        return None

    def _extrair_desconto_percentual_html(self, response_sel):
        seletores = [
            "[class*='discount']::text",
            "[class*='Discount']::text",
            "[class*='descuento']::text",
            "[class*='badge']::text",
            "[class*='flag']::text",
            "[class*='promotion']::text",
            "[class*='promo']::text",
            "span::text",
        ]

        candidatos = []
        for sel in seletores:
            for txt in response_sel.css(sel).getall():
                pct = self._extrair_percentual_do_texto(txt)
                if pct is not None:
                    candidatos.append(pct)

        if candidatos:
            return max(candidatos)

        texto_total = " ".join(
            t.strip() for t in response_sel.css("body *::text").getall() if t and t.strip()
        )
        return self._extrair_percentual_do_texto(texto_total)

    def _coletar_precos_html(self, response_sel):
        resultados = {
            "precoDe": None,
            "precoPor": None,
            "oferta": None,
            "desconto_percentual": None,
        }

        candidatos_principais = []
        seletores_principal = [
            "[class*='price_sellingPrice']",
            "[class*='sellingPrice']",
            "[class*='spotPrice']",
            "[class*='bestPrice']",
            "[class*='price-selling-price']",
        ]
        for sel in seletores_principal:
            for node in response_sel.css(sel):
                vf = self._extrair_preco_principal_do_node(node)
                if vf is not None:
                    candidatos_principais.append(vf)
        preco_principal_global = min(candidatos_principais) if candidatos_principais else None

        bloco_principal = None
        for sel in [
            "[class*='vtex-product-price']",
            "[class*='productPrice']",
            "[class*='Price']",
        ]:
            candidatos = response_sel.css(sel)
            if candidatos:
                bloco_principal = self._normalizar_node(candidatos)
                break

        melhor_match = None

        if bloco_principal is not None:
            desconto_bloco = self._extrair_desconto_percentual_do_node(bloco_principal)
            if desconto_bloco is not None:
                resultados["desconto_percentual"] = desconto_bloco
                resultados["oferta"] = f"-{desconto_bloco}%"

            valores = sorted(self._extrair_valores_monetarios_do_node(bloco_principal))
            valores_validos = [v for v in valores if 0 < v < 100000]

            if len(valores_validos) >= 2:
                preco_desc = min(valores_validos)
                preco_sem_desc = max(valores_validos)
                if preco_sem_desc > preco_desc:
                    melhor_match = (preco_desc, preco_sem_desc)

            if melhor_match is None:
                preco_riscado = self._extrair_preco_riscado_do_node(bloco_principal)
                preco_principal = self._extrair_preco_principal_do_node(bloco_principal)
                if (
                    preco_riscado is not None
                    and preco_principal is not None
                    and preco_riscado > preco_principal
                ):
                    melhor_match = (preco_principal, preco_riscado)

        if melhor_match is None:
            blocos = response_sel.css(
                """
                [class*='price'],
                [class*='Price']
                """
            )
            for node in blocos:
                node = self._normalizar_node(node)
                if node is None:
                    continue

                desconto_node = self._extrair_desconto_percentual_do_node(node)
                valores = sorted(self._extrair_valores_monetarios_do_node(node))
                valores_validos = [v for v in valores if 0 < v < 100000]
                preco_riscado = self._extrair_preco_riscado_do_node(node)
                preco_principal = self._extrair_preco_principal_do_node(node)

                if desconto_node is not None and resultados["desconto_percentual"] is None:
                    resultados["desconto_percentual"] = desconto_node
                    resultados["oferta"] = f"-{desconto_node}%"

                if len(valores_validos) >= 2 and desconto_node is not None:
                    preco_desc = min(valores_validos)
                    preco_sem_desc = max(valores_validos)
                    if preco_sem_desc > preco_desc:
                        melhor_match = (preco_desc, preco_sem_desc)
                        break

                if (
                    desconto_node is not None
                    and preco_riscado is not None
                    and preco_principal is not None
                    and preco_riscado > preco_principal
                ):
                    melhor_match = (preco_principal, preco_riscado)
                    break

        if melhor_match:
            price_float, list_price_float = melhor_match
            resultados["precoDe"] = self.float_para_preco_str(list_price_float)
            resultados["precoPor"] = self.float_para_preco_str(price_float)
            if not resultados["oferta"] and list_price_float > price_float:
                resultados["oferta"] = "x"
        elif preco_principal_global is not None:
            resultados["precoDe"] = self.float_para_preco_str(preco_principal_global)

        if resultados["desconto_percentual"] is None:
            desconto_pct = self._extrair_desconto_percentual_html(response_sel)
            if desconto_pct is not None:
                resultados["desconto_percentual"] = desconto_pct
                resultados["oferta"] = f"-{desconto_pct}%"

        return resultados

    def _preco_container_principal(self, response_sel):
        txt = response_sel.css(
            ".valtech-carrefourar-product-price-0-x-currencyContainer::text"
        ).get()
        if not txt:
            return None
        return self.preco_str_para_float(txt)

    def _coletar_precos_card(self, produto):
        resultados = {
            "precoDe": None,
            "precoPor": None,
            "oferta": None,
            "desconto_percentual": None,
        }

        preco_principal = self._extrair_preco_principal_do_node(produto)
        preco_riscado = self._extrair_preco_riscado_do_node(produto)
        valores = sorted(self._extrair_valores_monetarios_do_node(produto))
        desconto_pct = self._extrair_desconto_percentual_do_node(produto)

        if desconto_pct is not None:
            resultados["desconto_percentual"] = desconto_pct
            resultados["oferta"] = f"-{desconto_pct}%"

        if (
            preco_riscado is not None
            and preco_principal is not None
            and preco_riscado > preco_principal
        ):
            resultados["precoDe"] = self.float_para_preco_str(preco_riscado)
            resultados["precoPor"] = self.float_para_preco_str(preco_principal)
            if not resultados["oferta"]:
                resultados["oferta"] = "x"
            return resultados

        valores_validos = [v for v in valores if 0 < v < 100000]
        if len(valores_validos) >= 2:
            menor = min(valores_validos)
            maior = max(valores_validos)
            if maior > menor:
                resultados["precoDe"] = self.float_para_preco_str(maior)
                resultados["precoPor"] = self.float_para_preco_str(menor)
                if not resultados["oferta"]:
                    resultados["oferta"] = "x"
                return resultados

        if preco_principal is not None:
            resultados["precoDe"] = self.float_para_preco_str(preco_principal)

        return resultados

    # ---------------- JSON embutido (VTEX) ----------------

    def _extrair_jsons_embutidos(self, response_sel):
        blobs = []

        for raw_json in response_sel.css('script[type="application/ld+json"]::text').getall():
            raw_json = (raw_json or "").strip()
            if not raw_json:
                continue
            try:
                blobs.append(json.loads(raw_json))
            except Exception:
                pass

        scripts = response_sel.css("script::text").getall()
        for txt in scripts:
            txt = (txt or "").strip()
            if not txt:
                continue

            candidatos = [
                r"__NEXT_DATA__\s*=\s*({.*})\s*;?\s*$",
                r"window\.__NEXT_DATA__\s*=\s*({.*})\s*;?\s*$",
                r"__STATE__\s*=\s*({.*})\s*;?\s*$",
                r"window\.__STATE__\s*=\s*({.*})\s*;?\s*$",
                r"window\.__PRELOADED_STATE__\s*=\s*({.*})\s*;?\s*$",
                r"window\.__INITIAL_STATE__\s*=\s*({.*})\s*;?\s*$",
            ]

            for padrao in candidatos:
                m = re.search(padrao, txt, re.S)
                if m:
                    try:
                        blobs.append(json.loads(m.group(1)))
                    except Exception:
                        pass

            txt_strip = txt.strip()
            if txt_strip.startswith("{") and txt_strip.endswith("}"):
                try:
                    blobs.append(json.loads(txt_strip))
                except Exception:
                    pass

        return blobs

    def _coletar_skus_dos_jsons(self, obj, encontrados=None):
        if encontrados is None:
            encontrados = []

        if isinstance(obj, dict):
            tem_itemid = "itemId" in obj
            tem_sellers = isinstance(obj.get("sellers"), list)
            tem_ean = "ean" in obj or "referenceId" in obj

            if tem_itemid and (tem_sellers or tem_ean):
                encontrados.append(obj)

            for v in obj.values():
                self._coletar_skus_dos_jsons(v, encontrados)

        elif isinstance(obj, list):
            for item in obj:
                self._coletar_skus_dos_jsons(item, encontrados)

        return encontrados

    def _extrair_item_por_ean(self, response_sel, ean_buscado):
        ean_buscado = self._normalizar_ean(ean_buscado)
        self.logger.info("_extrair_item_por_ean ean_buscado=%r", ean_buscado)
        if not ean_buscado:
            return None

        blobs = self._extrair_jsons_embutidos(response_sel)
        self.logger.info("json blobs encontrados=%d", len(blobs))

        skus = []
        for blob in blobs:
            skus.extend(self._coletar_skus_dos_jsons(blob, []))
        self.logger.info("skus brutos encontrados=%d", len(skus))

        vistos = set()
        skus_unicos = []
        for sku in skus:
            chave = str(sku.get("itemId") or "").strip()
            if chave and chave not in vistos:
                vistos.add(chave)
                skus_unicos.append(sku)
        self.logger.info("skus únicos encontrados=%d", len(skus_unicos))

        for sku in skus_unicos:
            ean_sku = self._normalizar_ean(sku.get("ean"))
            refs = sku.get("referenceId") or []

            if ean_sku == ean_buscado:
                self.logger.info("match por ean direto | itemId=%r", sku.get("itemId"))
                return sku

            if isinstance(refs, list):
                for ref in refs:
                    if isinstance(ref, dict):
                        valor = self._normalizar_ean(ref.get("Value") or ref.get("value"))
                        if valor == ean_buscado:
                            self.logger.info(
                                "match por referenceId | itemId=%r", sku.get("itemId")
                            )
                            return sku

        if skus_unicos:
            self.logger.info(
                "nenhum SKU bateu por EAN, usando fallback primeiro sku | itemId=%r",
                skus_unicos[0].get("itemId"),
            )
            return skus_unicos[0]

        return None

    def _validar_tripla_produto(
        self, ean_esperado, sku_esperado, nome_esperado, sku_item, nome_pagina
    ):
        ean_ok = False
        sku_ok = False
        nome_ok = False

        ean_esperado = self._normalizar_ean(ean_esperado)
        sku_esperado = self._normalizar_sku(sku_esperado)

        ean_sku = self._normalizar_ean(sku_item.get("ean"))
        if ean_esperado and ean_sku == ean_esperado:
            ean_ok = True

        refs = sku_item.get("referenceId") or []
        if not ean_ok and isinstance(refs, list):
            for ref in refs:
                if isinstance(ref, dict):
                    valor = self._normalizar_ean(ref.get("Value") or ref.get("value"))
                    if valor == ean_esperado:
                        ean_ok = True
                        break

        sku_item_id = self._normalizar_sku(sku_item.get("itemId"))
        if sku_esperado and sku_item_id and sku_item_id == sku_esperado:
            sku_ok = True

        nome_item = sku_item.get("name") or ""
        if nome_esperado and nome_item:
            if self.nome_parece_produto(nome_esperado) and self.nome_parece_produto(nome_item):
                if self.normalizar_texto(nome_esperado) in self.normalizar_texto(
                    nome_item
                ) or self.normalizar_texto(nome_item) in self.normalizar_texto(
                    nome_esperado
                ):
                    nome_ok = True
        elif nome_pagina and nome_item:
            if self.normalizar_texto(nome_pagina) in self.normalizar_texto(
                nome_item
            ) or self.normalizar_texto(nome_item) in self.normalizar_texto(nome_pagina):
                nome_ok = True
        elif nome_pagina and nome_esperado:
            if self.normalizar_texto(nome_pagina) in self.normalizar_texto(
                nome_esperado
            ) or self.normalizar_texto(nome_esperado) in self.normalizar_texto(
                nome_pagina
            ):
                nome_ok = True

        self.logger.info(
            "_validar_tripla_produto ean_ok=%s sku_ok=%s nome_ok=%s | "
            "ean=%r sku=%r nome=%r nome_pagina=%r itemId=%r itemEan=%r itemName=%r",
            ean_ok,
            sku_ok,
            nome_ok,
            ean_esperado,
            sku_esperado,
            nome_esperado,
            nome_pagina,
            sku_item.get("itemId"),
            sku_item.get("ean"),
            sku_item.get("name"),
        )

        if ean_ok and (sku_ok or nome_ok):
            return True
        return False

    def _preencher_precos_via_json(self, item, sku_item):
        sellers = sku_item.get("sellers") or []
        for seller in sellers:
            comm = seller.get("commertialOffer") or {}
            price = comm.get("Price")
            list_price = comm.get("ListPrice")

            candidatos_float = []
            for v in (price, list_price):
                vf = self.preco_str_para_float(v)
                if vf is not None:
                    candidatos_float.append(vf)

            if candidatos_float and not item.get("precoDe"):
                maior = max(candidatos_float)
                item["precoDe"] = self.float_para_preco_str(maior)
                return item

        return item

    # ---------------- API VTEX por SKU ----------------

    def _request_catalog_por_sku(self, sku, ean_atual, item_base):
        url = self.site_cfg["catalog_sku"].format(sku=quote(str(sku)))
        self.logger.info("Agendando catalog por sku=%s | %s", sku, url)
        return Request(
            url=url,
            callback=self.parse_catalog_por_sku,
            dont_filter=True,
            meta={
                "ean_atual": str(ean_atual),
                "item_base": item_base,
                "sku_encontrado": str(sku),
            },
        )

    def parse_catalog_por_sku(self, response):
        ean_atual = str(response.meta.get("ean_atual") or "").strip()
        item = response.meta.get("item_base", {}).copy()
        sku_encontrado = str(response.meta.get("sku_encontrado") or "").strip()

        self.logger.info(
            "parse_catalog_por_sku status=%s sku=%s ean=%s url=%s",
            response.status,
            sku_encontrado,
            ean_atual,
            response.url,
        )

        try:
            data = json.loads(response.text or "[]")
        except Exception as exc:
            self.logger.info("Falha parse json catalog sku=%s error=%s", sku_encontrado, exc)
            return

        if not isinstance(data, list) or not data:
            self.logger.info("Catalog vazio para sku=%s", sku_encontrado)
            return

        sku_item = None
        nome_pagina = None
        for prod in data:
            nome_prod = prod.get("productName") or prod.get("productNameWithBrand")
            if nome_prod and not nome_pagina:
                nome_pagina = nome_prod
            items = prod.get("items") or []
            for it in items:
                if str(it.get("itemId") or "").strip() == sku_encontrado:
                    sku_item = it
                    break
            if sku_item:
                break

        if not sku_item:
            self.logger.info("SKU %s não localizado dentro do catalog response", sku_encontrado)
            return

        item["sku"] = sku_encontrado or item.get("sku")

        if not item.get("nome"):
            nome_sku = sku_item.get("name") or nome_pagina
            if nome_sku:
                item["nome"] = str(nome_sku).strip()

        if not item.get("marca") and item.get("nome"):
            item["marca"] = self.extrair_marca(item["nome"])

        tripla_ok = self._validar_tripla_produto(
            ean_esperado=ean_atual,
            sku_esperado=sku_encontrado,
            nome_esperado=item.get("nome"),
            sku_item=sku_item,
            nome_pagina=nome_pagina,
        )

        if not tripla_ok:
            self.logger.info(
                "Tripla API catalog não confere | ean=%r | sku=%r | nome=%r | url=%s",
                ean_atual,
                sku_encontrado,
                item.get("nome"),
                response.url,
            )
            return

        sellers = sku_item.get("sellers") or []
        if isinstance(sellers, list) and sellers and not item.get("precoDe"):
            for seller in sellers:
                offer = seller.get("commertialOffer") or {}
                price_float = self.preco_str_para_float(offer.get("Price"))
                if price_float is not None and price_float > 0:
                    item["precoDe"] = self.float_para_preco_str(price_float)
                    break

        if not item.get("precoDe"):
            self.logger.info("Saindo sem precoDe após catalog sku=%s", sku_encontrado)
            return

        item["link"] = item.get("link") or response.url

        chave_item = (
            str(item.get("ean")),
            str(item.get("sku")),
            str(item.get("link")),
        )
        if chave_item in self.itens_emitidos:
            self.logger.info("Item duplicado ignorado %r", chave_item)
            return
        self.itens_emitidos.add(chave_item)

        item.setdefault("print_tela_path", None)

        self.logger.info(
            "Emitindo item final ean=%s sku=%s nome=%r",
            item.get("ean"),
            item.get("sku"),
            item.get("nome"),
        )
        yield item

    # ---------------- Lista de produtos (busca) ----------------

    def obter_produtos_da_pagina(self, response):
        response_sel = self._get_html_selector(response)
        seletores_por_loja = {
            "jumbo_ar": [
                "article",
                "[class*='product']",
                "[class*='Product']",
                "[class*='vtex-product-summary']",
                "a[href*='/p']",
                "a[href*='?_q=']",
            ],
        }

        lista = seletores_por_loja.get(self.loja, ["article", "[class*='product']"])
        vistos = []
        chaves_vistas = set()

        for seletor in lista:
            for node in response_sel.css(seletor):
                href = node.css("a::attr(href), ::attr(href)").get()
                textos = node.css("::text").getall()
                texto_base = "".join(textos[:3]).strip() if textos else ""
                chave = f"{href or ''}|{texto_base}"
                if chave not in chaves_vistas:
                    chaves_vistas.add(chave)
                    vistos.append(node)

        self.logger.info("Produtos encontrados na busca: %d", len(vistos))
        return vistos

    def extrair_nome_produto_lista(self, produto):
        seletores_nome = [
            "[class*='productName']::text",
            "[class*='product-name']::text",
            "[class*='ProductName']::text",
            "[class*='productNameContainer']::text",
            "[class*='vtex-store-components-3-x-productNameContainer']::text",
            "h1::text",
            "h2::text",
            "h3::text",
            "h4::text",
            "[class*='name']::text",
            "[class*='title']::text",
            "a::text",
        ]

        candidatos = []
        for seletor in seletores_nome:
            for nome in produto.css(seletor).getall():
                nome_limpo = self.limpar_nome_candidato(nome)
                if nome_limpo and nome_limpo not in candidatos:
                    candidatos.append(nome_limpo)

        for nome_limpo in candidatos:
            if self.nome_parece_produto(nome_limpo):
                return nome_limpo
        return None

    def extrair_link_produto_lista(self, produto, response):
        response_sel = self._get_html_selector(response)
        seletores_link = [
            "a[href*='/producto/']::attr(href)",
            "a[href*='/p/']::attr(href)",
            "a[href*='/p']::attr(href)",
            "a[href*='?_q=']::attr(href)",
            "a::attr(href)",
            "::attr(href)",
        ]

        for seletor in seletores_link:
            link = produto.css(seletor).get()
            if link:
                return response_sel.urljoin(link)
        return None

    def extrair_next_page(self, response):
        response_sel = self._get_html_selector(response)
        seletores = [
            "a[rel='next']::attr(href)",
            "a[aria-label*='Próxima']::attr(href)",
            "a[aria-label*='Next']::attr(href)",
            "a[aria-label*='Siguiente']::attr(href)",
            "a[title*='Next']::attr(href)",
            "a[title*='Siguiente']::attr(href)",
            ".pagination a.next::attr(href)",
            "li.next a::attr(href)",
        ]

        for seletor in seletores:
            next_page = response_sel.css(seletor).get()
            if next_page:
                return next_page
        return None

    def parse_search(self, response):
        ean_atual = response.meta.get("ean_atual") or self.ean
        self.logger.info("parse_search url=%s ean=%s", response.url, ean_atual)

        produtos = self.obter_produtos_da_pagina(response)
        for produto in produtos:
            nome_limpo = self.extrair_nome_produto_lista(produto)
            if not nome_limpo:
                continue

            link_absoluto = self.extrair_link_produto_lista(produto, response)
            if not link_absoluto:
                continue
            if not self.eh_url_de_produto(link_absoluto):
                continue

            precos_card = self._coletar_precos_card(produto)

            item_base = {
                "loja": self.loja,
                "ean": str(ean_atual),
                "sku": None,
                "nome": nome_limpo,
                "marca": self.extrair_marca(nome_limpo),
                "precoDe": precos_card.get("precoDe"),
                "precoPor": precos_card.get("precoPor"),
                "oferta": precos_card.get("oferta"),
                "desconto_percentual": precos_card.get("desconto_percentual"),
                "print_tela_path": None,
                "link": link_absoluto,
            }

            self.logger.info(
                "Agendando produto da busca nome=%r | %s", nome_limpo, link_absoluto
            )

            yield Request(
                url=link_absoluto,
                callback=self.parse_produto,
                dont_filter=True,
                meta={
                    "item_base": item_base,
                    "ean_atual": ean_atual,
                    "via_url_direta": False,
                    "zyte_api_automap": {
                        "browserHtml": True,
                        "screenshot": True,
                    },
                },
            )

    def parse_produto(self, response):
        ean_atual = str(response.meta.get("ean_atual") or self.ean or "").strip()
        item = (response.meta.get("item_base") or {}).copy()
        via_url_direta = response.meta.get("via_url_direta", False)

        response_sel = self._get_html_selector(response)

        if not item.get("nome"):
            nome_pagina = self.extrair_nome_produto_pagina(response_sel, ean_atual=ean_atual)
            if nome_pagina:
                item["nome"] = nome_pagina

        if not item.get("marca") and item.get("nome"):
            item["marca"] = self.extrair_marca(item["nome"])

        if not item.get("print_tela_path"):
            try:
                item["print_tela_path"] = self.salvar_screenshot(item, response)
            except Exception:
                item["print_tela_path"] = None

        sku_item = self._extrair_item_por_ean(response_sel, ean_atual)
        if not sku_item:
            self.logger.info("Nenhum sku encontrado na página do produto | ean=%s url=%s", ean_atual, response.url)
            return

        sku_encontrado = self._normalizar_sku(sku_item.get("itemId"))
        if not sku_encontrado:
            self.logger.info("SKU vazio após extração | ean=%s url=%s", ean_atual, response.url)
            return

        item["sku"] = sku_encontrado
        item["link"] = item.get("link") or response.url

        precos_html = self._coletar_precos_html(response_sel)

        if via_url_direta:
            if not item.get("precoDe") and precos_html.get("precoDe"):
                item["precoDe"] = precos_html.get("precoDe")
            if not item.get("precoPor") and precos_html.get("precoPor"):
                item["precoPor"] = precos_html.get("precoPor")
            if not item.get("oferta") and precos_html.get("oferta"):
                item["oferta"] = precos_html.get("oferta")
            if not item.get("desconto_percentual") and precos_html.get("desconto_percentual") is not None:
                item["desconto_percentual"] = precos_html.get("desconto_percentual")

        yield self._request_catalog_por_sku(
            sku=sku_encontrado,
            ean_atual=ean_atual,
            item_base=item,
        )