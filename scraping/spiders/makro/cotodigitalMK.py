import csv
import json
import re
import unicodedata
from ast import literal_eval
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import quote


import scrapy
from openpyxl import load_workbook



class CotodigitalMkSpider(scrapy.Spider):
    name = "cotodigital_mk"
    allowed_domains = ["api.coto.com.ar", "www.cotodigital.com.ar", "cotodigital.com.ar"]


    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "RETRY_TIMES": 2,
        "DOWNLOAD_TIMEOUT": 30,
        "FEED_EXPORT_ENCODING": "utf-8",
        "LOG_LEVEL": "INFO",
        "FEED_EXPORT_FIELDS": [
            "ean_entrada",
            "nome_coto",
            "preco_coto",
            "articulo_descripcion",
        ],
    }


    HEADER_ALIASES = {
        "articulo nr": "Artículo NR",
        "artículo nr": "Artículo NR",
        "codigo interno do concorrente": "Artículo NR",
        "cod interno do concorrente": "Artículo NR",
        "codigo interno": "Artículo NR",
        "cod interno": "Artículo NR",


        "articulo descripcion": "Artículo DESCRIPCION",
        "artículo descripcion": "Artículo DESCRIPCION",
        "descripcion": "Artículo DESCRIPCION",
        "descripción": "Artículo DESCRIPCION",
        "articulo": "Artículo DESCRIPCION",


        "ean": "EAN",
        "codigo de barras": "EAN",
        "cod barras": "EAN",
        "barcode": "EAN",


        "area": "AREA",
        "área": "AREA",


        "main group": "MAIN GROUP",
        "grupo principal": "MAIN GROUP",


        "grupo": "GRUPO",

        "ecompetidor": "eCompetidor",
        "competidor": "eCompetidor",
    }


    REQUIRED_SEARCH_COLUMNS = {"Artículo DESCRIPCION", "EAN"}


    def __init__(self, input_file=None, store_id="200", sheet_name=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file = input_file or "input.csv"
        self.store_id = str(store_id)
        self.sheet_name = sheet_name
        self.api_key = "key_r6xzz4IAoTWcipni"
        self.rows = []


    # =========================
    # INÍCIO DO FLUXO SCRAPY
    # =========================


    def start_requests(self):
        self.rows = self.load_input_rows(self.input_file, self.sheet_name)
        self.logger.info(f"[START] Linhas carregadas: {len(self.rows)} | arquivo={self.input_file}")

        if self.rows:
            self.logger.info(f"[DEBUG] Primeira linha normalizada: {self.rows[0]}")

        self.rows = self.filter_rows_by_competitor(self.rows, target="Coto Ciudadela")
        self.logger.info(
            f"[FILTER] Linhas após filtro eCompetidor='Coto Ciudadela': {len(self.rows)}"
        )

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/137.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Referer": "https://www.cotodigital.com.ar/",
            "Origin": "https://www.cotodigital.com.ar",
        }


        for idx, row in enumerate(self.rows, start=1):
            articulo_nr = self.clean_text(row.get("Artículo NR", ""))
            descripcion = self.clean_text(row.get("Artículo DESCRIPCION", ""))
            ean = self.clean_ean(row.get("EAN", ""))


            if idx <= 5:
                self.logger.info(
                    f"[ROW {idx}] articulo_nr={articulo_nr} | descricao={descripcion[:80]} | ean={ean}"
                )


            termos = []
            if ean:
                termos.append(("ean", ean))
            if descripcion:
                termos.append(("descripcion", descripcion))


            if not termos:
                yield self.build_empty_item(
                    row=row,
                    erro="Linha sem EAN e sem descrição para busca."
                )
                continue


            busca_tipo, termo = termos[0]
            url = self.build_search_url(termo)


            self.logger.info(
                f"[SEARCH] tipo={busca_tipo} | termo={termo} | artigo_nr={articulo_nr}"
            )


            yield scrapy.Request(
                url=url,
                callback=self.parse_search,
                errback=self.errback_search,
                dont_filter=True,
                headers=headers,
                meta={
                    "row": row,
                    "headers_used": headers,
                    "search_attempt_index": 0,
                    "search_terms": termos,
                    "search_tipo": busca_tipo,
                    "search_termo": termo,
                },
            )


    def parse_search(self, response):
        row = response.meta["row"]
        headers = response.meta["headers_used"]
        search_attempt_index = response.meta["search_attempt_index"]
        search_terms = response.meta["search_terms"]
        search_tipo = response.meta["search_tipo"]
        search_termo = response.meta["search_termo"]


        self.logger.info(
            f"[SEARCH_RESPONSE] status={response.status} | tipo={search_tipo} | termo={search_termo}"
        )


        data = response.json()
        response_data = data.get("response", {})
        results = response_data.get("results", []) or []


        results = results[:10]


        best = self.choose_best_result(row, results)


        if best:
            data_map = self.extract_result_fields(best)


            raw_preco = data_map.get("preco", "")
            raw_ref = data_map.get("preco_referencia", "")


            preco_coto = self._simplificar_preco_saida(raw_preco, prefer="formatPrice")
            preco_ref = self._simplificar_preco_saida(raw_ref, prefer="listPrice")


            self.logger.info(
                f"[DEBUG_PRECO_FINAL] store_id={self.store_id} | preco_coto={preco_coto} | preco_ref={preco_ref}"
            )


            yield {
                "articulo_nr": row.get("Artículo NR", ""),
                "articulo_descripcion": row.get("Artículo DESCRIPCION", ""),
                "ean_entrada": row.get("EAN", ""),
                "area": row.get("AREA", ""),
                "main_group": row.get("MAIN GROUP", ""),
                "grupo": row.get("GRUPO", ""),
                "eCompetidor": row.get("eCompetidor", ""),

                "busca_tipo": search_tipo,
                "busca_termo": search_termo,
                "produto_encontrado": True,
                "score_match": best.get("_score_match", ""),

                "nome_coto": data_map.get("nome", ""),
                "marca_coto": data_map.get("marca", ""),
                "preco_coto": preco_coto,
                "preco_referencia_coto": preco_ref,
                "ean_coto": data_map.get("ean", ""),
                "sku_coto": data_map.get("sku", ""),
                "url_produto": data_map.get("url", ""),
                "imagem": data_map.get("imagem", ""),

                "search_url": response.url,
                "total_resultados": response_data.get("total_num_results", 0),
                "erro": "",
            }
            return


        next_attempt = search_attempt_index + 1
        if next_attempt < len(search_terms):
            novo_tipo, novo_termo = search_terms[next_attempt]
            nova_url = self.build_search_url(novo_termo)


            self.logger.info(
                f"[RETRY_SEARCH] tipo={novo_tipo} | termo={novo_termo} | artigo_nr={row.get('Artículo NR', '')}"
            )


            yield scrapy.Request(
                url=nova_url,
                callback=self.parse_search,
                errback=self.errback_search,
                dont_filter=True,
                headers=headers,
                meta={
                    "row": row,
                    "headers_used": headers,
                    "search_attempt_index": next_attempt,
                    "search_terms": search_terms,
                    "search_tipo": novo_tipo,
                    "search_termo": novo_termo,
                },
            )
            return


        yield self.build_empty_item(
            row=row,
            busca_tipo=search_tipo,
            busca_termo=search_termo,
            search_url=response.url,
            total_resultados=response_data.get("total_num_results", 0),
            erro="Nenhum resultado compatível encontrado."
        )


    def errback_search(self, failure):
        request = getattr(failure, "request", None)
        row = request.meta.get("row", {}) if request else {}
        search_tipo = request.meta.get("search_tipo", "") if request else ""
        search_termo = request.meta.get("search_termo", "") if request else ""


        yield self.build_empty_item(
            row=row,
            busca_tipo=search_tipo,
            busca_termo=search_termo,
            search_url=request.url if request else "",
            total_resultados="",
            erro=repr(failure.value),
        )


    def build_search_url(self, termo):
        termo = self.clean_text(termo)
        termo_enc = quote(termo, safe="")
        pre_filter = quote(
            json.dumps({"name": "store_availability", "value": self.store_id}, separators=(",", ":")),
            safe=""
        )
        return (
            "https://api.coto.com.ar/api/v1/ms-digital-sitio-bff-web/api/v1/products/search/"
            f"{termo_enc}?key={self.api_key}&num_results_per_page=24&pre_filter_expression={pre_filter}"
        )

    def filter_rows_by_competitor(self, rows, target="Coto Ciudadela"):
        target_norm = self.normalize_text(target)
        filtered = []

        for row in rows:
            competitor = row.get("eCompetidor", "")
            if self.normalize_text(competitor) == target_norm:
                filtered.append(row)

        return filtered


    # =========================
    # ESCOLHA DO MELHOR RESULT
    # =========================


    def choose_best_result(self, row, results):
        ean_entrada = self.clean_ean(row.get("EAN", ""))
        desc_entrada = self.normalize_text(row.get("Artículo DESCRIPCION", ""))
        area_entrada = self.normalize_text(row.get("AREA", ""))
        main_group_entrada = self.normalize_text(row.get("MAIN GROUP", ""))
        grupo_entrada = self.normalize_text(row.get("GRUPO", ""))


        best = None
        best_score = -1


        for result in results:
            data_map = self.extract_result_fields(result)


            nome = self.normalize_text(data_map.get("nome", ""))
            marca = self.normalize_text(data_map.get("marca", ""))
            categoria_texto = " ".join(
                x for x in [
                    self.normalize_text(result.get("data", {}).get("AREA", "")),
                    self.normalize_text(result.get("data", {}).get("MAIN GROUP", "")),
                    self.normalize_text(result.get("data", {}).get("GRUPO", "")),
                ] if x
            )


            ean_result = self.clean_ean(data_map.get("ean", ""))
            sku_result = self.clean_text(data_map.get("sku", ""))


            score = 0


            if ean_entrada and ean_result and ean_entrada == ean_result:
                score += 100


            if ean_entrada and sku_result and ean_entrada in sku_result:
                score += 20


            if desc_entrada and nome:
                score += int(100 * SequenceMatcher(None, desc_entrada, nome).ratio())


            if desc_entrada and marca and marca in desc_entrada:
                score += 10


            categorias_entrada = " ".join(x for x in [area_entrada, main_group_entrada, grupo_entrada] if x)
            if categorias_entrada and categoria_texto:
                score += int(30 * SequenceMatcher(None, categorias_entrada, categoria_texto).ratio())


            result["_score_match"] = score


            if score > best_score:
                best_score = score
                best = result


        if best is not None:
            best["_score_match"] = best_score


        return best


    # =========================
    # FILTRO DE PREÇO
    # =========================


    def _extrair_format_price_de_raw(self, raw_prices):
        """
        Filtra apenas o formatPrice/listPrice da loja self.store_id.
        Garante retorno como strings simples.
        """
        if not raw_prices:
            return "", ""


        if isinstance(raw_prices, list):
            preco_list = raw_prices
        elif isinstance(raw_prices, str):
            try:
                preco_list = literal_eval(raw_prices)
            except Exception:
                preco_list = []
        elif isinstance(raw_prices, dict):
            preco_list = [raw_prices]
        else:
            try:
                preco_list = list(raw_prices)
            except Exception:
                preco_list = []


        if not preco_list:
            return "", ""


        store_target = self.store_id.zfill(3)
        chosen = None


        for p in preco_list:
            if not isinstance(p, dict):
                continue
            store = str(p.get("store", "")).zfill(3)
            if store == store_target:
                chosen = p
                break


        if chosen is None:
            for p in preco_list:
                if isinstance(p, dict) and (
                    p.get("formatPrice") is not None or p.get("listPrice") is not None
                ):
                    chosen = p
                    break


        if chosen is None or not isinstance(chosen, dict):
            return "", ""


        format_price = chosen.get("formatPrice")
        list_price = chosen.get("listPrice")


        preco = ""
        preco_referencia = ""


        if format_price not in (None, ""):
            preco = str(format_price)


        if list_price not in (None, ""):
            preco_referencia = str(list_price)


        return preco, preco_referencia


    def _simplificar_preco_saida(self, valor, prefer="formatPrice"):
        """
        Última camada de proteção:
        se por algum motivo vier lista/dict/string de lista, transforma em valor simples.
        """
        if valor in (None, ""):
            return ""


        if isinstance(valor, (int, float)):
            if isinstance(valor, float) and valor.is_integer():
                return str(int(valor))
            return str(valor)


        if isinstance(valor, dict):
            if prefer == "listPrice":
                return str(valor.get("listPrice") or valor.get("formatPrice") or "")
            return str(valor.get("formatPrice") or valor.get("listPrice") or "")


        parsed = valor
        if isinstance(valor, str):
            texto = valor.strip()
            try:
                parsed = literal_eval(texto)
            except Exception:
                return texto


        if isinstance(parsed, list) and parsed:
            loja_alvo = str(self.store_id).zfill(3)
            escolhido = None


            for p in parsed:
                if isinstance(p, dict) and str(p.get("store", "")).zfill(3) == loja_alvo:
                    escolhido = p
                    break


            if escolhido is None:
                for p in parsed:
                    if isinstance(p, dict):
                        escolhido = p
                        break


            if not isinstance(escolhido, dict):
                return ""


            if prefer == "listPrice":
                return str(escolhido.get("listPrice") or escolhido.get("formatPrice") or "")
            return str(escolhido.get("formatPrice") or escolhido.get("listPrice") or "")


        if isinstance(parsed, dict):
            if prefer == "listPrice":
                return str(parsed.get("listPrice") or parsed.get("formatPrice") or "")
            return str(parsed.get("formatPrice") or parsed.get("listPrice") or "")


        return str(parsed).strip()


    def extract_result_fields(self, result):
        data = result.get("data", {}) or {}


        imagem = ""
        image_url = data.get("image_url")
        image_urls = data.get("image_urls")
        if image_url:
            imagem = image_url
        elif isinstance(image_urls, list) and image_urls:
            imagem = image_urls[0]


        raw_prices = (
            data.get("prices")
            or data.get("price_list")
            or data.get("priceList")
            or data.get("storePrices")
        )


        preco, preco_referencia = self._extrair_format_price_de_raw(raw_prices)


        if not preco:
            raw_p = data.get("price") or data.get("current_price") or ""
            if raw_p not in (None, ""):
                preco = self._simplificar_preco_saida(raw_p, prefer="formatPrice")


        if not preco_referencia:
            raw_pr = (
                data.get("list_price")
                or data.get("original_price")
                or data.get("reference_price")
                or ""
            )
            if raw_pr not in (None, ""):
                preco_referencia = self._simplificar_preco_saida(raw_pr, prefer="listPrice")


        return {
            "nome": data.get("product_name") or data.get("name") or result.get("value", "") or "",
            "marca": data.get("brand") or data.get("product_brand") or "",
            "preco": preco,
            "preco_referencia": preco_referencia,
            "ean": data.get("ean") or data.get("gtin") or data.get("barcode") or "",
            "sku": data.get("sku") or data.get("id") or result.get("id", "") or "",
            "url": data.get("url") or data.get("product_url") or "",
            "imagem": imagem,
        }


    # =========================
    # LEITURA DE INPUT (CSV/XLSX)
    # =========================


    def load_input_rows(self, file_path, sheet_name=None):
        path = Path(file_path)


        if not path.is_absolute():
            candidates = [
                Path.cwd() / path,
                Path(__file__).resolve().parent / path,
                Path(__file__).resolve().parent.parent / path,
                Path(__file__).resolve().parent.parent.parent / path,
            ]
            found = next((p for p in candidates if p.exists()), None)
            if found:
                path = found


        if not path.exists():
            raise FileNotFoundError(
                f"Arquivo não encontrado: {file_path} | cwd={Path.cwd()}"
            )


        suffix = path.suffix.lower()


        if suffix == ".csv":
            return self.load_csv_rows(path)


        if suffix in [".xlsx", ".xlsm"]:
            return self.load_xlsx_rows(path, sheet_name)


        raise ValueError(f"Extensão não suportada: {suffix}")


    def load_csv_rows(self, path):
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            all_rows = list(reader)


        if not all_rows:
            return []


        header_idx = self.find_header_row_index(all_rows)
        headers = [self.map_header_name(h) for h in all_rows[header_idx]]
        self.logger.info(f"[CSV] Header row index: {header_idx}")
        self.logger.info(f"[CSV] Headers mapeados: {headers}")


        output = []
        for raw_values in all_rows[header_idx + 1:]:
            if not raw_values or all(not self.clean_text(v) for v in raw_values):
                continue


            row = {}
            for idx, header in enumerate(headers):
                if not header:
                    continue
                value = raw_values[idx] if idx < len(raw_values) else ""
                row[header] = self.clean_cell_value(value)


            output.append(row)


        return output


    def load_xlsx_rows(self, path, sheet_name=None):
        wb = load_workbook(filename=path, read_only=True, data_only=True)


        if sheet_name and sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
        else:
            ws = self.choose_best_sheet(wb)


        self.logger.info(f"[XLSX] Aba usada: {ws.title}")


        all_rows = []
        for row in ws.iter_rows(values_only=True):
            all_rows.append([self.clean_cell_value(cell) for cell in row])


        if not all_rows:
            return []


        header_idx = self.find_header_row_index(all_rows)
        raw_headers = all_rows[header_idx]
        mapped_headers = [self.map_header_name(h) for h in raw_headers]


        self.logger.info(f"[XLSX] Header row index: {header_idx}")
        self.logger.info(f"[XLSX] Headers brutos: {raw_headers}")
        self.logger.info(f"[XLSX] Headers mapeados: {mapped_headers}")


        output = []
        for raw_values in all_rows[header_idx + 1:]:
            if not raw_values or all(not self.clean_text(v) for v in raw_values):
                continue


            row = {}
            for idx, header in enumerate(mapped_headers):
                if not header:
                    continue
                value = raw_values[idx] if idx < len(raw_values) else ""
                row[header] = self.clean_cell_value(value)


            if any(self.clean_text(v) for v in row.values()):
                output.append(row)


        return output


    def choose_best_sheet(self, workbook):
        best_ws = workbook[workbook.sheetnames[0]]
        best_score = -1


        for sheet_name in workbook.sheetnames:
            ws = workbook[sheet_name]


            preview_rows = []
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if i >= 15:
                    break
                preview_rows.append([self.clean_cell_value(cell) for cell in row])


            score = self.score_sheet(preview_rows)
            self.logger.info(f"[XLSX] Score aba '{ws.title}': {score}")


            if score > best_score:
                best_score = score
                best_ws = ws


        return best_ws


    def score_sheet(self, rows):
        best = 0
        for row in rows[:15]:
            normalized = [self.normalize_header(v) for v in row if self.clean_text(v)]
            score = 0
            for col in normalized:
                if col in self.HEADER_ALIASES:
                    score += 1
            best = max(best, score)
        return best


    def find_header_row_index(self, rows):
        best_index = 0
        best_score = -1
        max_rows = min(len(rows), 20)


        for idx in range(max_rows):
            row = rows[idx]
            normalized = [self.normalize_header(v) for v in row if self.clean_text(v)]


            score = 0
            found_mapped = set()


            for col in normalized:
                mapped = self.HEADER_ALIASES.get(col)
                if mapped:
                    score += 1
                    found_mapped.add(mapped)


            if "EAN" in found_mapped:
                score += 2
            if "Artículo DESCRIPCION" in found_mapped:
                score += 2


            if score > best_score:
                best_score = score
                best_index = idx


        self.logger.info(f"[HEADER] Linha escolhida como cabeçalho: {best_index} | score={best_score}")
        return best_index


    def map_header_name(self, header):
        normalized = self.normalize_header(header)
        return self.HEADER_ALIASES.get(normalized, self.clean_text(header))


    def normalize_header(self, value):
        value = self.clean_text(value)
        value = value.replace("\n", " ").replace("\r", " ").replace("_", " ")
        value = re.sub(r"\s+", " ", value).strip().lower()
        value = unicodedata.normalize("NFKD", value)
        value = "".join(ch for ch in value if not unicodedata.combining(ch))
        return value


    # =========================
    # ITENS VAZIOS / LIMPEZA
    # =========================


    def build_empty_item(
        self,
        row,
        busca_tipo="",
        busca_termo="",
        search_url="",
        total_resultados="",
        erro="",
    ):
        return {
            "articulo_nr": row.get("Artículo NR", ""),
            "articulo_descripcion": row.get("Artículo DESCRIPCION", ""),
            "ean_entrada": row.get("EAN", ""),
            "area": row.get("AREA", ""),
            "main_group": row.get("MAIN GROUP", ""),
            "grupo": row.get("GRUPO", ""),
            "eCompetidor": row.get("eCompetidor", ""),
            "busca_tipo": busca_tipo,
            "busca_termo": busca_termo,
            "produto_encontrado": False,
            "score_match": "",
            "nome_coto": "",
            "marca_coto": "",
            "preco_coto": "",
            "preco_referencia_coto": "",
            "ean_coto": "",
            "sku_coto": "",
            "url_produto": "",
            "imagem": "",
            "search_url": search_url,
            "total_resultados": total_resultados,
            "erro": erro,
        }


    def clean_cell_value(self, value):
        if value is None:
            return ""
        if isinstance(value, float):
            if value.is_integer():
                return str(int(value))
            return str(value)
        return str(value).strip()


    def clean_text(self, value):
        if value is None:
            return ""
        return str(value).strip()


    def clean_ean(self, value):
        if value is None:
            return ""
        value = str(value).strip()
        digits = re.sub(r"\D+", "", value)
        return digits


    def normalize_text(self, value):
        value = self.clean_text(value).lower()
        value = unicodedata.normalize("NFKD", value)
        value = "".join(ch for ch in value if not unicodedata.combining(ch))
        value = re.sub(r"[^a-z0-9\s]", " ", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+", " ", value).strip()
        return value