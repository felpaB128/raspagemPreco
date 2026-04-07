import base64
import csv
import json
import re
from pathlib import Path
from urllib.parse import quote

from scrapy import Spider, Request

try:
    import openpyxl
except ImportError:
    openpyxl = None


class CarrefourPrecoSpider(Spider):
    name = "carrefour_preco"
    allowed_domains = ["www.carrefour.com.ar", "carrefour.com.ar"]

    custom_settings = {
        "ZYTE_API_TRANSPARENT_MODE": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DOWNLOAD_DELAY": 0.5,
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, arquivo_entrada=None, ean=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not arquivo_entrada and not ean:
            raise ValueError("Passe arquivo_entrada ou ean")

        self.arquivo_entrada = arquivo_entrada
        self.ean = ean

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

        header_row_idx = None
        for i, row in enumerate(rows):
            if row and any(c is not None and str(c).strip() for c in row):
                header_row_idx = i
                break

        if header_row_idx is None:
            raise ValueError("Planilha sem dados.")

        header = [str(h).strip() if h is not None else "" for h in rows[header_row_idx]]
        header_normalizado = [h.lower() for h in header]
        nomes = {c: idx for idx, c in enumerate(header_normalizado)}

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

        eans = []
        for row in rows[header_row_idx + 1:]:
            if row is None or idx_ean >= len(row):
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

        return eans[:50]

    def _decode_body(self, cap):
        body_b64 = cap.get("httpResponseBody")
        if not body_b64:
            return None
        try:
            return base64.b64decode(body_b64).decode("utf-8", errors="ignore")
        except Exception:
            return None

    def _walk_dicts(self, obj):
        encontrados = []

        def walk(x):
            if isinstance(x, dict):
                encontrados.append(x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for i in x:
                    walk(i)

        walk(obj)
        return encontrados

    def start_requests(self):
        if self.arquivo_entrada:
            eans = self.ler_eans_arquivo(self.arquivo_entrada)
        else:
            eans = [self.ean]

        for ean in eans:
            url = f"https://www.carrefour.com.ar/?keyword={quote(str(ean))}"

            yield Request(
                url=url,
                callback=self.parse_busca,
                dont_filter=True,
                meta={
                    "ean_atual": str(ean),
                    "zyte_api_automap": {
                        "browserHtml": True,
                        "networkCapture": [
                            {
                                "filterType": "url",
                                "matchType": "contains",
                                "value": "api",
                                "httpResponseBody": True,
                            },
                            {
                                "filterType": "url",
                                "matchType": "contains",
                                "value": "graphql",
                                "httpResponseBody": True,
                            },
                            {
                                "filterType": "url",
                                "matchType": "contains",
                                "value": "search",
                                "httpResponseBody": True,
                            },
                        ],
                    },
                },
            )

    def parse_busca(self, response):
        ean = re.sub(r"\D", "", response.meta["ean_atual"])
        capturas = response.raw_api_response.get("networkCapture", [])

        self.logger.info("Carrefour AR | EAN=%s | capturas=%d", ean, len(capturas))

        vistos = set()

        for cap in capturas:
            body = self._decode_body(cap)
            if not body:
                continue

            try:
                data = json.loads(body)
            except Exception:
                continue

            for d in self._walk_dicts(data):
                texto = " ".join(str(v) for v in d.values() if isinstance(v, (str, int, float)))
                texto_norm = re.sub(r"\D", "", texto)

                if ean not in texto_norm:
                    continue

                nome = d.get("name") or d.get("productName") or d.get("nombre") or d.get("title")
                marca = d.get("brand") or d.get("brandName") or d.get("marca")
                preco = d.get("price") or d.get("bestPrice") or d.get("sellingPrice") or d.get("precio")
                link = d.get("link") or d.get("url") or d.get("productUrl") or d.get("href")

                item = {
                    "ean": ean,
                    "nome": nome,
                    "marca": marca,
                    "preco": preco,
                    "loja": "carrefour_ar",
                    "link": response.urljoin(link) if link else None,
                }

                chave = json.dumps(item, sort_keys=True, ensure_ascii=False)
                if chave in vistos:
                    continue
                vistos.add(chave)

                yield item