import argparse
import csv
import re
import sys
import time
from pathlib import Path


def slugify(texto: str) -> str:
    texto = (texto or "").strip().lower()
    texto = re.sub(r"[^\w\s-]", "", texto, flags=re.UNICODE)
    texto = re.sub(r"[-\s]+", "-", texto)
    return (texto[:120] or "item").strip("-") or "item"


def garantir_coluna(fieldnames, coluna):
    fieldnames = list(fieldnames or [])
    if coluna not in fieldnames:
        fieldnames.append(coluna)
    return fieldnames


def tem_desconto(row):
    valor = str(row.get("desconto_percentual") or "").strip()
    return bool(valor)


def escrever_csv(caminho_saida: Path, fieldnames, rows):
    fieldnames = list(fieldnames or [])
    with caminho_saida.open("w", encoding="utf-8", newline="") as f:
        if fieldnames:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            if rows:
                writer.writerows(rows)
        else:
            f.write("")


def detectar_loja_pelo_nome_arquivo(nome_arquivo: str) -> str:
    nome = nome_arquivo.lower()
    if "carrefour" in nome:
        return "carrefour"
    if "jumbo" in nome:
        return "jumbo"
    if "masonline" in nome:
        return "masonline"
    return "geral"


def tirar_screenshot_e_separar(
    csv_entrada: Path,
    csv_promocoes: Path,
    csv_sem_promocoes: Path,
    pasta_prints: Path,
    headless: bool,
    timeout_ms: int,
    delay_s: float,
):
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        print(
            "Playwright não está instalado. Rode: pip install playwright && playwright install chromium",
            file=sys.stderr,
        )
        raise

    if not csv_entrada.exists():
        print(f"Arquivo CSV não encontrado: {csv_entrada}", file=sys.stderr)
        return

    pasta_prints.mkdir(parents=True, exist_ok=True)

    with csv_entrada.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        fieldnames_originais = reader.fieldnames
        if not fieldnames_originais:
            print(f"[{csv_entrada.name}] CSV vazio ou sem cabeçalho. Nada para processar.", file=sys.stderr)
            escrever_csv(csv_promocoes, [], [])
            escrever_csv(csv_sem_promocoes, [], [])
            return

        fieldnames = garantir_coluna(fieldnames_originais, "print_tela_path")
        rows = [
            row for row in reader
            if row and any(str(v or "").strip() for v in row.values())
        ]

    if "link" not in fieldnames_originais:
        print(f"[{csv_entrada.name}] O CSV precisa ter a coluna 'link'.", file=sys.stderr)
        escrever_csv(csv_promocoes, fieldnames, [])
        escrever_csv(csv_sem_promocoes, fieldnames, [])
        return

    if not rows:
        print(f"[{csv_entrada.name}] CSV com cabeçalho, mas sem linhas de dados. Nada para processar.", file=sys.stderr)
        escrever_csv(csv_promocoes, fieldnames, [])
        escrever_csv(csv_sem_promocoes, fieldnames, [])
        return

    rows_promocoes = []
    rows_sem_promocoes = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            device_scale_factor=1,
            color_scheme="light",
            locale="pt-BR",
        )

        total = len(rows)

        for i, row in enumerate(rows, start=1):
            row = dict(row)
            row.setdefault("print_tela_path", "")

            url = str(row.get("link") or "").strip()
            nome = str(row.get("nome") or f"item-{i}").strip()

            if not url:
                row["print_tela_path"] = ""
                print(f"[{csv_entrada.name}] [{i}/{total}] Sem link: {nome}")
            else:
                nome_arquivo = f"{i:03d}-{slugify(nome)}.png"
                caminho = pasta_prints / nome_arquivo

                page = None
                try:
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    try:
                        page.wait_for_load_state("networkidle", timeout=10000)
                    except Exception:
                        pass

                    page.screenshot(
                        path=str(caminho),
                        full_page=True,
                        animations="disabled",
                    )
                    row["print_tela_path"] = str(caminho.resolve())
                    print(f"[{csv_entrada.name}] [{i}/{total}] OK: {nome}")
                except Exception as e:
                    row["print_tela_path"] = ""
                    print(f"[{csv_entrada.name}] [{i}/{total}] ERRO: {nome} | {url} | {e}", file=sys.stderr)
                finally:
                    if page:
                        try:
                            page.close()
                        except Exception:
                            pass

                if delay_s > 0:
                    time.sleep(delay_s)

            if tem_desconto(row):
                rows_promocoes.append(row)
            else:
                rows_sem_promocoes.append(row)

        context.close()
        browser.close()

    escrever_csv(csv_promocoes, fieldnames, rows_promocoes)
    escrever_csv(csv_sem_promocoes, fieldnames, rows_sem_promocoes)

    print(f"\n[{csv_entrada.name}] CSV promoções gerado com sucesso: {csv_promocoes.resolve()}")
    print(f"[{csv_entrada.name}] CSV sem promoções gerado com sucesso: {csv_sem_promocoes.resolve()}")
    print(f"[{csv_entrada.name}] Prints salvos em: {pasta_prints.resolve()}")


def processar_multiplos_csvs(
    arquivos_csv,
    pasta_saida_base: Path,
    pasta_prints_base: Path,
    headless: bool,
    timeout_ms: int,
    delay_s: float,
):
    pasta_saida_base.mkdir(parents=True, exist_ok=True)
    pasta_prints_base.mkdir(parents=True, exist_ok=True)

    for caminho_str in arquivos_csv:
        csv_entrada = Path(caminho_str)

        loja = detectar_loja_pelo_nome_arquivo(csv_entrada.name)

        csv_promocoes = pasta_saida_base / f"{loja}_promocoes.csv"
        csv_sem_promocoes = pasta_saida_base / f"{loja}_sem_promocoes.csv"
        pasta_prints = pasta_prints_base / loja

        tirar_screenshot_e_separar(
            csv_entrada=csv_entrada,
            csv_promocoes=csv_promocoes,
            csv_sem_promocoes=csv_sem_promocoes,
            pasta_prints=pasta_prints,
            headless=headless,
            timeout_ms=timeout_ms,
            delay_s=delay_s,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Lê múltiplos CSVs com coluna link, tira prints e gera 2 CSVs por arquivo: promoções e sem promoções."
    )
    parser.add_argument(
        "--inputs",
        "-i",
        nargs="+",
        default=["carrefour_teste.csv", "jumbo_teste.csv", "masonline_teste.csv"],
        help="Lista de CSVs de entrada",
    )
    parser.add_argument(
        "--output-dir",
        default="saida_csv",
        help="Pasta onde salvar os CSVs de saída",
    )
    parser.add_argument(
        "--prints-dir",
        "-p",
        default="prints",
        help="Pasta base onde salvar os PNGs",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Mostra o navegador enquanto executa",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=45000,
        help="Timeout por página em ms",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay entre páginas em segundos",
    )
    args = parser.parse_args()

    processar_multiplos_csvs(
        arquivos_csv=args.inputs,
        pasta_saida_base=Path(args.output_dir),
        pasta_prints_base=Path(args.prints_dir),
        headless=not args.show_browser,
        timeout_ms=args.timeout_ms,
        delay_s=args.delay,
    )