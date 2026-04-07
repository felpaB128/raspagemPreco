import argparse
import csv
from pathlib import Path


def tem_desconto(row):
    valor = (row.get("desconto_percentual") or "").strip()
    return bool(valor)


def escrever_csv(caminho_saida: Path, fieldnames, rows):
    with caminho_saida.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def separar_promocoes_arquivo(
    csv_entrada: Path,
    csv_promocoes: Path,
    csv_sem_promocoes: Path,
):
    if not csv_entrada.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_entrada}")

    with csv_entrada.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError(f"[{csv_entrada.name}] CSV sem cabeçalho.")

        rows_promocoes = []
        rows_sem_promocoes = []

        for row in reader:
            if not any((v or "").strip() for v in row.values()):
                continue

            if tem_desconto(row):
                rows_promocoes.append(row)
            else:
                rows_sem_promocoes.append(row)

        fieldnames = reader.fieldnames

    escrever_csv(csv_promocoes, fieldnames, rows_promocoes)
    escrever_csv(csv_sem_promocoes, fieldnames, rows_sem_promocoes)

    print(
        f"[{csv_entrada.name}] CSV promoções gerado: {csv_promocoes.resolve()} "
        f"({len(rows_promocoes)} itens)"
    )
    print(
        f"[{csv_entrada.name}] CSV sem promoções gerado: {csv_sem_promocoes.resolve()} "
        f"({len(rows_sem_promocoes)} itens)"
    )


def detectar_loja_pelo_nome_arquivo(nome_arquivo: str) -> str:
    nome = nome_arquivo.lower()
    if "carrefour" in nome:
        return "carrefour"
    if "jumbo" in nome:
        return "jumbo"
    if "masonline" in nome:
        return "masonline"
    return Path(nome_arquivo).stem


def separar_promocoes_multiplos(
    arquivos_entrada,
    pasta_saida: Path,
):
    pasta_saida.mkdir(parents=True, exist_ok=True)

    for caminho_str in arquivos_entrada:
        csv_entrada = Path(caminho_str)

        loja = detectar_loja_pelo_nome_arquivo(csv_entrada.name)

        csv_promocoes = pasta_saida / f"{loja}_promocoes.csv"
        csv_sem_promocoes = pasta_saida / f"{loja}_sem_promocoes.csv"

        separar_promocoes_arquivo(
            csv_entrada=csv_entrada,
            csv_promocoes=csv_promocoes,
            csv_sem_promocoes=csv_sem_promocoes,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Separa produtos em promoção e sem promoção para múltiplos CSVs."
    )
    parser.add_argument(
        "--inputs",
        "-i",
        nargs="+",
        default=["carrefour_teste.csv", "jumbo_teste.csv", "masonline_teste.csv"],
        help="Lista de CSVs de entrada (com coluna desconto_percentual)",
    )
    parser.add_argument(
        "--output-dir",
        default="saida_promocoes",
        help="Pasta onde salvar os CSVs resultantes",
    )
    args = parser.parse_args()

    separar_promocoes_multiplos(
        arquivos_entrada=args.inputs,
        pasta_saida=Path(args.output_dir),
    )