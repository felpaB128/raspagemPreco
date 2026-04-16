[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [Console]::OutputEncoding

Set-Location $PSScriptRoot

$arquivoEntrada = "C:\Users\Felipe Braga\Desktop\trabalho\WebScraping\scraping\Leitura\Makro Mensual -  PS y SS-SE ACTUALIZADO - PRECIOS.xlsx"
$pythonVenv = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$pastaSaida = Join-Path $PSScriptRoot "saida_csv"
$pastaPromos = Join-Path $PSScriptRoot "saida_promocoes"

if (-not (Test-Path $pythonVenv)) {
    Write-Host "Python da virtualenv não encontrado em: $pythonVenv" -ForegroundColor Red
    exit 1
}

New-Item -ItemType Directory -Force -Path $pastaSaida | Out-Null
New-Item -ItemType Directory -Force -Path $pastaPromos | Out-Null

function Executar-Python {
    param(
        [string]$Descricao,
        [string[]]$Argumentos
    )

    Write-Host ""
    Write-Host "==================================================" -ForegroundColor DarkCyan
    Write-Host $Descricao -ForegroundColor Cyan
    Write-Host "==================================================" -ForegroundColor DarkCyan

    & $pythonVenv @Argumentos
    $codigo = $LASTEXITCODE

    if ($codigo -ne 0) {
        Write-Host "$Descricao falhou (código $codigo)." -ForegroundColor Red
        exit $codigo
    }
}

$csvCoto = Join-Path $pastaSaida "produtos_coto.csv"
$csvCarrefour = Join-Path $pastaSaida "produtos_carrefour.csv"
$csvDia = Join-Path $pastaSaida "produtos_dia.csv"
$csvQuarto = Join-Path $pastaSaida "produtos_quarto_mercado.csv"

# 1) COTO
Executar-Python "Rodando spider Coto" @(
    "-m", "scrapy", "crawl", "cotodigital_mk",
    "-a", "input_file=$arquivoEntrada",
    "-a", "store_id=200",
    "-O", $csvCoto
)

# 2) CARREFOUR
Executar-Python "Rodando spider Carrefour" @(
    "-m", "scrapy", "crawl", "carrefour_mk",
    "-a", "arquivo_entrada=$arquivoEntrada",
    "-O", $csvCarrefour
)

# 3) DIA
Executar-Python "Rodando spider Dia" @(
    "-m", "scrapy", "crawl", "supermercadosdia_mk",
    "-a", "arquivo_entrada=$arquivoEntrada",
    "-O", $csvDia
)

# 4) QUARTO SCRIPT / MERCADO
Executar-Python "Rodando quarto spider" @(
    "-m", "scrapy", "crawl", "produto_por_ean",
    "-a", "arquivo_entrada=$arquivoEntrada",
    "-a", "loja=masonline_ar",
    "-O", $csvQuarto
)

# SEPARAR PROMOÇÕES
Executar-Python "Separando promoções e não promoções dos 4 CSVs" @(
    ".\separar_promocoes.py",
    "--inputs", $csvCoto, $csvCarrefour, $csvDia, $csvQuarto,
    "--output-dir", $pastaPromos
)

Write-Host "" 
Write-Host "Processo concluído com sucesso para os 4 scripts." -ForegroundColor Green
Write-Host "CSVs brutos: $pastaSaida" -ForegroundColor Green
Write-Host "CSVs separados em promoções/sem promoções: $pastaPromos" -ForegroundColor Green