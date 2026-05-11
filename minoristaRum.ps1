[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [Console]::OutputEncoding

Set-Location $PSScriptRoot

$arquivoEntrada = "C:\Users\Felipe Braga\Desktop\trabalho\WebScraping\scraping\Leitura\BASE RETAIL MINORISTA 23_02_2026.xlsx"
$pythonVenv = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$pastaSaida = Join-Path $PSScriptRoot "minorista_csv"

if (-not (Test-Path $pythonVenv)) {
    Write-Host "Python da virtualenv não encontrado em: $pythonVenv" -ForegroundColor Red
    exit 1
}

if (-not (Test-Path $arquivoEntrada)) {
    Write-Host "Arquivo de entrada não encontrado em: $arquivoEntrada" -ForegroundColor Red
    exit 1
}

New-Item -ItemType Directory -Force -Path $pastaSaida | Out-Null

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

$csvCarrefour = Join-Path $pastaSaida "carrefour_saida.csv"
$csvJumbo = Join-Path $pastaSaida "jumbo_saida.csv"
$csvMasonline = Join-Path $pastaSaida "masonline_saida.csv"

# 1) CARREFOUR
Executar-Python "Rodando spider Carrefour" @(
    "-m", "scrapy", "crawl", "carrefour_mr",
    "-a", "arquivo_entrada=$arquivoEntrada",
    "-O", $csvCarrefour
)

# 2) JUMBO
Executar-Python "Rodando spider Jumbo" @(
    "-m", "scrapy", "crawl", "jumbo_search",
    "-a", "ean_file=$arquivoEntrada",
    "-O", $csvJumbo
)

# 3) MASONLINE
Executar-Python "Rodando spider Masonline" @(
    "-m", "scrapy", "crawl", "masonline",
    "-a", "arquivo_entrada=$arquivoEntrada",
    "-O", $csvMasonline
)

Write-Host ""
Write-Host "Processo concluído com sucesso para os 3 spiders." -ForegroundColor Green
Write-Host "Arquivos CSV gerados em: $pastaSaida" -ForegroundColor Green
Read-Host "Pressione Enter para sair"