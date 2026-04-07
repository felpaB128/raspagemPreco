[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [Console]::OutputEncoding

Set-Location $PSScriptRoot

$arquivoEntrada = "C:\Users\Felipe Braga\Desktop\trabalho\WebScraping\scraping\Leitura\BASE RETAIL MINORISTA 23_02_2026.xlsx"
$pythonVenv = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $pythonVenv)) {
    Write-Host "Python da virtualenv não encontrado em: $pythonVenv" -ForegroundColor Red
    exit 1
}

# -----------------------------
# CARREFOUR ARGENTINA
# -----------------------------
& $pythonVenv -m scrapy crawl produto_por_ean -a "arquivo_entrada=$arquivoEntrada" -a "loja=carrefour_ar" -O produtos_carrefour_ar.csv

if ($LASTEXITCODE -ne 0) {
    Write-Host "Spider Carrefour AR falhou (código $LASTEXITCODE). Processo interrompido." -ForegroundColor Red
    exit $LASTEXITCODE
}

& $pythonVenv .\tirar_prints.py `
    -i produtos_carrefour_ar.csv `
    --output-promocoes produtos_carrefour_ar_promocoes.csv `
    --output-sem-promocoes produtos_carrefour_ar_sem_promocoes.csv `
    -p prints

if ($LASTEXITCODE -ne 0) {
    Write-Host "tirar_prints.py do Carrefour AR falhou (código $LASTEXITCODE)." -ForegroundColor Red
    exit $LASTEXITCODE
}

# -----------------------------
# JUMBO ARGENTINA
# -----------------------------
& $pythonVenv -m scrapy crawl produto_por_ean -a "arquivo_entrada=$arquivoEntrada" -a "loja=jumbo_ar" -O produtos_jumbo_ar.csv

if ($LASTEXITCODE -ne 0) {
    Write-Host "Spider Jumbo AR falhou (código $LASTEXITCODE). Processo interrompido." -ForegroundColor Red
    exit $LASTEXITCODE
}

& $pythonVenv .\tirar_prints.py `
    -i produtos_jumbo_ar.csv `
    --output-promocoes produtos_jumbo_ar_promocoes.csv `
    --output-sem-promocoes produtos_jumbo_ar_sem_promocoes.csv `
    -p prints

if ($LASTEXITCODE -ne 0) {
    Write-Host "tirar_prints.py do Jumbo AR falhou (código $LASTEXITCODE)." -ForegroundColor Red
    exit $LASTEXITCODE
}

# -----------------------------
# MAS ONLINE ARGENTINA
# -----------------------------
& $pythonVenv -m scrapy crawl produto_por_ean -a "arquivo_entrada=$arquivoEntrada" -a "loja=masonline_ar" -O produtos_masonline_ar.csv

if ($LASTEXITCODE -ne 0) {
    Write-Host "Spider MasOnline AR falhou (código $LASTEXITCODE). Processo interrompido." -ForegroundColor Red
    exit $LASTEXITCODE
}

& $pythonVenv .\tirar_prints.py `
    -i produtos_masonline_ar.csv `
    --output-promocoes produtos_masonline_ar_promocoes.csv `
    --output-sem-promocoes produtos_masonline_ar_sem_promocoes.csv `
    -p prints

if ($LASTEXITCODE -ne 0) {
    Write-Host "tirar_prints.py do MasOnline AR falhou (código $LASTEXITCODE)." -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "Processo concluído com sucesso para as 3 lojas." -ForegroundColor Green