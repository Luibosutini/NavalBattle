# Naval Battle GUI Startup Script (Windows)
# ダブルクリック（または右クリック→PowerShellで実行）で
# ブラウザ GUI サーバを起動し、ブラウザを自動で開きます。
#
# 環境変数はリポジトリ直下の .env（GUI の「初期設定」から生成可能）か、
# 既に設定済みのものを使用します。

Write-Host "=== Naval Battle GUI Startup ===" -ForegroundColor Cyan

$repoRoot = (Resolve-Path "$PSScriptRoot\..").Path
Set-Location $repoRoot

# .env があれば読み込む
$envFile = Join-Path $repoRoot ".env"
if (Test-Path $envFile) {
    Write-Host "Loading $envFile" -ForegroundColor Green
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#=]+)=(.*)$') {
            [System.Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2])
        }
    }
}

Write-Host "Starting GUI server (http://127.0.0.1:8800/) ..." -ForegroundColor Yellow
python -m naval gui
