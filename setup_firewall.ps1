# PowerShell script to allow inbound traffic on ports 8001, 8002, 8003 for Raft Cluster
# Run as Administrator if required by your Windows policy

Write-Host "================================================================" -ForegroundColor Cyan
Write-Host " Adding Windows Defender Firewall rules for Raft Cluster (Ports 8001-8003)..." -ForegroundColor Cyan
Write-Host "================================================================" -ForegroundColor Cyan

$ports = @(8001, 8002, 8003)

foreach ($p in $ports) {
    $ruleName = "Raft Cluster Node $p"
    # Remove old rule if exists
    Remove-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
    # Add new inbound rule
    New-NetFirewallRule -DisplayName $ruleName `
                        -Direction Inbound `
                        -LocalPort $p `
                        -Protocol TCP `
                        -Action Allow `
                        -Profile Any `
                        -ErrorAction SilentlyContinue | Out-Null
    Write-Host " [Firewall] Port $p allowed (Inbound TCP)." -ForegroundColor Green
}

Write-Host "`nAll rules added successfully! Other devices on the same Wi-Fi can now connect." -ForegroundColor Green
