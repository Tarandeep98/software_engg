@echo off
:: Windows Batch script to allow inbound traffic on ports 8001, 8002, 8003 for Raft Cluster
echo ================================================================
echo  Adding Windows Defender Firewall rules for Raft Cluster (Ports 8001-8003)...
echo  Note: If prompted, please click "Yes" to allow Administrator rights.
echo ================================================================

netsh advfirewall firewall add rule name="Raft Cluster Node 8001" dir=in action=allow protocol=TCP localport=8001
netsh advfirewall firewall add rule name="Raft Cluster Node 8002" dir=in action=allow protocol=TCP localport=8002
netsh advfirewall firewall add rule name="Raft Cluster Node 8003" dir=in action=allow protocol=TCP localport=8003

echo.
echo ================================================================
echo  Done! Ports 8001, 8002, and 8003 are now open for local Wi-Fi devices.
echo ================================================================
pause
