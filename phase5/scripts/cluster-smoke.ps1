# Phase 4 cluster smoke test: three gateway nodes, cross-node unicast delivery
# and cross-node duplicate login (kick).
#
# Usage (from phase4/):
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/cluster-smoke.ps1
#
# Evidence is written to docs/bench/raw/cluster/smoke/ (overwritten per run).

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'cluster-common.ps1')

$exitCode = 0
try {
    Initialize-ClusterRun 'smoke'
    # The demo client restarts client_msg_id at 1 for every process and
    # (from_user, client_msg_id) is the global idempotency key, so a fixed user
    # name would turn every send of a second run into "duplicate". A run-scoped
    # sender name keeps the scenario repeatable without deleting stored data.
    $stamp = Get-Date -Format 'MMddHHmmss'
    $senderUser = "bob-$stamp"
    $senderPattern = 'deliver from=' + [regex]::Escape($senderUser) + '/d1'
    Add-Param "sender_user: $senderUser (run-scoped, avoids the (from_user, client_msg_id) idempotency key)"
    Add-Param 'nodes: node1 127.0.0.1:5001/9101, node2 127.0.0.1:5002/9102, node3 127.0.0.1:5003/9103'
    Add-Param 'gateway_args: -redis-addr 127.0.0.1:6379 -node-heartbeat-interval 500ms -node-ttl 1500ms -node-cleanup-interval 500ms -stats-interval 2s'

    Test-DockerAvailable
    Write-Step-Pass 'docker is available'
    Assert-PostgresRunning
    Write-Step-Pass "PostgreSQL container $($script:PgContainer) is running"
    Ensure-RedisRunning
    Write-Step-Pass 'Redis container is running and answers PONG'
    Build-Binaries
    Write-Step-Pass 'bin/gateway.exe and bin/client.exe built'
    Assert-PortsFree @(5001, 5002, 5003, 9101, 9102, 9103)
    Write-Step-Pass 'cluster ports 5001-5003 and 9101-9103 are free'

    $tStart = Get-Date
    $null = Start-GatewayProcess -Name 'gateway-node1' -NodeID 'node1' -Addr '127.0.0.1:5001' -NodeAddr '127.0.0.1:9101'
    $null = Start-GatewayProcess -Name 'gateway-node2' -NodeID 'node2' -Addr '127.0.0.1:5002' -NodeAddr '127.0.0.1:9102'
    $null = Start-GatewayProcess -Name 'gateway-node3' -NodeID 'node3' -Addr '127.0.0.1:5003' -NodeAddr '127.0.0.1:9103'
    Wait-TcpOpen @(
        '127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003',
        '127.0.0.1:9101', '127.0.0.1:9102', '127.0.0.1:9103'
    ) 15
    Write-Step-Pass "3 gateway nodes healthy (6 ports) in $(Get-Elapsed $tStart)s"

    # 1) alice receives on node3.
    $alice3 = Start-ClientProcess -Name 'alice-node3' -ClientArgs @(
        '-addr', '127.0.0.1:5003', '-user', 'alice', '-device', 'd1'
    )
    Wait-ClientLogMatch $alice3 'logged in as alice/d1' 10 'alice failed to log in on node3' | Out-Null
    Write-Step-Pass 'alice/d1 logged in on node3 (127.0.0.1:5003)'
    Wait-ClientLogMatch $alice3 'pull_done count=' 10 'alice did not finish the initial PULL on node3' | Out-Null
    Start-Sleep -Milliseconds 300
    $baseline = Count-LogMatches (Get-ClientLogText $alice3) $senderPattern
    Write-Step-Info "baseline deliveries of alice/d1 from $senderUser before the test: $baseline"

    # 2) bob (run-scoped sender) sends three messages on node1.
    $bob = Start-ClientProcess -Name 'bob-node1-send' -ClientArgs @(
        '-addr', '127.0.0.1:5001', '-user', $senderUser, '-device', 'd1',
        '-to', 'alice', '-count', '3', '-interval', '300ms'
    )
    $bobCode = Wait-ProcessExit $bob 20 'bob sender did not exit'
    Assert-True ($bobCode -eq 0) "bob sender on node1 exited with code 0"
    $acks = Count-LogMatches (Get-ClientLogText $bob) 'send_ack status=accepted'
    Assert-True ($acks -ge 3) "bob received 3 accepted SEND_ACKs (got $acks)"

    # 3) cross-node unicast node1 -> node3.
    $tDeliver = Get-Date
    $delivered = Wait-ClientLogCount $alice3 $senderPattern ($baseline + 3) 10 `
        'cross-node deliveries from node1 to node3 are missing'
    Write-Step-Pass "node3 received $($delivered - $baseline) new cross-node delivers from $senderUser/d1 in $(Get-Elapsed $tDeliver)s"

    # 4) alice logs in again on node2: node3 must be kicked.
    $tKick = Get-Date
    $alice2 = Start-ClientProcess -Name 'alice-node2-dup-login' -ClientArgs @(
        '-addr', '127.0.0.1:5002', '-user', 'alice', '-device', 'd1',
        '-to', $senderUser, '-count', '1', '-interval', '100ms'
    )
    Wait-ClientLogMatch $alice3 'server closed connection' 8 `
        'node3 did not report server closed connection for the kicked session' | Out-Null
    Write-Step-Pass "node3 receiver observed 'server closed connection' after the node2 login"

    $alice3Code = Wait-ProcessExit $alice3 8 'kicked node3 receiver did not exit'
    $kickElapsed = Get-Elapsed $tKick
    Assert-True ($kickElapsed -le 8) "kicked node3 receiver exited within ${kickElapsed}s (<= 8s)"
    Write-Step-Info "kicked receiver exit code: $alice3Code"

    $dupCode = Wait-ProcessExit $alice2 8 'alice duplicate-login client did not exit'
    Assert-True ($dupCode -eq 0) "alice duplicate-login client on node2 exited with code 0"

    # 5) the route must now belong to node2. A route only exists while its
    # session is live, and the temporary duplicate-login client deleted its
    # route on exit, so the assertion runs with the replacement session.
    Write-Step-Info 'the duplicate-login client deletes its route on exit; a live node2 session is asserted instead'
    $aliceLive = Start-ClientProcess -Name 'alice-node2-live' -ClientArgs @(
        '-addr', '127.0.0.1:5002', '-user', 'alice', '-device', 'd1'
    )
    Wait-ClientLogMatch $aliceLive 'logged in as alice/d1' 10 'alice failed to log in on node2' | Out-Null
    $nodes = Get-RedisRouteNodes 'alice'
    Assert-True ($nodes -contains 'node2') "Redis route for alice points to node2 (found: $($nodes -join ','))"
    Assert-True (-not ($nodes -contains 'node3')) "Redis route for alice no longer points to node3"
}
catch {
    $msg = $_.Exception.Message
    if ($msg -notlike 'FAIL:*') {
        Write-Host "FAIL: $msg"
        $script:Steps.Add("FAIL: $msg")
    }
    $exitCode = 1
}
finally {
    Stop-AllStartedProcesses
    Write-Summary $exitCode
}

exit $exitCode
