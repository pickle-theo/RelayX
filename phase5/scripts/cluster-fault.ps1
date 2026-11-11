# Phase 4 cluster fault injection: kill -9 one gateway node and verify that the
# client fails over, the registry reclaims the dead node and cross-node
# delivery keeps working.
#
# Usage (from phase4/):
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/cluster-fault.ps1
#
# Evidence is written to docs/bench/raw/cluster/fault/ (overwritten per run).

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'cluster-common.ps1')

$exitCode = 0
try {
    Initialize-ClusterRun 'fault'
    # Run-scoped sender per client process: the demo client restarts
    # client_msg_id at 1 and (from_user, client_msg_id) is the global
    # idempotency key.
    $stamp = Get-Date -Format 'MMddHHmmss'
    $senderUser = "bob-$stamp-a"
    $senderUser2 = "bob-$stamp-b"
    $senderPattern = 'deliver from=' + [regex]::Escape($senderUser) + '/d1'
    $senderPattern2 = 'deliver from=' + [regex]::Escape($senderUser2) + '/d1'
    Add-Param "sender_users: $senderUser, $senderUser2 (run-scoped per client process, avoids the (from_user, client_msg_id) idempotency key)"
    Add-Param 'nodes: node1 127.0.0.1:5001/9101, node2 127.0.0.1:5002/9102, node3 127.0.0.1:5003/9103'
    Add-Param 'gateway_args: -redis-addr 127.0.0.1:6379 -node-heartbeat-interval 500ms -node-ttl 1500ms -node-cleanup-interval 500ms -stats-interval 2s'
    Add-Param 'client_addrs: 127.0.0.1:5003,127.0.0.1:5001,127.0.0.1:5002'
    Add-Param 'fault: Stop-Process -Id <node3 pid> -Force (kill -9 equivalent)'

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
    $gw3 = Start-GatewayProcess -Name 'gateway-node3' -NodeID 'node3' -Addr '127.0.0.1:5003' -NodeAddr '127.0.0.1:9103'
    Wait-TcpOpen @(
        '127.0.0.1:5001', '127.0.0.1:5002', '127.0.0.1:5003',
        '127.0.0.1:9101', '127.0.0.1:9102', '127.0.0.1:9103'
    ) 15
    Write-Step-Pass "3 gateway nodes healthy (6 ports) in $(Get-Elapsed $tStart)s"

    # 1) alice starts on node3 with the other nodes as failover addresses.
    $alice = Start-ClientProcess -Name 'alice-failover' -ClientArgs @(
        '-addr', '127.0.0.1:5003,127.0.0.1:5001,127.0.0.1:5002',
        '-user', 'alice', '-device', 'd1'
    )
    Wait-ClientLogMatch $alice 'logged in as alice/d1' 10 'alice failed to log in on node3' | Out-Null
    Write-Step-Pass 'alice/d1 logged in on node3 (127.0.0.1:5003, multi-address client)'
    $nodes = Get-RedisRouteNodes 'alice'
    Assert-True ($nodes -contains 'node3') "Redis route for alice points to node3 (found: $($nodes -join ','))"

    Wait-ClientLogMatch $alice 'pull_done count=' 10 'alice did not finish the initial PULL on node3' | Out-Null
    Start-Sleep -Milliseconds 300
    $baseline = Count-LogMatches (Get-ClientLogText $alice) $senderPattern
    Write-Step-Info "baseline deliveries of alice/d1 from $senderUser before the fault: $baseline"

    # 2) bob (run-scoped sender) sends two messages while node3 is still alive.
    $bob1 = Start-ClientProcess -Name 'bob-node1-before-kill' -ClientArgs @(
        '-addr', '127.0.0.1:5001', '-user', $senderUser, '-device', 'd1',
        '-to', 'alice', '-count', '2', '-interval', '200ms'
    )
    $bob1Code = Wait-ProcessExit $bob1 20 'bob sender (before kill) did not exit'
    Assert-True ($bob1Code -eq 0) 'bob sender before the kill exited with code 0'
    $delivered = Wait-ClientLogCount $alice $senderPattern ($baseline + 2) 10 `
        'alice did not receive the two messages sent to node3'
    Write-Step-Pass "alice received $($delivered - $baseline) cross-node deliveries before the fault"

    # 3) kill -9 node3.
    $tKill = Get-Date
    Stop-Process -Id $gw3.Process.Id -Force -ErrorAction SilentlyContinue
    [void]$gw3.Process.WaitForExit(5000)
    Assert-True ($gw3.Process.HasExited) 'node3 gateway killed with Stop-Process -Force (kill -9 equivalent)'

    # 4) the client must notice the dead node and switch.
    Wait-ClientLogMatch $alice 'connection lost, switching gateway node' 10 `
        'alice did not log the gateway switch after node3 died' | Out-Null
    $tSwitch = Get-Elapsed $tKill
    Wait-ClientLogMatch $alice 'connected to 127\.0\.0\.1:500(1|2) as ' 10 `
        'alice did not reconnect to node1/node2' | Out-Null
    $tReconnect = Get-Elapsed $tKill
    Write-Step-Pass "alice logged 'connection lost, switching gateway node' after ${tSwitch}s and reconnected in ${tReconnect}s"

    # 5) the registry must reclaim the dead node and its route index.
    Wait-RedisKeyGone 'relayx:node:node3' 10
    Wait-RedisKeyGone 'relayx:node_routes:node3' 10
    Write-Step-Pass "registry reclaimed node3: relayx:node:node3 and relayx:node_routes:node3 gone $(Get-Elapsed $tKill)s after the kill"

    # 6) delivery must still work after the switch.
    Start-Sleep -Milliseconds 500
    $afterSwitch = Count-LogMatches (Get-ClientLogText $alice) $senderPattern2
    Write-Step-Info "deliveries of alice/d1 from $senderUser2 after the switch: $afterSwitch"
    $bob2 = Start-ClientProcess -Name 'bob-node1-after-kill' -ClientArgs @(
        '-addr', '127.0.0.1:5001', '-user', $senderUser2, '-device', 'd1',
        '-to', 'alice', '-count', '2', '-interval', '200ms'
    )
    $bob2Code = Wait-ProcessExit $bob2 20 'bob sender (after kill) did not exit'
    Assert-True ($bob2Code -eq 0) 'bob sender after the kill exited with code 0'
    $delivered2 = Wait-ClientLogCount $alice $senderPattern2 ($afterSwitch + 2) 10 `
        'alice did not receive new deliveries after the failover'
    Write-Step-Pass "alice received $($delivered2 - $afterSwitch) new deliveries after the failover"

    # 7) the route must point at a live node, never at the dead one.
    $nodes = Get-RedisRouteNodes 'alice'
    Assert-True (($nodes -contains 'node1') -or ($nodes -contains 'node2')) `
        "Redis route for alice points to a live node (found: $($nodes -join ','))"
    Assert-True (-not ($nodes -contains 'node3')) 'Redis route for alice no longer points to the killed node3'
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
