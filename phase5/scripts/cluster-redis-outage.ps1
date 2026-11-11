# Phase 4 Redis outage test: a stopped Redis must not break SEND acceptance,
# must reject new logins with an internal LOGIN_ACK, and after recovery the
# offline messages must be replayed through PULL.
#
# Usage (from phase4/):
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/cluster-redis-outage.ps1
#
# Evidence is written to docs/bench/raw/cluster/redis-outage/ (overwritten per
# run). Redis is always restarted in the finally block.

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'cluster-common.ps1')

$exitCode = 0
try {
    Initialize-ClusterRun 'redis-outage'
    # Run-scoped sender: the demo client restarts client_msg_id at 1 and
    # (from_user, client_msg_id) is the global idempotency key.
    $stamp = Get-Date -Format 'MMddHHmmss'
    $senderUser = "bob-$stamp"
    Add-Param "sender_user: $senderUser (run-scoped, avoids the (from_user, client_msg_id) idempotency key)"
    Add-Param 'nodes: node1 127.0.0.1:5001/9101, node2 127.0.0.1:5002/9102, node3 127.0.0.1:5003/9103'
    Add-Param 'gateway_args: -redis-addr 127.0.0.1:6379 -node-heartbeat-interval 500ms -node-ttl 1500ms -node-cleanup-interval 500ms -stats-interval 2s'
    Add-Param 'fault: docker stop -t 1 relayx-phase4-redis'

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

    # Prepare alice's endpoint row (SEND resolves -to alice from PostgreSQL even
    # while alice is offline). A short login creates it.
    $alicePrep = Start-ClientProcess -Name 'alice-endpoint-prep' -ClientArgs @(
        '-addr', '127.0.0.1:5001', '-user', 'alice', '-device', 'd1',
        '-count', '1', '-interval', '1s'
    )
    $prepCode = Wait-ProcessExit $alicePrep 15 'alice endpoint preparation client did not exit'
    Assert-True (($prepCode -eq 0) -and ((Get-ClientLogText $alicePrep) -match 'logged in as alice/d1')) `
        'alice endpoint prepared in PostgreSQL (login-only client)'

    # 1) bob (run-scoped sender) sends two messages while Redis is still up.
    $bob = Start-ClientProcess -Name 'bob-node1-outage' -ClientArgs @(
        '-addr', '127.0.0.1:5001', '-user', $senderUser, '-device', 'd1',
        '-to', 'alice', '-count', '2', '-interval', '4s'
    )
    Wait-ClientLogMatch $bob "logged in as $senderUser/d1" 10 'bob failed to log in before the outage' | Out-Null
    Write-Step-Pass "bob ($senderUser)/d1 logged in on node1 before the outage"

    $tStop = Get-Date
    Stop-RedisContainer
    Write-Step-Pass "Redis stopped (docker stop -t 1) in $(Get-Elapsed $tStop)s"

    # 2) SEND must still be accepted and persisted while Redis is down.
    $bobCode = Wait-ProcessExit $bob 20 'bob sender did not finish during the Redis outage'
    Assert-True ($bobCode -eq 0) 'bob sender exited with code 0 during the Redis outage'
    $acks = Count-LogMatches (Get-ClientLogText $bob) 'send_ack status=accepted'
    Assert-True ($acks -ge 2) "SEND stayed durable with Redis down: $acks accepted SEND_ACKs"

    # 3) a new login must fail with an internal LOGIN_ACK.
    $carol = Start-ClientProcess -Name 'carol-login-outage' -ClientArgs @(
        '-addr', '127.0.0.1:5001', '-user', 'carol', '-device', 'd1', '-count', '1'
    )
    $carolCode = Wait-ProcessExit $carol 15 'carol login client did not exit during the outage'
    Assert-True ($carolCode -ne 0) "login while Redis is down exited non-zero (code $carolCode)"
    $carolText = Get-ClientLogText $carol
    Assert-True (($carolText -match 'login rejected') -and ($carolText -match 'status=3|cluster unavailable')) `
        'LOGIN_ACK internal: carol log reports login rejected / cluster unavailable'

    # 4) recover Redis and verify the offline messages are replayed.
    $tRedis = Get-Date
    Start-RedisContainer
    Write-Step-Pass "Redis restarted and answering PONG in $(Get-Elapsed $tRedis)s"

    $alice = Start-ClientProcess -Name 'alice-node1-pull' -ClientArgs @(
        '-addr', '127.0.0.1:5001', '-user', 'alice', '-device', 'd1'
    )
    Wait-ClientLogMatch $alice 'logged in as alice/d1' 15 'alice did not log in after Redis recovered' | Out-Null
    Write-Step-Pass 'alice/d1 logged in on node1 after Redis recovered'

    $deliveries = 0
    $maxPull = 0
    $deadline = (Get-Date).AddSeconds(15)
    while ((Get-Date) -lt $deadline) {
        $text = Get-ClientLogText $alice
        $deliveries = Count-LogMatches $text ('deliver from=' + [regex]::Escape($senderUser) + '/d1')
        $pullCounts = @([regex]::Matches($text, 'pull_done count=(\d+)') |
            ForEach-Object { [int]$_.Groups[1].Value })
        if ($pullCounts.Count -gt 0) {
            $maxPull = ($pullCounts | Measure-Object -Maximum).Maximum
        }
        if ($deliveries -ge 2 -or $maxPull -ge 2) {
            break
        }
        Start-Sleep -Milliseconds 100
    }
    Assert-True (($deliveries -ge 2) -or ($maxPull -ge 2)) `
        "offline messages replayed after Redis recovery (deliveries=$deliveries, max pull_done count=$maxPull)"
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
    try {
        $running = (& docker inspect -f '{{.State.Running}}' $script:RedisContainer 2>$null | Out-String).Trim()
        if ($running -ne 'true') {
            & docker start $script:RedisContainer | Out-Null
            if ($LASTEXITCODE -eq 0) {
                Wait-RedisPong 30
                Write-Host 'cleanup: Redis container restarted'
            }
        }
    }
    catch {
        Write-Host "cleanup: failed to restore Redis: $($_.Exception.Message)"
    }
    Write-Summary $exitCode
}

exit $exitCode
