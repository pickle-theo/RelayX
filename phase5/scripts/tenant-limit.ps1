# Phase 5 tenant throttling verification:
#   A tenant with a low rate limit must not starve the other tenants, and a
#   throttled delivery is only postponed: the push attempt counter never grows
#   because of throttling.
#
# Usage (from phase5/):
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/tenant-limit.ps1
#
# Evidence is written to docs/bench/raw/engine/tenant-limit/ (overwritten per run).

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'engine-common.ps1')

$bigCount = 12
$smallCount = 4
$freeCount = 2

$exitCode = 0
try {
    Initialize-EngineRun 'tenant-limit'
    Add-Param 'gateway: 127.0.0.1:9000 node-a node-addr 127.0.0.1:9100'
    Add-Param 'message-service: 127.0.0.1:8081'
    Add-Param 'delivery-worker: worker-id w1 default-inflight 4 tenant-cache-ttl 1s'
    Add-Param "tenant big: rate_per_sec=1 burst=1 max_inflight=1 ($bigCount messages to bob)"
    Add-Param "tenant small: rate_per_sec=0 burst=0 max_inflight=0 ($smallCount messages to carol)"
    Add-Param "tenant free: no quota row ($freeCount messages to carol, default limits)"

    Test-DockerAvailable
    Write-Step-Pass 'docker is available'
    Assert-PostgresRunning
    Write-Step-Pass "PostgreSQL container $($script:PgContainer) is running"
    Ensure-RedisRunning
    Write-Step-Pass 'Redis container is running and answers PONG'
    Ensure-KafkaRunning
    Write-Step-Pass "Kafka container $($script:KafkaContainer) is running and answers --list"
    Reset-KafkaTopics
    Write-Step-Pass 'Kafka topics relayx.fanout.v1 and relayx.deliver.v1 recreated empty'

    Clear-EngineDatabase
    Write-Step-Pass 'deliveries/messages/endpoints/outbox truncated'

    # The throttled tenant gets a strict quota. 'small' is configured with an
    # explicit zero quota and 'free' has no row at all, so both must fall back
    # to the worker defaults.
    Invoke-Sql "INSERT INTO tenants (id, rate_per_sec, burst, max_inflight)
                VALUES ('big', 1, 1, 1)
                ON CONFLICT (id) DO UPDATE SET
                  rate_per_sec = EXCLUDED.rate_per_sec,
                  burst = EXCLUDED.burst,
                  max_inflight = EXCLUDED.max_inflight,
                  updated_at = now()" | Out-Null
    Invoke-Sql "INSERT INTO tenants (id, rate_per_sec, burst, max_inflight)
                VALUES ('small', 0, 0, 0)
                ON CONFLICT (id) DO UPDATE SET
                  rate_per_sec = EXCLUDED.rate_per_sec,
                  burst = EXCLUDED.burst,
                  max_inflight = EXCLUDED.max_inflight,
                  updated_at = now()" | Out-Null
    Invoke-Sql "DELETE FROM tenants WHERE id = 'free'" | Out-Null
    Write-Step-Pass "tenant quotas written (big=1/s burst 1 inflight 1, small=zero quota, free=unconfigured)"

    Build-Binaries
    Write-Step-Pass 'bin/gateway.exe, client.exe, message-service.exe, delivery-worker.exe built'

    Assert-PortsFree @(9000, 9100, 8081)
    Write-Step-Pass 'ports 9000, 9100 and 8081 are free'

    $gateway = Start-GatewayProcess -Name 'gateway' -Addr '127.0.0.1:9000' -NodeID 'node-a' -NodeAddr '127.0.0.1:9100'
    $svc = Start-MessageServiceProcess -Name 'message-service' -HttpAddr '127.0.0.1:8081'
    $worker = Start-WorkerProcess -Name 'worker-w1' -WorkerID 'w1' -LeaseTTL '30s' `
        -ScanInterval '200ms' -TenantCacheTTL '1s' -DefaultInflight '4'

    Wait-TcpOpen @('127.0.0.1:9000', '127.0.0.1:9100') 20
    Write-Step-Pass 'gateway TCP 9000 and node port 9100 are listening'
    Wait-HttpOK "$($script:MessageServiceUrl)/healthz" 20
    Write-Step-Pass 'message-service /healthz returns 200'
    Wait-LogMatch $worker 'delivery worker w1 started' 20 'delivery-worker w1 did not start' | Out-Null
    Write-Step-Pass 'delivery-worker w1 started'

    $bob = Start-ClientProcess -Name 'client-bob' -ClientArgs @(
        '-addr', '127.0.0.1:9000',
        '-name', 'bob', '-user', 'bob', '-device', 'phone',
        '-ack', 'both', '-pull=false', '-interval', '5s'
    )
    $carol = Start-ClientProcess -Name 'client-carol' -ClientArgs @(
        '-addr', '127.0.0.1:9000',
        '-name', 'carol', '-user', 'carol', '-device', 'laptop',
        '-ack', 'both', '-pull=false', '-interval', '5s'
    )
    Wait-LogMatch $bob 'logged in as bob/phone' 20 'bob/phone did not log in' | Out-Null
    Wait-LogMatch $carol 'logged in as carol/laptop' 20 'carol/laptop did not log in' | Out-Null
    Write-Step-Pass 'bob/phone and carol/laptop are online'

    for ($i = 1; $i -le $bigCount; $i++) {
        $r = Invoke-AcceptMessage -TenantID 'big' -FromUser 'alice' -FromDevice 'web' `
            -Target 'bob' -TargetType 'user' -Payload "big-$i" -ClientMsgID (5200 + $i) -TTLSeconds 300
        Assert-True ($r.StatusCode -eq 202) "the big tenant accepted message $i (got $($r.StatusCode))"
    }
    for ($i = 1; $i -le $smallCount; $i++) {
        $r = Invoke-AcceptMessage -TenantID 'small' -FromUser 'alice' -FromDevice 'web' `
            -Target 'carol' -TargetType 'user' -Payload "small-$i" -ClientMsgID (5300 + $i) -TTLSeconds 300
        Assert-True ($r.StatusCode -eq 202) "the small tenant accepted message $i (got $($r.StatusCode))"
    }
    for ($i = 1; $i -le $freeCount; $i++) {
        $r = Invoke-AcceptMessage -TenantID 'free' -FromUser 'alice' -FromDevice 'web' `
            -Target 'carol' -TargetType 'user' -Payload "free-$i" -ClientMsgID (5400 + $i) -TTLSeconds 300
        Assert-True ($r.StatusCode -eq 202) "the unconfigured tenant accepted message $i (got $($r.StatusCode))"
    }
    Write-Step-Pass "accepted $bigCount messages for 'big', $smallCount for 'small' and $freeCount for 'free'"

    Wait-SqlValue "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'big'" `
        "$bigCount" 60 'the big tenant did not get one delivery per message' | Out-Null
    Wait-SqlValue "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'small'" `
        "$smallCount" 60 'the small tenant did not get one delivery per message' | Out-Null
    Wait-SqlValue "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'free'" `
        "$freeCount" 60 'the unconfigured tenant did not get one delivery per message' | Out-Null
    Write-Step-Pass 'the fan-out created one delivery per accepted message for every tenant'

    # The big tenant is limited to one delivery per second, so the other tenants
    # have to make progress while it is held back.
    Start-Sleep -Seconds 6
    $smallSent = Invoke-Sql "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'small' AND d.state >= 1"
    Assert-True ([int]$smallSent -eq $smallCount) "the small tenant delivered all $smallCount messages (got $smallSent)"
    $freeSent = Invoke-Sql "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'free' AND d.state >= 1"
    Assert-True ([int]$freeSent -eq $freeCount) "the unconfigured tenant delivered all $freeCount messages (got $freeSent)"
    Write-Step-Pass 'the other tenants were not affected by the throttled tenant'

    $bigSent = Invoke-Sql "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'big' AND d.state >= 1"
    Assert-True ([int]$bigSent -lt $bigCount) "the big tenant is still throttled ($bigSent of $bigCount delivered)"
    Write-Step-Pass "the big tenant is throttled ($bigSent of $bigCount delivered after 6s)"

    # Throttling postpones a delivery, it never consumes a push attempt.
    $consumed = Invoke-Sql "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'big' AND d.state = 0 AND d.attempts > 0"
    Assert-True ($consumed -eq '0') "throttling consumed $consumed push attempts of the big tenant"
    $failed = Invoke-Sql "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'big' AND d.state = 6"
    Assert-True ($failed -eq '0') "the throttled tenant has $failed failed deliveries"
    $postponed = Invoke-Sql "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'big' AND d.state = 0 AND d.next_retry_at IS NOT NULL"
    Assert-True ([int]$postponed -ge 1) 'the throttled deliveries were postponed with next_retry_at'
    Write-Step-Pass 'throttling only moved next_retry_at, no push attempt was consumed'

    $throttled = Wait-LogMaxCounter -Entry $worker -Pattern 'throttled=(\d+)' -Minimum 1 -TimeoutSeconds 15 `
        -Description 'the worker stats do not report a throttled delivery'
    Assert-True ($throttled -ge 1) "the worker stats report throttled=$throttled"
    $statsText = Get-EntryLogText $worker
    Assert-True ($statsText -match 'tenant_inflight=') 'the worker stats report the per tenant in-flight counter'
    Assert-True ($statsText -match 'outbox_pending=') 'the worker stats report the outbox backlog'
    Assert-True ($statsText -match 'leases_held=') 'the worker stats report the held leases'
    Write-Step-Pass "the worker stats report the tenant counters (throttled=$throttled)"

    # Relaxing the quota drains the backlog of the throttled tenant.
    Invoke-Sql "UPDATE tenants SET rate_per_sec = 0, burst = 0, max_inflight = 0, updated_at = now() WHERE id = 'big'" | Out-Null
    Write-Step-Pass 'the quota of the big tenant was relaxed'

    Wait-SqlValue "SELECT count(*) FROM messages WHERE tenant_id = 'big' AND state = 2" "$bigCount" 90 `
        'the backlog of the big tenant was not drained after the quota was relaxed' | Out-Null
    Write-Step-Pass "all $bigCount messages of the big tenant completed after the quota was relaxed"

    Wait-SqlValue "SELECT count(*) FROM messages WHERE tenant_id = 'small' AND state = 2" "$smallCount" 30 `
        'the messages of the small tenant did not complete' | Out-Null
    Wait-SqlValue "SELECT count(*) FROM messages WHERE tenant_id = 'free' AND state = 2" "$freeCount" 30 `
        'the messages of the unconfigured tenant did not complete' | Out-Null
    Write-Step-Pass 'the messages of the other tenants completed as well'

    $pendingBig = Invoke-Sql "SELECT count(*) FROM deliveries d JOIN messages m ON m.id = d.message_id WHERE m.tenant_id = 'big' AND d.state = 0"
    Assert-True ($pendingBig -eq '0') "the big tenant still has $pendingBig pending deliveries"
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
