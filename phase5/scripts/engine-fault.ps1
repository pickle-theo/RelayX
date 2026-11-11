# Phase 5 engine fault-injection verification:
#   1. A delivery worker is killed (the Windows equivalent of kill -9) while it
#      holds a row lease. Another worker must take the row over after the lease
#      expires and finish the delivery.
#   2. Kafka is stopped: acceptance still succeeds and the outbox row stays
#      pending. After Kafka comes back the backlog drains and the delivery is
#      completed.
#
# Usage (from phase5/):
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/engine-fault.ps1
#
# Evidence is written to docs/bench/raw/engine/fault/ (overwritten per run).

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'engine-common.ps1')

# A TEST-NET-1 address never answers, so dialing it blocks until the node dial
# timeout expires. Gateway B advertises it, which keeps the delivery leased
# while the worker is killed.
$blackholeAddr = '192.0.2.1:9199'

$exitCode = 0
try {
    Initialize-EngineRun 'fault'
    Add-Param 'gateway A: 127.0.0.1:9000 node-a node-addr 127.0.0.1:9100 advertise 127.0.0.1:9100'
    Add-Param "gateway B: 127.0.0.1:9001 node-b node-addr 127.0.0.1:9101 advertise $blackholeAddr"
    Add-Param 'message-service: 127.0.0.1:8081'
    Add-Param 'worker w1: lease-ttl 40s node-dial-timeout 60s (killed during the push)'
    Add-Param 'worker w2: lease-ttl 40s (takes the lease over after it expires)'
    Add-Param 'kafka: stopped for the acceptance phase, restarted for the drain phase'

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

    Build-Binaries
    Write-Step-Pass 'bin/gateway.exe, client.exe, message-service.exe, delivery-worker.exe built'

    Assert-PortsFree @(9000, 9001, 9100, 9101, 8081)
    Write-Step-Pass 'ports 9000, 9001, 9100, 9101 and 8081 are free'

    $gatewayA = Start-GatewayProcess -Name 'gateway-a' -Addr '127.0.0.1:9000' `
        -NodeID 'node-a' -NodeAddr '127.0.0.1:9100' -NodeAdvertiseAddr '127.0.0.1:9100'
    $gatewayB = Start-GatewayProcess -Name 'gateway-b' -Addr '127.0.0.1:9001' `
        -NodeID 'node-b' -NodeAddr '127.0.0.1:9101' -NodeAdvertiseAddr $blackholeAddr
    $svc = Start-MessageServiceProcess -Name 'message-service' -HttpAddr '127.0.0.1:8081'
    $worker1 = Start-WorkerProcess -Name 'worker-w1' -WorkerID 'w1' -LeaseTTL '40s' `
        -ScanInterval '200ms' -NodeDialTimeout '60s'

    Wait-TcpOpen @('127.0.0.1:9000', '127.0.0.1:9100', '127.0.0.1:9001', '127.0.0.1:9101') 20
    Write-Step-Pass 'both gateways listen on their client and node ports'
    Wait-HttpOK "$($script:MessageServiceUrl)/healthz" 20
    Write-Step-Pass 'message-service /healthz returns 200'
    Wait-LogMatch $worker1 'delivery worker w1 started' 20 'delivery-worker w1 did not start' | Out-Null
    Write-Step-Pass 'delivery-worker w1 started'

    # The session lives on gateway B, whose advertised node address is the
    # blackhole. The worker therefore blocks in the NODE_DELIVER dial and keeps
    # the lease while it is killed.
    $bobOnB = Start-ClientProcess -Name 'client-bob-on-b' -ClientArgs @(
        '-addr', '127.0.0.1:9001',
        '-name', 'bob', '-user', 'bob', '-device', 'phone',
        '-ack', 'both', '-pull=false', '-interval', '5s'
    )
    Wait-LogMatch $bobOnB 'logged in as bob/phone' 20 'bob/phone did not log in to gateway B' | Out-Null
    Wait-SqlValue "SELECT count(*) FROM endpoints WHERE user_id = 'bob' AND device_id = 'phone'" '1' 10 `
        'bob/phone endpoint was not recorded by the login' | Out-Null
    Write-Step-Pass 'bob/phone logged in on gateway B and its endpoint row exists'

    $res = Invoke-AcceptMessage -TenantID 'default' -FromUser 'alice' -FromDevice 'web' `
        -Target 'bob' -TargetType 'user' -Payload 'phase5-fault-payload' `
        -ClientMsgID 5101 -TTLSeconds 300
    Assert-True ($res.StatusCode -eq 202) "POST /v1/messages returned HTTP 202 (got $($res.StatusCode))"
    $msgId = $res.MessageID
    Add-Param "crash message_id: $msgId"

    Wait-SqlValue "SELECT count(*) FROM deliveries WHERE message_id = $msgId" '1' 30 `
        'fan-out did not create the delivery for bob/phone' | Out-Null
    Write-Step-Pass 'fan-out created exactly one delivery row'

    # The worker claims the row and blocks in the dial to the blackhole address.
    $owner = Wait-SqlUntil -Sql "SELECT coalesce(worker_id,'') FROM deliveries WHERE message_id = $msgId" `
        -Predicate { param($v) $v -eq 'w1' } -TimeoutSeconds 30 `
        -Description 'worker w1 did not take the lease on the delivery'
    Write-Step-Pass "worker w1 holds the lease on delivery of message $msgId"
    $leased = Invoke-Sql "SELECT (lease_until > now())::int FROM deliveries WHERE message_id = $msgId"
    Assert-True ($leased -eq '1') 'the leased row has lease_until in the future'

    # kill -9: taskkill /F terminates the process without any graceful shutdown.
    & taskkill /PID $worker1.Process.Id /T /F 2>&1 | Out-Null
    Wait-ProcessExit $worker1 15 'worker w1 did not die after taskkill /F' | Out-Null
    Write-Step-Pass "worker w1 was killed (taskkill /F, pid $($worker1.Process.Id))"

    $stillOwned = Invoke-Sql "SELECT coalesce(worker_id,'') FROM deliveries WHERE message_id = $msgId"
    Assert-True ($stillOwned -eq 'w1') "the killed worker still owns the lease (owner '$stillOwned')"
    $leaseHeld = Invoke-Sql "SELECT (lease_until > now())::int FROM deliveries WHERE message_id = $msgId"
    Assert-True ($leaseHeld -eq '1') 'the lease is still valid after the crash'
    Write-Step-Pass 'the crash left the lease behind, so no other worker may claim the row yet'

    # Move the session to gateway A later, after the takeover was observed: while
    # bob/phone stays on gateway B every push blocks in the dial to the
    # blackhole address, so the owner of the lease stays visible in the table.
    $worker2 = Start-WorkerProcess -Name 'worker-w2' -WorkerID 'w2' -LeaseTTL '40s' `
        -ScanInterval '200ms' -NodeDialTimeout '5s'
    Wait-LogMatch $worker2 'delivery worker w2 started' 20 'delivery-worker w2 did not start' | Out-Null
    Write-Step-Pass 'delivery-worker w2 started'

    # While the lease of the dead worker is still valid nobody may claim the row.
    $remaining = [int](Invoke-Sql "SELECT coalesce(floor(extract(epoch from (lease_until - now())))::int, 0) FROM deliveries WHERE message_id = $msgId")
    $hold = [Math]::Min(15, [Math]::Max(1, $remaining - 15))
    Start-Sleep -Seconds $hold
    $duringLease = Invoke-Sql "SELECT coalesce(worker_id,'') FROM deliveries WHERE message_id = $msgId"
    Assert-True ($duringLease -eq 'w1') "worker w2 did not steal the row while the lease was valid (owner '$duringLease')"
    $attemptsDuringLease = Invoke-Sql "SELECT attempts FROM deliveries WHERE message_id = $msgId"
    Assert-True ($attemptsDuringLease -eq '0') "no push attempt was made while the lease was valid (attempts $attemptsDuringLease)"
    $claimedDuringLease = Get-LogMaxCounter $worker2 'claimed=(\d+)'
    Assert-True ($claimedDuringLease -eq 0) "worker w2 did not claim any row while the lease was valid (claimed $claimedDuringLease)"
    Write-Step-Pass "the lease of the dead worker excluded worker w2 for ${hold}s"

    Wait-SqlUntil -Sql "SELECT coalesce(worker_id,'') FROM deliveries WHERE message_id = $msgId" `
        -Predicate { param($v) $v -eq 'w2' } -TimeoutSeconds 90 `
        -Description 'worker w2 did not take the row over after the lease expired' | Out-Null
    Write-Step-Pass 'worker w2 took the row over after the lease expired'

    $claimedAfter = Wait-LogMaxCounter -Entry $worker2 -Pattern 'claimed=(\d+)' -Minimum 1 -TimeoutSeconds 15 `
        -Description 'worker w2 did not report the claim of the taken over row'
    Assert-True ($claimedAfter -ge 1) "worker w2 claimed the row (claimed counter $claimedAfter)"

    # Move the session to gateway A so the retry of the taken over row can be
    # delivered.
    $bobOnA = Start-ClientProcess -Name 'client-bob-on-a' -ClientArgs @(
        '-addr', '127.0.0.1:9000',
        '-name', 'bob', '-user', 'bob', '-device', 'phone',
        '-ack', 'both', '-pull=false', '-interval', '5s'
    )
    Wait-LogMatch $bobOnA 'logged in as bob/phone' 20 'bob/phone did not log in to gateway A' | Out-Null
    Write-Step-Pass 'bob/phone reconnected on gateway A, the route now points at node-a'

    Wait-SqlUntil -Sql "SELECT state FROM deliveries WHERE message_id = $msgId" `
        -Predicate { param($v) [int]$v -ge 1 } -TimeoutSeconds 60 `
        -Description 'the delivery did not reach SENT after the takeover' | Out-Null
    Write-Step-Pass 'the delivery reached SENT after the takeover'
    Wait-SqlValue "SELECT state FROM messages WHERE id = $msgId" '2' 30 `
        'the message did not reach COMPLETED after the takeover' | Out-Null
    Write-Step-Pass 'the message reached COMPLETED after the takeover'

    $rows = Invoke-Sql "SELECT count(*) FROM deliveries WHERE message_id = $msgId"
    Assert-True ($rows -eq '1') "the takeover produced exactly one delivery row (got $rows)"
    Write-Step-Pass 'the takeover did not duplicate the delivery row'

    # --- Kafka outage --------------------------------------------------------
    Stop-KafkaContainer
    Write-Step-Pass "Kafka container $($script:KafkaContainer) stopped"

    $res2 = Invoke-AcceptMessage -TenantID 'default' -FromUser 'alice' -FromDevice 'web' `
        -Target 'bob' -TargetType 'user' -Payload 'phase5-fault-kafka-down' `
        -ClientMsgID 5102 -TTLSeconds 300
    Assert-True ($res2.StatusCode -eq 202) "acceptance still returned HTTP 202 while Kafka was down (got $($res2.StatusCode))"
    $msgId2 = $res2.MessageID
    Add-Param "kafka-down message_id: $msgId2"

    $pending = Invoke-Sql "SELECT count(*) FROM outbox WHERE state = 0 AND (payload->>'message_id')::bigint = $msgId2"
    Assert-True ($pending -eq '1') "the outbox row of message $msgId2 is pending while Kafka is down"
    Write-Step-Pass 'the outbox row stays pending while Kafka is down'

    # The gateway SEND path uses the same acceptance transaction, so it has to
    # succeed while the event pipeline is down.
    $alice = Start-ClientProcess -Name 'client-alice' -ClientArgs @(
        '-addr', '127.0.0.1:9000',
        '-name', 'alice', '-user', 'alice', '-device', 'web',
        '-to', 'bob', '-count', '1', '-interval', '1s',
        '-ack', 'none', '-pull=false'
    )
    Wait-LogMatch $alice 'send_ack status=accepted' 30 `
        'the gateway SEND path did not accept the message while Kafka was down' | Out-Null
    Write-Step-Pass 'the gateway SEND path still accepted the message while Kafka was down'

    # The relay records the failed publication: the attempt counter grows and
    # the next attempt is scheduled.
    Wait-SqlUntil -Sql "SELECT attempts FROM outbox WHERE state = 0 AND (payload->>'message_id')::bigint = $msgId2" `
        -Predicate { param($v) [int]$v -ge 1 } -TimeoutSeconds 30 `
        -Description 'the outbox relay did not record a failed publication attempt' | Out-Null
    $nextAttempt = Invoke-Sql "SELECT (next_attempt_at IS NOT NULL)::int FROM outbox WHERE state = 0 AND (payload->>'message_id')::bigint = $msgId2"
    Assert-True ($nextAttempt -eq '1') 'the failed outbox row has a scheduled next attempt'

    # The relay that owns the row logs the failed publication. Gateway and
    # worker both run an in-process relay, so any of them may report it.
    $relayFailure = $false
    $deadline = (Get-Date).AddSeconds(20)
    while (-not $relayFailure -and (Get-Date) -lt $deadline) {
        foreach ($entry in @($worker2, $gatewayA, $gatewayB)) {
            if ((Get-EntryLogText $entry) -match 'outbox relay failed') {
                $relayFailure = $true
                break
            }
        }
        if (-not $relayFailure) {
            Start-Sleep -Milliseconds 250
        }
    }
    Assert-True $relayFailure 'no relay logged the failed publication to Kafka'
    Write-Step-Pass 'the outbox relay recorded the failure (attempts grew, next attempt scheduled)'

    Start-KafkaContainer
    Write-Step-Pass 'Kafka container restarted and the topics are available again'

    # The consumer cannot survive a broker outage, so the worker may have died
    # with it. A fresh worker drains the backlog.
    $worker2.Process.Refresh()
    if ($worker2.Process.HasExited) {
        Write-Step-Info 'worker w2 died with the Kafka outage, starting worker w3'
        $worker3 = Start-WorkerProcess -Name 'worker-w3' -WorkerID 'w3' -LeaseTTL '30s' -ScanInterval '200ms'
        Wait-LogMatch $worker3 'delivery worker w3 started' 20 'delivery-worker w3 did not start' | Out-Null
    }

    Wait-SqlValue "SELECT state FROM outbox WHERE (payload->>'message_id')::bigint = $msgId2" '1' 90 `
        'the outbox backlog was not published after Kafka came back' | Out-Null
    Write-Step-Pass 'the outbox backlog was published after Kafka came back'

    Wait-SqlValue "SELECT count(*) FROM deliveries WHERE message_id = $msgId2" '1' 60 `
        'the fan-out did not create the delivery for the message accepted during the outage' | Out-Null
    Wait-SqlUntil -Sql "SELECT state FROM deliveries WHERE message_id = $msgId2" `
        -Predicate { param($v) [int]$v -ge 1 } -TimeoutSeconds 60 `
        -Description 'the delivery accepted during the outage did not reach SENT' | Out-Null
    Write-Step-Pass 'the delivery accepted during the outage reached SENT'

    Wait-SqlValue "SELECT state FROM messages WHERE id = $msgId2" '2' 30 `
        'the message accepted during the outage did not reach COMPLETED' | Out-Null
    Write-Step-Pass 'the message accepted during the outage reached COMPLETED'

    $left = Invoke-Sql 'SELECT count(*) FROM outbox WHERE state <> 1'
    Assert-True ($left -eq '0') "no outbox row is left unpublished (got $left)"
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
