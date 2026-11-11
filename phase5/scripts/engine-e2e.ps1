# Phase 5 engine end-to-end verification:
#   HTTP acceptance -> transactional outbox -> Kafka fan-out -> delivery rows
#   -> lease-based push -> receipt/business ACK -> message COMPLETED.
#
# Usage (from phase5/):
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts/engine-e2e.ps1
#
# Evidence is written to docs/bench/raw/engine/e2e/ (overwritten per run).

[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'engine-common.ps1')

$exitCode = 0
try {
    Initialize-EngineRun 'e2e'
    Add-Param 'gateway: 127.0.0.1:9000 node-a node-addr 127.0.0.1:9100 redis 127.0.0.1:6379 kafka 127.0.0.1:9092'
    Add-Param 'message-service: 127.0.0.1:8081'
    Add-Param 'delivery-worker: worker-id w1 lease-ttl 5s scan-interval 200ms'
    Add-Param 'recipient: client bob/phone (-ack both, -pull), sender: HTTP POST /v1/messages from alice/web'
    Add-Param 'kafka topics: relayx.fanout.v1 / relayx.deliver.v1 (3 partitions, recreated per run)'

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

    Assert-PortsFree @(9000, 9100, 8081)
    Write-Step-Pass 'ports 9000, 9100 and 8081 are free'

    $gateway = Start-GatewayProcess -Name 'gateway' -Addr '127.0.0.1:9000' -NodeID 'node-a' -NodeAddr '127.0.0.1:9100'
    $svc = Start-MessageServiceProcess -Name 'message-service' -HttpAddr '127.0.0.1:8081'
    $worker = Start-WorkerProcess -Name 'worker-w1' -WorkerID 'w1' -LeaseTTL '5s' -ScanInterval '200ms'

    Wait-TcpOpen @('127.0.0.1:9000', '127.0.0.1:9100') 20
    Write-Step-Pass 'gateway TCP 9000 and node port 9100 are listening'
    Wait-HttpOK "$($script:MessageServiceUrl)/healthz" 20
    Write-Step-Pass 'message-service /healthz returns 200'
    Wait-LogMatch $worker 'delivery worker w1 started' 20 'delivery-worker w1 did not start' | Out-Null
    Write-Step-Pass 'delivery-worker w1 started'

    $bob = Start-ClientProcess -Name 'client-bob-phone' -ClientArgs @(
        '-addr', '127.0.0.1:9000',
        '-name', 'bob', '-user', 'bob', '-device', 'phone',
        '-ack', 'both', '-pull', '-interval', '5s'
    )
    Wait-LogMatch $bob 'logged in as bob/phone' 20 'bob/phone did not log in to the gateway' | Out-Null
    Wait-SqlValue "SELECT count(*) FROM endpoints WHERE user_id = 'bob' AND device_id = 'phone'" '1' 10 `
        'bob/phone endpoint was not recorded by the login' | Out-Null
    Write-Step-Pass 'bob/phone logged in and its endpoint row exists'

    $tAccepted = Get-Date
    $res = Invoke-AcceptMessage -TenantID 'default' -FromUser 'alice' -FromDevice 'web' `
        -Target 'bob' -TargetType 'user' -Payload 'phase5-e2e-payload' `
        -ClientMsgID 1001 -TTLSeconds 300
    Assert-True ($res.StatusCode -eq 202) "POST /v1/messages returned HTTP 202 (got $($res.StatusCode))"
    Assert-True ($res.MessageID -gt 0) "acceptance returned message_id $($res.MessageID) (> 0)"
    Assert-True ($res.TenantID -eq 'default') "acceptance echoed tenant_id default (got '$($res.TenantID)')"
    Add-Param "message_id: $($res.MessageID), client_msg_id: 1001, tenant: default"

    $text = Wait-LogMatch $bob 'deliver from=alice/web message_id=([0-9]+)' 20 `
        'bob/phone did not log a DELIVER from alice/web'
    $deliverCount = Count-LogMatches $text 'deliver from=alice/web'
    Write-Step-Pass "client log shows $deliverCount DELIVER from alice/web to bob/phone"

    Wait-SqlValue 'SELECT count(*) FROM outbox' '1' 10 'outbox row was not written' | Out-Null
    Wait-SqlValue 'SELECT count(*) FROM outbox WHERE state = 1' '1' 15 `
        'outbox row was not published (state=1)' | Out-Null
    Write-Step-Pass 'outbox row published to Kafka (all rows state=1)'

    Wait-SqlValue "SELECT count(*) FROM deliveries WHERE message_id = $($res.MessageID)" '1' 15 `
        'fan-out did not create exactly one delivery for bob/phone' | Out-Null
    $dstate = Wait-SqlValue "SELECT state FROM deliveries WHERE message_id = $($res.MessageID)" '3' 20 `
        'delivery did not reach BUSINESS_ACKED (state=3)'
    Write-Step-Pass "delivery state=$dstate (BUSINESS_ACKED) after the two-level ACK"

    $mstate = Wait-SqlValue "SELECT state FROM messages WHERE id = $($res.MessageID)" '2' 15 `
        'message did not reach COMPLETED (state=2)'
    Write-Step-Pass "message state=$mstate (COMPLETED)"

    $elapsed = Get-Elapsed $tAccepted
    Write-Step-Pass "end-to-end acceptance -> completed took ${elapsed}s"

    $snapshot = Invoke-Sql "SELECT m.id, m.state, d.state, d.attempts, (o.published_at IS NOT NULL) FROM messages m JOIN deliveries d ON d.message_id = m.id JOIN outbox o ON true LIMIT 1"
    Write-Step-Info "row snapshot (message_id|message_state|delivery_state|attempts|outbox_published): $snapshot"
    $pending = Invoke-Sql 'SELECT count(*) FROM outbox WHERE state <> 1'
    Assert-True ($pending -eq '0') 'no outbox row is left unpublished'
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
