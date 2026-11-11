@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5001 -user bob-1111143134 -device d1 -to alice -count 2 -interval 4s > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\redis-outage\bob-node1-outage.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\redis-outage\bob-node1-outage.err.log"
exit /b %ERRORLEVEL%
