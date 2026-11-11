@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5001 -user alice -device d1 -count 1 -interval 1s > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\redis-outage\alice-endpoint-prep.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\redis-outage\alice-endpoint-prep.err.log"
exit /b %ERRORLEVEL%
