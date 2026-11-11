@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5002 -user alice -device d1 > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\alice-node2-live.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\alice-node2-live.err.log"
exit /b %ERRORLEVEL%
