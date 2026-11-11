@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5003 -user alice -device d1 > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\alice-node3.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\alice-node3.err.log"
exit /b %ERRORLEVEL%
