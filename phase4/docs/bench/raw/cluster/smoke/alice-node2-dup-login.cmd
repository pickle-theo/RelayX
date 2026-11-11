@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5002 -user alice -device d1 -to bob-1111143055 -count 1 -interval 100ms > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\alice-node2-dup-login.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\alice-node2-dup-login.err.log"
exit /b %ERRORLEVEL%
