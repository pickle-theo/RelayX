@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5001 -user bob-1111143055 -device d1 -to alice -count 3 -interval 300ms > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\bob-node1-send.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\smoke\bob-node1-send.err.log"
exit /b %ERRORLEVEL%
