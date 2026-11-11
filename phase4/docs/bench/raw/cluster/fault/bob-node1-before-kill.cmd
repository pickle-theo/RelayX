@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5001 -user bob-1111143114-a -device d1 -to alice -count 2 -interval 200ms > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\fault\bob-node1-before-kill.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\fault\bob-node1-before-kill.err.log"
exit /b %ERRORLEVEL%
