@echo off
"C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\bin\client.exe" -addr 127.0.0.1:5003,127.0.0.1:5001,127.0.0.1:5002 -user alice -device d1 > "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\fault\alice-failover.out.log" 2> "C:\Users\Administrator\Documents\trae_projects\RelayX\phase4\docs\bench\raw\cluster\fault\alice-failover.err.log"
exit /b %ERRORLEVEL%
