@echo off
echo ============================================================
echo   DistChat - Starting Server B (port 9002)
echo   Peer: 127.0.0.1:9001
echo ============================================================
python "%~dp0server\server.py" B 127.0.0.1 9002 127.0.0.1:9001
pause
