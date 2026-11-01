@echo off
echo ============================================================
echo   DistChat - Starting Server A (port 9001)
echo   Peer: 127.0.0.1:9002
echo ============================================================
python "%~dp0server\server.py" A 127.0.0.1 9001 127.0.0.1:9002
pause
