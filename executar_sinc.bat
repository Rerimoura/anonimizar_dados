@echo off
:: ============================================================
::  SYNC VENDAS - Task Scheduler
::  Configura no Task Scheduler apontando para este .bat
:: ============================================================

:: Ajuste os caminhos abaixo para o seu ambiente
set PYTHON=C:\Users\User\AppData\Local\Programs\Python\Python313\python.exe
set SCRIPT=C:\Users\User\OneDrive - ALIANCA ATACADISTA LTDA (3)\Cursos\ASIMOV\Projetos pessoais\faker\sync_vendas.py
set LOG=C:\Users\User\OneDrive - ALIANCA ATACADISTA LTDA (3)\Cursos\ASIMOV\Projetos pessoais\faker\sync_log.txt

echo ============================================================ >> %LOG%
echo Executado em: %DATE% %TIME% >> %LOG%
echo ============================================================ >> %LOG%

%PYTHON% %SCRIPT% >> %LOG% 2>&1

echo Finalizado em: %DATE% %TIME% >> %LOG%
echo. >> %LOG%