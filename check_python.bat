@echo off
setlocal
set LOG=%~dp0check.log

echo === Checking python install === > "%LOG%"
echo. >> "%LOG%"

echo --- python on PATH? --- >> "%LOG%"
where python >> "%LOG%" 2>&1
echo. >> "%LOG%"

echo --- py launcher? --- >> "%LOG%"
where py >> "%LOG%" 2>&1
echo. >> "%LOG%"

echo --- LocalAppData Python install? --- >> "%LOG%"
dir "%LocalAppData%\Programs\Python" >> "%LOG%" 2>&1
echo. >> "%LOG%"

echo --- Is installer still running? --- >> "%LOG%"
tasklist /FI "IMAGENAME eq python-3.14.5-amd64.exe" >> "%LOG%" 2>&1
tasklist /FI "IMAGENAME eq Python-3.14.5*" >> "%LOG%" 2>&1
echo. >> "%LOG%"

echo === DONE === >> "%LOG%"
endlocal
