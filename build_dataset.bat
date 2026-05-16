@echo off
setlocal
set HERE=%~dp0
set LOG=%HERE%build_dataset.log

echo === Running build_dataset.py === > "%LOG%"
echo This takes 3-5 minutes for full Nifty 500... >> "%LOG%"
echo. >> "%LOG%"
cd /d "%HERE%"
python build_dataset.py >> "%LOG%" 2>&1

echo. >> "%LOG%"
echo === DONE === >> "%LOG%"
endlocal
