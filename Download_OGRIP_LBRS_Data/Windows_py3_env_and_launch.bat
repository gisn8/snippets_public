@echo off

REM Python OSGeo Environment and Program Launcher for Windows

REM Or wherever OSGeo4W is installed
SET ROOT=C:\OSGeo4W64

REM Change the PROGNAME variable to %1 if you want to use this to make this the default program to open all Python scripts in Windows.
REM As is, this script works when this and the target file share the folder, otherwise replace with the full file path ("C:\...\file.py").
SET PROGNAME="download_ogrip_LBRS_layers.py"

if exist CALL "%ROOT%\bin\o4w_env.bat" (
	CALL "%ROOT%\bin\o4w_env.bat"
	REM echo o4w env set
	REM PAUSE
)

if exist "%ROOT%\bin\py3_env.bat" (
	CALL "%ROOT%\bin\py3_env.bat"
	REM echo py3_env set
	REM PAUSE
)


if exist "%ROOT%\bin\qt5_env.bat" (
	CALL "%ROOT%\bin\qt5_env.bat"
	REM echo qt5_env set
	REM PAUSE
)

REM Just covering the Python bases for a while.
if exist %ROOT%\apps\Python36 (
	SET PYTHONPATH=%PYTHONPATH%;%ROOT%\apps\Python36
	REM echo Environments set
	REM PAUSE
)

if exist %ROOT%\apps\Python37 (
	SET PYTHONPATH=%PYTHONPATH%;%ROOT%\apps\Python37
	REM echo Environments set
	REM PAUSE
)

if exist %ROOT%\apps\Python38 (
	SET PYTHONPATH=%PYTHONPATH%;%ROOT%\apps\Python38
	REM echo Environments set
	REM PAUSE
)

if exist %ROOT%\apps\Python39 (
	SET PYTHONPATH=%PYTHONPATH%;%ROOT%\apps\Python39
	REM echo Environments set
	REM PAUSE
)

REM To use QGIS processing tools within python script if needed
if exist %ROOT%\apps\qgis\python (
	SET PYTHONPATH=%PYTHONPATH%;%ROOT%\apps\qgis\python
)

REM Find .py (with parent cmd window) or .pyw (windowless) if UI present and if no parent desired. 
REM Mileage may vary with pyw on Windows 10, but shortcuts provide option to at least minimize parent window.
if exist "%PROGNAME%w" (
    call python3 "%PROGNAME%w" %*
) else (
    call python3 "%PROGNAME%" %*
)
