@ECHO off
set PYTHONPATH=%PYTHONPATH%;.

python CryoCloud\Tools\ccworkflow.py %* --standalone
