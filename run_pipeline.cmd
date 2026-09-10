@echo off
REM ---------------------------------------------------------------------------
REM Run the whole notebook pipeline unattended, in dependency order.
REM
REM   run_pipeline.cmd                    the standard chain
REM   run_pipeline.cmd --list             show the stages and exit
REM   run_pipeline.cmd --dry-run          print the plan without running
REM   run_pipeline.cmd --include-fetch    add the hours-long API fetch first
REM   run_pipeline.cmd --only parquet ndjson
REM   run_pipeline.cmd --from benchmark   resume after a failure
REM
REM Every argument is passed straight through to notebooks/run_pipeline.py.
REM A transcript is appended to data\pipeline_run.log.
REM ---------------------------------------------------------------------------

setlocal

REM Run from the project root whatever directory this was invoked from, so
REM "docker compose" finds docker-compose.yml.
pushd "%~dp0"

REM The containers must be up; this is a no-op when they already are.
docker compose up -d
if errorlevel 1 (
    echo.
    echo Could not start the stack. Is Docker Desktop running?
    exit /b 1
)

REM run_pipeline.py waits for MongoDB itself before the first notebook runs.
docker exec -w /home/jovyan/work group13_jupyter python run_pipeline.py %*
set RC=%errorlevel%

popd

echo.
if %RC%==0 (
    echo Pipeline finished. Transcript: data\pipeline_run.log
) else (
    echo Pipeline FAILED ^(exit %RC%^). See data\pipeline_run.log and the notebook
    echo that stopped - its outputs hold the traceback.
)

exit /b %RC%
