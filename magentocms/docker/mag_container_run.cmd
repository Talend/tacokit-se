@ECHO OFF
rem starting this script example
rem mag_container_run.cmd --docker-host 192.168.99.100 --magento-port 32567 --magento-port-secure 32568 --magento-use-secure 0 --magento-use-secure-admin 0

rem set DOCKER_MACHINE_HOSTNAME=localhost
set DOCKER_MACHINE_HOSTNAME=localhost
set MAGENTO_PORT=80
set MAGENTO_PORT_SECURE=443
set MAGENTO_USE_SECURE=0
set MAGENTO_USE_SECURE_ADMIN=0

:loop
IF NOT "%1"=="" (
    IF "%1"=="--docker-host" (
        SET DOCKER_MACHINE_HOSTNAME=%2
        SHIFT
    )
    IF "%1"=="--magento-port" (
        SET MAGENTO_PORT=%2
        SHIFT
    )
    IF "%1"=="--magento-port-secure" (
        SET MAGENTO_PORT_SECURE=%2
        SHIFT
    )
    IF "%1"=="--magento-use-secure" (
        SET MAGENTO_USE_SECURE=%2
        SHIFT
    )
    IF "%1"=="--magento-use-secure-admin" (
        SET MAGENTO_USE_SECURE_ADMIN=%2
        SHIFT
    )
    SHIFT
    GOTO :loop
)

if "%MAGENTO_PORT%" == "80" (
  set MAGENTO_BASE_URL=http://%DOCKER_MACHINE_HOSTNAME%
) else (
  set MAGENTO_BASE_URL=http://%DOCKER_MACHINE_HOSTNAME%:%MAGENTO_PORT%
) 
if "%MAGENTO_PORT%" == "443" (
  set MAGENTO_BASE_URL_SECURE=https://%DOCKER_MACHINE_HOSTNAME%
) else (
  set MAGENTO_BASE_URL_SECURE=https://%DOCKER_MACHINE_HOSTNAME%:%MAGENTO_PORT_SECURE%
) 

echo Start magento docker container with parameters:
REM ECHO DOCKER_MACHINE_HOSTNAME = %DOCKER_MACHINE_HOSTNAME%
echo MAGENTO_BASE_URL = %MAGENTO_BASE_URL%
echo MAGENTO_BASE_URL_SECURE = %MAGENTO_BASE_URL_SECURE%
ECHO MAGENTO_PORT = %MAGENTO_PORT%
ECHO MAGENTO_PORT_SECURE = %MAGENTO_PORT_SECURE%
ECHO MAGENTO_USE_SECURE = %MAGENTO_USE_SECURE%
ECHO MAGENTO_USE_SECURE_ADMIN = %MAGENTO_USE_SECURE_ADMIN%


rem run docker container, pass environment variables to container's supervisor
REM docker run --rm -d -p %MAGENTO_PORT%:80 -p %MAGENTO_PORT_SECURE%:443 -e DOCKER_MACHINE_HOSTNAME=%DOCKER_MACHINE_HOSTNAME% -e MAGENTO_PORT=%MAGENTO_PORT% -e MAGENTO_PORT_SECURE=%MAGENTO_PORT_SECURE% -e MAGENTO_USE_SECURE=%MAGENTO_USE_SECURE% -e MAGENTO_USE_SECURE_ADMIN=%MAGENTO_USE_SECURE_ADMIN% components-integration-test-magentocms:1.0.0
docker run --rm -d -p %MAGENTO_PORT%:80 -p %MAGENTO_PORT_SECURE%:443 -e MAGENTO_BASE_URL=%MAGENTO_BASE_URL% -e MAGENTO_BASE_URL_SECURE=%MAGENTO_BASE_URL_SECURE% -e MAGENTO_USE_SECURE=%MAGENTO_USE_SECURE% -e MAGENTO_USE_SECURE_ADMIN=%MAGENTO_USE_SECURE_ADMIN% registry.datapwn.com/sbovsunovskyi/components-integration-test-magentocms:1.0.0

