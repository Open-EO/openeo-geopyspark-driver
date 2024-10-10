@ECHO OFF
SET parent_folder=%~dp1

::Remove trailing backslash:
IF %parent_folder:~-1%==\ SET parent_folder=%parent_folder:~0,-1%

IF NOT "%2"=="" (
    :: check if path exists:
    IF NOT EXIST "%2" (
        ECHO Folder not found: %2
        EXIT /B 1
    )
    ECHO Mounting user chosen path not supported in Windows!
)

:: --entrypoint /bin/bash
:: /etc/passwd to avoid "whoami: cannot find name for user ID"
:: Opening a /vsi file with the netCDF driver requires Linux userfaultfd to be available. If running from Docker, --security-opt seccomp=unconfined might be needed.
:: mount is used to read process_graph and write results
:: Avoid -i, to avoid "the input device is not a TTY"
:: --network host can fix internet connection when the host machine is behind a VPN
:: --rm to remove the container after it finishes. Remove all stopped containers with `docker container prune -f && docker ps -a`
docker run -t -v /etc/passwd:/etc/passwd:ro --security-opt seccomp=unconfined -v "%parent_folder%":/opt/docker_mount --network host --rm openeo_docker_local
echo Output written to: %parent_folder%
