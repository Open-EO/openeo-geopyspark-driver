@ECHO OFF
SET parent_folder=%~dp1

::Remove trailing backslash:
IF %parent_folder:~-1%==\ SET parent_folder=%parent_folder:~0,-1%

:: --entrypoint /bin/bash
:: /etc/passwd to avoid "whoami: cannot find name for user ID"
:: Opening a /vsi file with the netCDF driver requires Linux userfaultfd to be available. If running from Docker, --security-opt seccomp=unconfined might be needed.
:: mount is used to read process_graph and write results
:: Avoid -i, to avoid "the input device is not a TTY"
:: --network host can fix internet connection when the host machine is behind a VPN
docker run -t -v /etc/passwd:/etc/passwd:ro --security-opt seccomp=unconfined -v "%parent_folder%":/opt/docker_mount --network host openeo_docker_local
echo Output written to: %parent_folder%
