#!/usr/bin/env sh

# matches path in DelayedVector
find /data/projects/OpenEO -maxdepth 1 -type d -name 'download_*' -mmin +60 -printf '%p%n' -exec rm -r {} \;
