#!/bin/bash

set -eo pipefail
set -x

1>&2 echo 'Downloading webcam images'
# Delete all old webcam images
find /var/webcam -mtime +$IPL_WEBCAM_KEEP_DAYS -type f -delete
# Delete all empty directories
find /var/webcam  -type d -empty -delete
# Download all new files. Limit to now -(DOWNLOAD_MAX_DAYS - 1) days, otherwise we would delete and re-download data
# all the time.
DOWNLOAD_MAX_DAYS=$((IPL_WEBCAM_KEEP_DAYS-1))
lftp -e "mirror --newer-than=now-${DOWNLOAD_MAX_DAYS}days -c --parallel=$IPL_WEBCAM_WORKER --verbose / /var/webcam; quit;" -u $IPL_WEBCAM_USER,$IPL_WEBCAM_PASSWORD $IPL_WEBCAM_SERVER
