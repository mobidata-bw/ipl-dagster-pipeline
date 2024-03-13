import gzip
import os
import shutil
import tempfile
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth

user_agent = 'IPL (MobiData-BW) +https://github.com/mobidata-bw/ipl-dagster-pipeline'


def get(url: str, headers: dict[str, str] | None = None, timeout=15, **kwargs):
    if headers is None:
        headers = {}
    headers['User-Agent'] = user_agent

    return requests.get(url, headers=headers, timeout=timeout, **kwargs)


def download(
    source: str,
    destination_path: str,
    filename: str,
    create_precompressed: bool = False,
    auth: tuple[str, str] | None = None,
    force: bool = False,
    timeout=15,
) -> None:
    """
    Downloads given source and, after completion, moves it to destination
    path with the specified name. Pre-existings files of the same name will
    be overwritten.
    In case create_precompressed is specified, a gzipped version of the
    file is created in the same path.

    Note: timestamps of remote files are not retained.
    """

    final_filename = Path(destination_path) / filename
    tmp_filename = final_filename.parent / (final_filename.name + '.tmp')

    if not final_filename.parent.exists():
        final_filename.parent.mkdir(parents=True)

    headers = {'User-Agent': user_agent}
    if not force and final_filename.exists():
        pre_existing_file_last_modified = datetime.utcfromtimestamp(final_filename.stat().st_mtime)
        headers['If-Modified-Since'] = pre_existing_file_last_modified.strftime('%a, %d %b %Y %H:%M:%S UTC')

    response = get(
        source,
        timeout=timeout,
        headers=headers,
        stream=True,
        auth=auth,
    )
    if response.status_code == 304:
        # File not modified since last download
        return

    with tmp_filename.open('wb') as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)

    last_modified = None
    last_modified_str = response.headers.get('Last-Modified')
    if last_modified_str:
        last_modified = parsedate_to_datetime(last_modified_str)
        os.utime(tmp_filename, (last_modified.timestamp(), last_modified.timestamp()))

    if gzip:
        tmp_gzip_filename = tmp_filename.parent / (tmp_filename.name + '.gz')
        final_gzip_filename = final_filename.parent / (final_filename.name + '.gz')

        with tmp_filename.open('rb') as f_in, gzip.open(tmp_gzip_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
            if last_modified:
                os.utime(tmp_gzip_filename, (last_modified.timestamp(), last_modified.timestamp()))
            tmp_gzip_filename.replace(final_gzip_filename)

    tmp_filename.replace(final_filename)
