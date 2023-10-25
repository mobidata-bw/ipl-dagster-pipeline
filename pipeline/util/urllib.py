import gzip
import os
import shutil
import tempfile
from urllib.request import urlretrieve


def download(source: str, destination_path: str, filename: str, create_precompressed: bool = False) -> None:
    """
    Downloads given source and, after completion, moves it to destination
    path with the specified name. Pre-existings files of the same name will
    be overwritten.
    In case create_precompressed is specified, a gzipped version of the
    file is created in the same path.

    Note: timestamps of remote files are not retained.
    """
    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpfilename = os.path.join(tmpdirname, filename)
        finalfilename = os.path.join(destination_path, filename)

        if not os.path.exists(destination_path):
            os.makedirs(destination_path)

        if source.startswith(('http:', 'https:')):
            urlretrieve(source, tmpfilename)  # noqa: S310 nosec explicitly checked for http
        else:
            raise ValueError from None

        if gzip:
            with open(tmpfilename, 'rb') as f_in, gzip.open(tmpfilename + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                os.replace(tmpfilename + '.gz', finalfilename + '.gz')

        os.replace(tmpfilename, finalfilename)
