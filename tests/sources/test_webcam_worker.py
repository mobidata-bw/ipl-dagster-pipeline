# Copyright 2025 Ernesto Ruge (ernesto.ruge@binary-butterfly.de)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Generator

import pytest
from dagster import AssetExecutionContext, PipesSubprocessClient
from freezegun import freeze_time

from pipeline.sources import WebcamWorker
from pipeline.sources.webcam_worker import WebcamWorkerConfig


@pytest.fixture
def ftp_path() -> Generator[Path, None, None]:
    tmp_dir = Path('/ftp/user')
    for tmp_sub_dir in tmp_dir.iterdir():
        shutil.rmtree(tmp_sub_dir)

    yield tmp_dir

    for tmp_sub_dir in tmp_dir.iterdir():
        shutil.rmtree(tmp_sub_dir)


@pytest.fixture
def image_path() -> Generator[Path, None, None]:
    tmp_dir = Path('/tmp/webcam_test_data')
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir()

    yield tmp_dir

    shutil.rmtree(tmp_dir)


@pytest.fixture
def symlink_path() -> Generator[Path, None, None]:
    tmp_dir = Path('/tmp/webcam_test_symlinks')
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir()

    yield tmp_dir

    shutil.rmtree(tmp_dir)


@pytest.fixture
def webcam_worker(
    image_path: Path,
    symlink_path: Path,
    stripped_pipes_subprocess_client: PipesSubprocessClient,
    mocked_asset_execution_context: AssetExecutionContext,
) -> WebcamWorker:
    return WebcamWorker(
        context=mocked_asset_execution_context,
        pipes_subprocess_client=stripped_pipes_subprocess_client,
        config=WebcamWorkerConfig(
            host='ftp',
            user='user',
            password='password',
            keep_days=10,
            worker_count=1,
            image_path=image_path,
            symlink_path=symlink_path,
            check_empty_files=False,
            remote_dir='.',  # FTP image does not chroot, so you would download the / file system
        ),
    )


def generate_image_path(image_path: Path, webcam_name: str, moment: datetime) -> Path:
    image_base_path = Path(
        image_path,
        webcam_name,
        f'{moment.year:04d}',
        f'{moment.month:02d}',
        f'{moment.day:02d}',
        f'{moment.hour:02d}',
    )
    image_base_path.mkdir(parents=True, exist_ok=True)
    image_path = Path(image_base_path, f'm{moment.strftime("%y%m%d%H%M%S%f")[:-3]}.jpg')
    image_path.touch()
    os.utime(image_path, (moment.timestamp(), moment.timestamp()))

    return image_path


def test_create_symlinks(image_path: Path, symlink_path: Path, webcam_worker: WebcamWorker):
    webcam_1_name = 'DEMO_WEBCAM_1'
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 10, 5, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 10, 4, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 10, 3, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 10, 2, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 10, 1, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 10, 0, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 9, 59, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 9, 58, 10, 100))
    generate_image_path(image_path, webcam_1_name, datetime(2025, 5, 10, 9, 57, 10, 100))

    webcam_2_name = 'DEMO_WEBCAM_2'
    generate_image_path(image_path, webcam_2_name, datetime(2025, 5, 9, 20, 5, 10, 100))
    generate_image_path(image_path, webcam_2_name, datetime(2025, 5, 9, 20, 4, 10, 100))

    with freeze_time(datetime(2025, 5, 10, 10, 10)):
        webcam_worker.symlink_with_index()

    symlinks = list(symlink_path.iterdir())
    assert len(symlinks) == 3
    symlinks.sort(key=lambda x: x.name)
    assert symlinks[0].name == 'DEMO_WEBCAM_1.jpg'
    assert symlinks[1].name == 'DEMO_WEBCAM_2.jpg'
    assert symlinks[2].name == 'index.html'


def test_download(image_path: Path, ftp_path: Path, webcam_worker: WebcamWorker):
    webcam_1_name = 'DEMO_WEBCAM_1'
    generate_image_path(ftp_path, webcam_1_name, datetime(2025, 5, 10, 10, 5, 10, 100))
    generate_image_path(ftp_path, webcam_1_name, datetime(2025, 5, 10, 9, 59, 10, 100))
    webcam_2_name = 'DEMO_WEBCAM_2'
    generate_image_path(ftp_path, webcam_2_name, datetime(2025, 5, 9, 20, 5, 10, 100))
    generate_image_path(ftp_path, webcam_2_name, datetime(2025, 5, 9, 20, 4, 10, 100))
    webcam_worker.download()
