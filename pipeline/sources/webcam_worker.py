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
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from subprocess import PIPE, Popen  # noqa: S404

from dagster import PipesSubprocessClient
from jinja2 import Environment, PackageLoader, Template, select_autoescape


@dataclass
class WebcamWorkerConfig:
    host: str
    user: str
    password: str
    image_path: Path
    symlink_path: Path
    keep_days: int
    worker_count: int
    check_empty_files: bool = True
    remote_dir: str = '/'


@dataclass
class SymlinkItem:
    path: Path
    name: str
    filename: str
    moment: datetime


class WebcamWorker:
    config: WebcamWorkerConfig
    pipes_subprocess_client: PipesSubprocessClient
    index_template: Template

    def __init__(self, config: WebcamWorkerConfig, pipes_subprocess_client: PipesSubprocessClient):
        self.config = config
        # TODO: how to get stdout from PipesSubprocessClient?
        self.pipes_subprocess_client = pipes_subprocess_client
        jinja2_env = Environment(loader=PackageLoader(package_name='pipeline'), autoescape=select_autoescape())
        self.index_template = jinja2_env.get_template('webcam_index.html.j2')

    def run(self):
        # TODO: splitting up in incremental and full download
        self.download()

        # We always symlink, as we need to publish the latest images
        self.symlink_with_index()

        # We just clean at full syncs
        self.clean()

    def download(self):
        result, _ = self._run_command(
            command=[
                'lftp',
                '-e',
                f'mirror --newer-than=now-{self.config.keep_days - 1}days -c --parallel={self.config.worker_count} {self.config.remote_dir} {self.config.image_path}; quit;"',
                '-u',
                f'{self.config.user},{self.config.password}',
                self.config.host,
            ],
        )
        match = re.match(r'Total: (\d*) directories?, (\d*) files?, (\d*) symlinks?', result)

        if match is None:
            raise RuntimeError(f'Could not parse lftp output: {result}')

        directories, files, symlinks = match.groups()

        # TODO: this would be perfect for metrics
        # TODO: how to return this to dagster?
        print(f'Downloaded {files} files, {directories} directories, {symlinks} symlinks')  # noqa: T201

    def symlink_with_index(self):
        symlink_items = self.symlink()
        index_page = self.index_template.render(symlink_items=symlink_items)
        index_path = Path(self.config.symlink_path, 'index.html')
        with open(index_path, 'w') as f:
            f.write(index_page)

    def symlink(self) -> list[SymlinkItem]:
        image_dir = self.config.image_path

        if not image_dir.exists():
            return []
        webcam_base_paths = list(image_dir.iterdir())
        items: list[SymlinkItem] = []
        for webcam_base_path in webcam_base_paths:
            symlink_item = self.symlink_per_webcam_base_path(webcam_base_path)

            if symlink_item is not None:
                items.append(symlink_item)

        items.sort(key=lambda x: x.name)
        return items

    def symlink_per_webcam_base_path(self, webcam_base_path: Path) -> SymlinkItem | None:
        symlink_item = self.get_latest_image_path_per_webcam_base_path(webcam_base_path)
        if symlink_item is None:
            return None

        self.config.symlink_path.mkdir(parents=True, exist_ok=True)
        symlink_path = Path(self.config.symlink_path, symlink_item.filename)
        symlink_path.unlink(missing_ok=True)
        os.symlink(symlink_item.path, symlink_path)

        return symlink_item

    def get_latest_image_path_per_webcam_base_path(self, webcam_base_path: Path) -> SymlinkItem | None:
        # We add 4 hours to make sure that we really get the latest image.
        check_datetime = datetime.now() + timedelta(hours=4)
        while check_datetime > datetime.now() - timedelta(days=self.config.keep_days):
            # Jump one hour back at each iteration
            check_datetime = check_datetime - timedelta(hours=1)

            image_base_path = Path(
                webcam_base_path,
                f'{check_datetime.year:04d}',
                f'{check_datetime.month:02d}',
                f'{check_datetime.day:02d}',
                f'{check_datetime.hour:02d}',
            )
            # If the base path does not exist, we can be sure that there are no images
            if not image_base_path.exists():
                continue

            latest_image_path: Path | None = None
            latest_image_datetime: datetime | None = None
            for image_path in image_base_path.iterdir():
                # We just want files
                if not image_path.is_file():
                    continue

                image_name = image_path.name
                # We just want images
                if not image_name.endswith('.jpg'):
                    continue

                # We ignore empty files
                if self.config.check_empty_files and image_path.stat().st_size == 0:
                    continue
                try:
                    image_datetime = datetime.strptime(image_name[1:-5], '%Y%m%d%H%M%S')
                except ValueError:
                    # If we have a value error, the file name is invalid, so we continue
                    continue

                if latest_image_datetime is None or image_datetime > latest_image_datetime:
                    latest_image_datetime = image_datetime
                    latest_image_path = image_path

            # If we found an image: break, so we can set the symlink
            if latest_image_path is not None and latest_image_datetime is not None:
                return SymlinkItem(
                    path=latest_image_path,
                    name=webcam_base_path.name,
                    filename=f'{webcam_base_path.name}.jpg',
                    moment=latest_image_datetime,
                )

        # If we did not find an image, we return None
        return None

    def clean(self):
        # Delete all old webcam images
        self._run_command(
            command=[
                'find',
                str(self.config.image_path),
                '-mtime',
                f'+{self.config.keep_days}',
                '-type',
                'f',
                '-delete',
            ],
        )
        # Delete all empty directories
        self._run_command(
            command=[
                'find',
                str(self.config.image_path),
                '-type',
                'd',
                '-empty',
                '-delete',
            ],
        )

    @staticmethod
    def _run_command(command: list[str]) -> tuple[str, str]:
        process = Popen(command, stdout=PIPE, stderr=PIPE)  # noqa: S603
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise RuntimeError(f'Subprocess {command} was not successful: {stderr.decode()}')

        return stdout.decode(), stderr.decode()
