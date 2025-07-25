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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from subprocess import PIPE, Popen  # noqa: S404
from zoneinfo import ZoneInfo

from dagster import AssetExecutionContext, PipesSubprocessClient
from jinja2 import Environment, PackageLoader, Template, select_autoescape


class CommandExecutionFailed(RuntimeError): ...


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


class RequirementsFailedException(Exception): ...


class WebcamWorker:
    config: WebcamWorkerConfig
    pipes_subprocess_client: PipesSubprocessClient
    context: AssetExecutionContext
    index_template: Template

    def __init__(self, config: WebcamWorkerConfig, context: AssetExecutionContext):
        self.config = config
        self.context = context

        jinja2_env = Environment(loader=PackageLoader(package_name='pipeline'), autoescape=select_autoescape())
        self.index_template = jinja2_env.get_template('webcam_index.html.j2')

    def run(self):
        # Check for directories which should never be in our paths
        for base_bath in [self.config.image_path, self.config.symlink_path]:
            for path in ['etc', '.ssh', '.bashrc']:
                full_path = Path(base_bath, path)
                if full_path.exists():
                    raise RequirementsFailedException(
                        f'Path {full_path} should not exist in {base_bath}. Did you mount the wrong path?',
                    )

        # TODO: splitting up in incremental and full download
        self.download()

        # We always symlink, as we need to publish the latest images
        symlink_items = self.symlink()
        self.generate_index_page(symlink_items)

        # We just clean at full syncs
        self.clean()

    def download(self):
        result, stderr = self._run_command(
            command=[
                'unbuffer',
                'lftp',
                '-e',
                f'mirror --newer-than=now-{self.config.keep_days - 1}days -c --parallel={self.config.worker_count} {self.config.remote_dir} {self.config.image_path}; quit;"',
                '-u',
                f'{self.config.user},{self.config.password}',
                self.config.host,
            ],
        )
        total_match = re.search(r'Total: (\d+) director(y|ies), (\d+) files?, (\d+) symlinks?', result)

        if total_match is None:
            self.context.log.error(f'Could not parse lftp output: {result} / {stderr}')
            return

        total_directories, _, total_files, total_symlinks = total_match.groups()
        new_match = re.search(r'New: (\d+) files?, (\d+) symlinks?', result)

        # TODO: this would be perfect for metrics
        metadata = {'total_directories': total_directories, 'total_files': total_files}
        if new_match:
            new_files, new_symlinks = new_match.groups()
            metadata['new_files'] = new_files
        self.context.add_output_metadata(metadata)

    def generate_index_page(self, symlink_items: list[SymlinkItem]):
        index_page = self.index_template.render(symlink_items=symlink_items)
        self.config.symlink_path.mkdir(parents=True, exist_ok=True)
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
        temp_symlink_path = Path(self.config.symlink_path, f'temp-{symlink_item.filename}')
        image_path = symlink_item.path.relative_to(self.config.symlink_path, walk_up=True)

        # Atomic symlinking by overwriting the old symlink
        temp_symlink_path.symlink_to(image_path)
        os.rename(temp_symlink_path, symlink_path)

        return symlink_item

    def get_latest_image_path_per_webcam_base_path(self, webcam_base_path: Path) -> SymlinkItem | None:
        # We add 4 hours to make sure that we really get the latest image. One is subtracted at the start of the loop,
        # the remaining 3 are enough for any issue with UTC / local time / daylight-saving time / issues
        # (which is max 2).
        check_datetime = datetime.now(tz=ZoneInfo('Europe/Berlin')) + timedelta(hours=4)
        while check_datetime > datetime.now(tz=ZoneInfo('Europe/Berlin')) - timedelta(days=self.config.keep_days):
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

                image_datetime = datetime.fromtimestamp(image_path.stat().st_mtime, tz=timezone.utc)
                image_datetime = image_datetime.astimezone(tz=ZoneInfo('Europe/Berlin'))

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
                '-regextype',
                'sed',
                '-regex',
                '.*m[0-9]\\{15\\}\\.jpg',
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
        process = Popen(command, stdout=PIPE, stderr=PIPE, text=True)  # noqa: S603
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise CommandExecutionFailed(f'Subprocess {command} was not successful: {stderr}')

        return stdout, stderr
