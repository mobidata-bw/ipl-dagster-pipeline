from typing import Optional

from dagster import (
    ConfigurableResource,
)
from pandas import DataFrame

from pipeline.sources.lamassu import Lamassu


# need mypy to ignore following line due to https://github.com/dagster-io/dagster/issues/17443
class LamassuResource(ConfigurableResource):  # type: ignore
    """
    LamassuResource is a ConfigurableResource wrapper around
    a Lamassu instance
    """

    lamassu_base_url: str

    def _lamassu(self) -> Lamassu:
        return Lamassu(self.lamassu_base_url)

    def get_systems(self) -> dict:
        return self._lamassu().get_systems()

    def get_system_feeds(self, system_id: str) -> dict:
        return self._lamassu().get_system_feeds(system_id, ['de', 'en', 'fr'])

    def get_vehicles_as_frame(self, feed: dict, system_id: str) -> Optional[DataFrame]:
        return self._lamassu().get_vehicles_as_frame(feed, system_id)

    def get_stations_as_frame(self, feed: dict, system_id: str) -> Optional[DataFrame]:
        return self._lamassu().get_stations_as_frame(feed, system_id)
