import traceback
from typing import List, Optional, Union
from urllib.parse import urljoin

import geopandas as gpd
import pandas as pd
import requests
from geopandas.array import ExtensionDtype, GeometryDtype
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection

from pipeline.util.urllib import get

# https://pandas.pydata.org/docs/user_guide/basics.html#basics-dtypes
STATION_COLUMNS = {
    'feed_id': pd.StringDtype(),
    'station_id': pd.StringDtype(),
    'name': pd.StringDtype(),
    'rental_uris_android': pd.StringDtype(),
    'rental_uris_ios': pd.StringDtype(),
    'rental_uris_web': pd.StringDtype(),
    'last_reported': pd.DatetimeTZDtype(tz='UTC'),
    'geometry': GeometryDtype(),
}

VEHICLE_COLUMNS = {
    'feed_id': pd.StringDtype(),
    'vehicle_id': pd.StringDtype(),
    'form_factor': pd.StringDtype(),
    'name': pd.StringDtype(),
    'is_reserved': pd.BooleanDtype(),
    'propulsion_type': pd.StringDtype(),
    'max_range_meters': pd.Int32Dtype(),
    'rental_uris_android': pd.StringDtype(),
    'rental_uris_ios': pd.StringDtype(),
    'rental_uris_web': pd.StringDtype(),
    'last_reported': pd.DatetimeTZDtype(tz='UTC'),
    'geometry': GeometryDtype(),
}

STATION_BY_FORM_FACTOR_COLUMNS = {
    # 'station_id', is already part of index, don't add as column
    'num_bicycles_available': pd.Int32Dtype(),
    'num_cargo_bicycles_available': pd.Int32Dtype(),
    'num_cars_available': pd.Int32Dtype(),
    'num_scooters_seated_available': pd.Int32Dtype(),
    'num_scooters_standing_available': pd.Int32Dtype(),
    'num_mopeds_available': pd.Int32Dtype(),
    'num_others_available': pd.Int32Dtype(),
    'last_reported': pd.DatetimeTZDtype(tz='UTC'),
}


class Lamassu:
    # Feed ids of feeds, whose scooters are scooter_seated.
    scooter_seated_feeds: List[str] = []
    lamassu_base_url: str
    # Timeout used for lamassu requests
    timeout: int = 2

    def __init__(self, lamassu_base_url: str, scooter_seated_feeds: Optional[List[str]] = None):
        self.lamassu_base_url = lamassu_base_url
        self.scooter_seated_feeds = scooter_seated_feeds if scooter_seated_feeds else []

    def get_systems(self) -> dict:
        url = urljoin(self.lamassu_base_url, 'gbfs-internal')
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()['systems']

    def get_system_feeds(self, system_id: str, preferred_feed_languages: list) -> dict:
        url = urljoin(self.lamassu_base_url, f'gbfs-internal/{system_id}/gbfs.json')
        resp = get(url, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()['data']
        for lang in preferred_feed_languages:
            if lang in data:
                feeds = data[lang]['feeds']
                return {x['name']: x['url'] for x in feeds}

        return {}

    def _group_and_pivot(self, dataframe, index, columns, values):
        # unpack index/columns if they are lists, not just single column names
        group_cols = [
            *(index if isinstance(index, list) else [index]),
            *(columns if isinstance(columns, list) else [columns]),
        ]
        grouped_sums_df = dataframe.groupby(group_cols).sum(numeric_only=True).reset_index()
        # convert int64 to Int32, which is sufficiently large and can represent NA,
        # which avoids they are converted to float by pivot
        for column_name in grouped_sums_df.columns:
            if grouped_sums_df[column_name].dtype.name == 'int64':
                grouped_sums_df[column_name] = grouped_sums_df[column_name].astype('Int32')
        return grouped_sums_df.pivot(index=index, columns=columns, values=values)

    def _availability_col_name_for_form_factor(self, feed_id: str, form_factor: str) -> str:
        """
        Maps form_factor to corresponding `num_<form_factors>_available` column names.
        Note: as GBFSv2.3 still supports scooter, which can be scooter_seated or scooter_standing,
        we map per default to scooter_standing. If a feed / provider's vehicles are scooter_seated,
        these should be explicitly defined in scooter_seated_feeds.
        """
        if 'scooter_' in form_factor:
            return 'num_scooters_' + form_factor[len('scooter_') :] + '_available'
        if 'scooter' == form_factor:
            # Note: for now most scooter vehicle_types correspond to scooter_standing.
            if feed_id in self.scooter_seated_feeds:
                return 'num_scooters_seating_available'
            return 'num_scooters_standing_available'
        return 'num_' + form_factor + 's_available'
    

    def get_station_status_by_form_factor_as_frame(self, feed: dict, feed_id: str) -> Optional[pd.DataFrame]:
        if 'station_status' not in feed or 'vehicle_types' not in feed:
            return None

        stations_status_df = self._load_feed_as_frame(
            feed['station_status'],
            'stations',
            'vehicle_types_available',
            ['station_id', 'num_bikes_available', 'is_renting', 'is_installed'],
            [],
        )
        vehicle_types_df = self._load_feed_as_frame(feed['vehicle_types'], 'vehicle_types')

        if vehicle_types_df.empty or stations_status_df.empty:
            return None

        # merge station_status and vehicle_type, so we know form_factor for vehicle_types_available
        merged = pd.merge(stations_status_df, vehicle_types_df, on=['vehicle_type_id'])
        # filter those not installed or not renting
        filtered = merged.loc[(merged['is_renting'] == True) & (merged['is_installed'] == True)]  # noqa: E712
        # rename num_bikes_available to upcoming GBFS3 num_vehicles_available
        filtered = filtered.rename(columns={'num_bikes_available': 'num_vehicles_available'})
        # convert stations_status into a dataframe, one row per station, a column per form_factor
        # reflecting the number of available vehicles of this form_factor
        stations_availabilities_by_form_factor_df = self._group_and_pivot(
            filtered, ['station_id', 'num_vehicles_available'], 'form_factor', 'count'
        )
        # rename form_factor cols to num_<form_factor>s_available
        renamings = {
            c: self._availability_col_name_for_form_factor(feed_id, c)
            for c in stations_availabilities_by_form_factor_df.columns
        }
        stations_availabilities_by_form_factor_df = stations_availabilities_by_form_factor_df.rename(columns=renamings)
        # add feed name
        stations_availabilities_by_form_factor_df['feed_id'] = feed_id

        return self._enforce_columns(stations_availabilities_by_form_factor_df, STATION_BY_FORM_FACTOR_COLUMNS)

    def get_stations_as_frame(self, feed: dict, feed_id: str) -> Optional[pd.DataFrame]:
        if 'station_information' not in feed:
            return None

        stations_infos_df = self._load_feed_as_frame(feed['station_information'], 'stations')

        if stations_infos_df.empty:
            return None

        # add feed name to do delete insert
        stations_infos_df['feed_id'] = feed_id

        # Add geometry
        stations_infos_df_with_geom = gpd.GeoDataFrame(
            stations_infos_df,
            geometry=gpd.points_from_xy(stations_infos_df.lon, stations_infos_df.lat),
            crs='EPSG:4326',
        )

        return self._enforce_columns(stations_infos_df_with_geom, STATION_COLUMNS)

        # TODO add operator
        # TODO schema sharing

    def get_vehicles_as_frame(self, feed: dict, feed_id: str) -> Optional[pd.DataFrame]:
        """
        Extracts (free floating) vehicles from gbfs feeds, joins them with vehicle_type info
        and updates the postgis table for this feed_id.
        Disabled and reserved vehicles are not returned.

        Note: the feed must contain free_bike_status and vehicle_types information.
        """
        # If feed does not provide free_bike_status, ignore
        if 'free_bike_status' not in feed or 'vehicle_types' not in feed:
            return None

        free_vehicle_status_df = self._load_feed_as_frame(feed['free_bike_status'], 'bikes')
        if free_vehicle_status_df.empty:
            return None
        cols_to_add = [col for col in ['lon', 'lat'] if col not in free_vehicle_status_df.columns]
        free_vehicle_status_df.loc[:, cols_to_add] = None
        free_vehicle_status_df = free_vehicle_status_df.rename(columns={'bike_id': 'vehicle_id'})

        vehicle_types_df = self._load_feed_as_frame(feed['vehicle_types'], 'vehicle_types')
        # Fix issues with duplicate vehicle_type_ids
        vehicle_types_df = vehicle_types_df.drop_duplicates(subset=['vehicle_type_id'], keep='last')

        # Join vehicles and their type informatoin
        merged = pd.merge(free_vehicle_status_df, vehicle_types_df, on=['vehicle_type_id'])
        merged['feed_id'] = feed_id
        # filter those already reserved or disabled
        # Note: 'is False' results in boolean label can not be used without a boolean index
        filtered = merged.loc[
            merged.lon.notnull() & (merged['is_reserved'] == False) & (merged['is_disabled'] == False) # noqa: E712
        ]

        # Add geometry
        filtered_with_geom = gpd.GeoDataFrame(
            filtered, geometry=gpd.points_from_xy(filtered.lon, filtered.lat), crs='EPSG:4326'
        )

        return self._enforce_columns(filtered_with_geom, VEHICLE_COLUMNS)

    def _enforce_columns(self, df: pd.DataFrame, column_names: dict[str, ExtensionDtype]) -> pd.DataFrame:
        """
        Make sure all intended columns exist in data frame.
        Unwanted colums are discarded. Intended, but not yet
        existing are created with value "None".
        """
        # Create missing columns with their appropriate dtype
        for column, dtype in column_names.items():
            if column not in df:
                df[column] = pd.Series(dtype=dtype) if dtype else None

        # restrict to essentiel columns or provide defaults
        return df[column_names.keys()]

    def _load_feed_as_frame(
        self,
        url: str,
        element: Optional[str] = None,
        record_path: Union[str, List[str], None] = None,
        meta: Union[str, List[Union[str, List[str]]], None] = None,
        default_record_path=None,
    ):
        """
        Loads a specific gbfs endpoint and returns the data node
        (or the data's <element> node) as a denormalized flat pandas frame.
        """
        resp = get(url, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()['data'][element] if element else resp.json()['data']
        if isinstance(record_path, str):
            for record in data:
                if record_path not in record:
                    record[record_path] = default_record_path
        return pd.json_normalize(data, record_path, meta, sep='_')
