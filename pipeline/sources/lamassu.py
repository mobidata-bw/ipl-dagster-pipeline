import logging
from typing import Dict, List, Optional, Union
from urllib.parse import urljoin

import geopandas as gpd
import pandas as pd
import requests
from geopandas.array import ExtensionDtype, GeometryDtype

from pipeline.util.urllib import get

# https://pandas.pydata.org/docs/user_guide/basics.html#basics-dtypes
STATION_COLUMNS = {
    'feed_id': pd.StringDtype(),
    'station_id': pd.StringDtype(),
    'name': pd.StringDtype(),
    'capacity': pd.Int32Dtype(),
    'rental_uris_android': pd.StringDtype(),
    'rental_uris_ios': pd.StringDtype(),
    'rental_uris_web': pd.StringDtype(),
    'last_reported': pd.DatetimeTZDtype(tz='UTC'),
    'geometry': GeometryDtype(),
}

VEHICLE_COLUMNS = {
    'feed_id': pd.StringDtype(),
    'station_id': pd.StringDtype(),
    'vehicle_id': pd.StringDtype(),
    'form_factor': pd.StringDtype(),
    'name': pd.StringDtype(),
    'propulsion_type': pd.StringDtype(),
    'current_fuel_percent': pd.Float32Dtype(),
    'current_range_meters': pd.Float32Dtype(),
    'max_range_meters': pd.Float32Dtype(),
    'rental_uris_android': pd.StringDtype(),
    'rental_uris_ios': pd.StringDtype(),
    'rental_uris_web': pd.StringDtype(),
    'last_reported': pd.DatetimeTZDtype(tz='UTC'),
    'geometry': GeometryDtype(),
}

STATION_BY_FORM_FACTOR_COLUMNS = {
    'feed_id': pd.StringDtype(),
    'station_id': pd.StringDtype(),
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
    # Base URL to a lamassu instance.
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

    def _get_default_vehicle_type(self, vehicle_types_df: pd.DataFrame) -> list[dict]:
        """
        In case all given vehicle types are of the same form factor,
        this function will return a minimal available_vehicles array including
        an arbitrary vehicle_type (which will by it's form factor define the station's
        form_factor).
        """
        vt_grouped_by_form_factor = vehicle_types_df.groupby('form_factor')
        if len(vt_grouped_by_form_factor) == 1:
            # as all vehicle_types have the same form_factor,
            # we assign an arbitrary vehicle_type
            return [{'vehicle_type_id': vehicle_types_df.loc[0]['vehicle_type_id'], 'count': 0}]
        # no form_factor or not unique. In this case, we don't assign any vehicle_type as default
        return []

    def get_station_status_by_form_factor_as_frame(self, feed: dict, feed_id: str) -> Optional[pd.DataFrame]:
        if 'station_status' not in feed or 'vehicle_types' not in feed:
            return None

        vehicle_types_df = self._load_feed_as_frame(feed['vehicle_types'], 'vehicle_types')
        if vehicle_types_df.empty:
            return None

        # Load station_status and return it as dataframe, one row per vehicle_types_available.
        # If vehicle_type_available is empty for a station_status, we assume
        # default_vehicles_types_availability.

        stations_status_df = self._load_feed_as_frame(
            feed['station_status'],
            'stations',
            'vehicle_types_available',
            # Note: in gbfs 2.3, num_bikes_available means num_vehicles_available, will be renamed in v3
            ['station_id', 'num_bikes_available', 'is_renting', 'is_installed', 'last_reported'],
            self._get_default_vehicle_type(vehicle_types_df),
        )

        if stations_status_df.empty:
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
            filtered, ['station_id', 'num_vehicles_available', 'last_reported'], 'form_factor', 'count'
        )
        # rename form_factor cols to num_<form_factor>s_available
        renamings = {
            c: self._availability_col_name_for_form_factor(feed_id, c)
            for c in stations_availabilities_by_form_factor_df.columns
        }
        stations_availabilities_by_form_factor_df = stations_availabilities_by_form_factor_df.rename(columns=renamings)
        return self._postprocess_columns_and_types(
            stations_availabilities_by_form_factor_df, feed_id, STATION_BY_FORM_FACTOR_COLUMNS, 'station_id'
        )

    def get_stations_as_frame(self, feed: dict, feed_id: str) -> Optional[pd.DataFrame]:
        if 'station_information' not in feed:
            return None

        stations_infos_df = self._load_feed_as_frame(feed['station_information'], 'stations')

        if stations_infos_df.empty:
            return None

        # Add geometry
        stations_infos_df_with_geom = gpd.GeoDataFrame(
            stations_infos_df,
            geometry=gpd.points_from_xy(stations_infos_df.lon, stations_infos_df.lat),
            crs='EPSG:4326',
        )

        return self._postprocess_columns_and_types(stations_infos_df_with_geom, feed_id, STATION_COLUMNS, 'station_id')

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
        # filter those already reserved or disabled
        # Note: 'is False' results in boolean label can not be used without a boolean index
        filtered = merged.loc[
            merged.lon.notnull() & (merged['is_reserved'] == False) & (merged['is_disabled'] == False)  # noqa: E712
        ]

        # Add geometry
        filtered_with_geom = gpd.GeoDataFrame(
            filtered, geometry=gpd.points_from_xy(filtered.lon, filtered.lat), crs='EPSG:4326'
        )
        return self._postprocess_columns_and_types(filtered_with_geom, feed_id, VEHICLE_COLUMNS, 'vehicle_id')

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
                # if record has no record_path property, or it's an empty collection, we replace by the default
                if record_path not in record or (
                    isinstance(record[record_path], list) and len(record[record_path]) == 0
                ):
                    record[record_path] = default_record_path
        return pd.json_normalize(data, record_path, meta, sep='_')

    @staticmethod
    def _coerce_int64_to_int32(dataframe: pd.DataFrame) -> None:
        """
        convert int64 to Int32, which can represent NA, and is
        sufficiently large to represent expected values.
        """
        for column_name in dataframe.columns:
            if dataframe[column_name].dtype.name == 'int64':
                dataframe[column_name] = dataframe[column_name].astype('Int32')

    @staticmethod
    def _group_and_pivot(
        dataframe: pd.DataFrame, index: Union[list, str], columns: Union[list, str], values: Union[list, str]
    ) -> pd.DataFrame:
        """
        Groups the dataframe by index and columns, summing up the value columns and returning them pivoted.
        """
        # unpack index/columns if they are lists, not just single column names
        group_cols = [
            *(index if isinstance(index, list) else [index]),
            *(columns if isinstance(columns, list) else [columns]),
        ]
        grouped_sums_df = dataframe.groupby(group_cols).sum(numeric_only=True).reset_index()
        Lamassu._coerce_int64_to_int32(grouped_sums_df)
        return grouped_sums_df.pivot(index=index, columns=columns, values=values)

    @staticmethod
    def _postprocess_columns_and_types(
        df: pd.DataFrame, feed_id: str, enforced_columns: Dict[str, ExtensionDtype], index: str
    ) -> pd.DataFrame:
        """
        Performs some datafram post-processsing common to all feature frames:
        * adds the given feed_id as column
        * sets the given index column as new index
        * converts last_reported from epoch to datetime column
        * assures that the returned dataframe has excactly the enforced_columns. Columns not contained in enforced_columns
          will be dropped, not yet existing columns created with their aassigned type, existing coerced to the given type
        """
        df = df.reset_index()
        df['feed_id'] = feed_id
        # convert seconds since epoch into datetime, if available (for vehicles, it's optional)
        if 'last_reported' in df.columns:
            df['last_reported'] = pd.to_datetime(df['last_reported'], unit='s', utc=True, errors='coerce')
        df_with_enforced_columns = Lamassu._enforce_columns(df, enforced_columns)
        return df_with_enforced_columns.set_index(index)

    @staticmethod
    def _enforce_columns(df: pd.DataFrame, column_names: dict[str, ExtensionDtype]) -> pd.DataFrame:
        """
        Make sure all intended columns exist in data frame.
        Unwanted colums are discarded. Intended, but not yet
        existing are created with value "None".
        """
        # Create missing columns with their appropriate dtype
        for column, dtype in column_names.items():
            if column not in df:
                df[column] = pd.Series(dtype=dtype) if dtype else None
            elif df[column].dtype != dtype:
                try:
                    # Coerce to intended dtype. This is needed as e.g. for
                    # columns with NA values, the column type
                    # otherwise would be created with float64.
                    df[column] = df[column].astype(dtype)
                except Exception:
                    logging.error(f'Error enforcing type {dtype} for column {column}')
                    raise

        # restrict to essentiel columns or provide defaults
        return df[column_names.keys()]
