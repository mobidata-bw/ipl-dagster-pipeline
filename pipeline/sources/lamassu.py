# push_lamassu_feeds_to_postgis(EnvVar("DIP_LAMASSU_BASE_URL"))


import traceback
from typing import Optional

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection

# TODO: Fragen MobiData-BW:
# Schema für Stationen und Fahrzeuge?
# Stationen können unterschiedliche Form-Faktoren umfassen. Als welche sollen Sie zurückgegeben werden?
# Wenn keine Fahrzeuge verfügbar sind, anhand derer dies ermittelbar wäre, ist dies nicht beurteilbar. Vorschlag: Kreuzprodukt aus vehicle_types.form_factor?
# num_bikes_available heißt es heute, ich würde auf num_vehicles_available umstellen
STATION_COLUMNS = [
    'feed_id',
    'station_id',
    'name',
    'num_bikes_available',
    'rental_uris_android',
    'rental_uris_ios',
    'rental_uris_web',
    'last_reported',
    'geometry',
]
VEHICLE_COLUMNS = [
    'feed_id',
    'vehicle_id',
    'form_factor',
    'name',
    'is_reserved',
    'propulsion_type',
    'max_range_meters',
    'rental_uris_android',
    'rental_uris_ios',
    'rental_uris_web',
    'last_reported',
    'geometry',
]


class Lamassu:
    lamassu_base_url: str
    # Timeout used for lamassu requests
    timeout: int = 2

    def __init__(self, lamassu_base_url: str):
        self.lamassu_base_url = lamassu_base_url

    def get_systems(self) -> dict:
        resp = requests.get(f'{self.lamassu_base_url}/gbfs', timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()['systems']

    def get_system_feeds(self, system_id: str, preferred_feed_languages: list) -> dict:
        resp = requests.get(f'{self.lamassu_base_url}/gbfs/{system_id}/gbfs.json', timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()['data']
        for lang in preferred_feed_languages:
            if lang in data:
                feeds = data[lang]['feeds']
                return {x['name']: x['url'] for x in feeds}

        return {}

    def get_stations_as_frame(self, feed: dict, feed_id: str) -> Optional[pd.DataFrame]:
        if 'station_information' not in feed or 'station_status' not in feed:
            return None

        stations_infos_df = self._load_feed_as_frame(feed['station_information'], 'stations')
        stations_status_df = self._load_feed_as_frame(feed['station_status'], 'stations')
        if stations_infos_df.empty or stations_status_df.empty:
            return None

        merged = pd.merge(stations_infos_df, stations_status_df, on=['station_id'])
        # add feed name to do delete insert
        merged['feed_id'] = feed_id
        # filter those not installed or not renting
        filtered = merged.loc[(merged['is_renting'] == True) & (merged['is_installed'] == True)]  # fmt: off

        # Add geometry
        filtered_with_geom = gpd.GeoDataFrame(
            filtered, geometry=gpd.points_from_xy(filtered.lon, filtered.lat), crs='EPSG:4326'
        )

        return self._enforce_columns(filtered_with_geom, STATION_COLUMNS)

        # TODO add operator
        # TODO add formfactor
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
        vehicle_types_df = self._load_feed_as_frame(feed['vehicle_types'], 'vehicle_types')

        if free_vehicle_status_df.empty:
            return None

        # Fix issues with duplicate vehicle_type_ids
        vehicle_types_df = vehicle_types_df.drop_duplicates(subset=['vehicle_type_id'], keep='last')

        # Join vehicles and their type informatoin
        merged = pd.merge(free_vehicle_status_df, vehicle_types_df, on=['vehicle_type_id'])
        merged['feed_id'] = feed_id
        # filter those already reserved or disabled
        # Note: 'is False' results in boolean label can not be used without a boolean index
        filtered = merged.loc[(merged['is_reserved'] == False) & (merged['is_disabled'] == False)]  # fmt: off

        # Add geometry
        filtered_with_geom = gpd.GeoDataFrame(
            filtered, geometry=gpd.points_from_xy(filtered.lon, filtered.lat), crs='EPSG:4326'
        )

        return self._enforce_columns(filtered_with_geom, VEHICLE_COLUMNS)

        # TODO add operator

    def _enforce_columns(self, df: pd.DataFrame, column_names: list) -> pd.DataFrame:
        """
        Make sure all intended columns exist in data frame.
        Unwanted colums are discarded. Intended, but not yet
        existing are created with value "None".
        """
        #
        for column in column_names:
            if column not in df:
                df[column] = None

        # restrict to essentiel columns or provide defaults
        return df[column_names]

    def _load_feed_as_frame(self, url: str, element: Optional[str] = None):
        """
        Loads a specific gbfs endpoint and returns the data node
        (or the data's <element> node) as a denormalized flat pandas frame.
        """
        resp = requests.get(url, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()['data'][element] if element else resp.json()['data']

        return pd.json_normalize(data, sep='_')
