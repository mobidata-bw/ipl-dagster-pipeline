import os

os.environ['ROADWORKS_SVZBW_DATEX2_DOWNLOAD_URL'] = (
    'https://api.mobidata-bw.de/datasets/traffic/roadworks/roadworks_svzbw.datex2.xml'
)
os.environ['IPL_ROADWORKS_ADB_SHORT_TERM_DATEX2_DOWNLOAD_URL'] = (
    'https://api.mobidata-bw.de/datasets/traffic/roadworks/roadworks_adb_short_term.datex2.xml'
)
os.environ['IPL_ROADWORKS_ADB_LONG_TERM_DATEX2_DOWNLOAD_URL'] = (
    'https://api.mobidata-bw.de/datasets/traffic/roadworks/roadworks_adb_long_term.datex2.xml'
)

# os.environ['EVENTS_SVZBW_DATEX2_DOWNLOAD_URL'] = 'https://api.mobidata-bw.de/datasets/traffic/roadworks/roadworks_svzbw.datex2.xml'


from pipeline.assets.traffic_incidents import (
    incidents_lmsbw_datex2,
    roadworks_adb_long_term_datex2,
    roadworks_adb_long_term_geoparquet,
    roadworks_adb_short_term_datex2,
    roadworks_adb_short_term_geoparquet,
    roadworks_and_events_bw_geoparquet,
    roadworks_svzbw_datex2,
    roadworks_svzbw_geoparquet,
)
from pipeline.transformer.cifs import DatexII2CifsTransformer


def test_roadworks_and_events_bw_geoparquet():
    roadworks_svzbw_datex2()
    roadworks_svzbw_geoparquet()
    roadworks_adb_long_term_datex2()
    roadworks_adb_long_term_geoparquet()
    roadworks_adb_short_term_datex2()
    roadworks_adb_short_term_geoparquet()
    roadworks_and_events_bw_geoparquet()
