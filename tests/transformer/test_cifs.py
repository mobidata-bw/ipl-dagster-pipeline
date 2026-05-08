#  pytest --disable-warnings -s tests/transformer/test_cifs.py
from datetime import datetime

import pytest

from pipeline.transformer.cifs import DatexII2CifsTransformer


def test_situation_1487640():
    """
    This tests asserts that for a complex situationRecord describing a
    roadwork with multiple independent situations, some of them in the past,
    only the currently active situtation is extracted with it's corresponding properties.
    """
    t = DatexII2CifsTransformer('Test', current_time=datetime.strptime('2024-01-01', '%Y-%m-%d'))
    cifs = t.transform('./tests/transformer/situation_1487640.xml')
    assert 'incidents' in cifs
    incidents = cifs['incidents']
    assert len(incidents) == 1
    incident = cifs['incidents'][0]

    assert incident['type'] == 'ROAD_CLOSED'
    assert incident['subtype'] == 'ROAD_CLOSED_CONSTRUCTION'
    assert incident['location']['street'] == 'L154 Albbruck-St. Blasien'


def test_situation_2959413_cifs():
    """
    This tests asserts that for a complex situationRecord describing a
    roadwork with multiple independent situations a specific situtation is extracted
    in CIFS format with it's corresponding properties.
    """
    t = DatexII2CifsTransformer('Test', current_time=datetime.strptime('2024-01-01', '%Y-%m-%d'))
    cifs = t.transform('./tests/transformer/situation_2959413-4272241-4272242-4272245.xml')
    assert 'incidents' in cifs
    incident = list(
        filter(lambda incident: incident['id'] == '2959413-4272241-4272242-4272245.001', cifs['incidents'])
    )[0]

    assert incident['type'] == 'CONSTRUCTION'
    assert incident['location']['street'] == 'L409 B294/L409 Krähenhart-B462/L409 Klosterreichenbach'
    assert incident['description'] == 'L409 Lkw-Verbot'


def test_situation_geometries():
    """
    This tests asserts that for a complex situationRecord describing a
    roadwork with multiple independent situations a specific situtation is extracted
    in GeoJSON format with it's corresponding properties and expected geometry
    """
    t = DatexII2CifsTransformer('Test', current_time=datetime.strptime('2024-01-01', '%Y-%m-%d'))
    geojson = t.transform('./tests/transformer/situation_multiple_records.xml', format='geojson')

    features = geojson['features']
    assert features[0]['geometry'] == {
        'coordinates': [[8.378342, 48.486938], [8.378332, 48.486914]],
        'type': 'LineString',
    }
    assert features[1]['geometry'] == {
        'coordinates': [[8.0, 48.0], [8.1, 48.1]],
        'type': 'LineString',
    }


def test_situation_multi_valid_periods():
    """
    This tests asserts that for a situationRecord with multiple separate validPeriods,
    multiple incidents are created.
    """
    expected_feature_properties = {
        'reference': 'Test',
        'description': 'A81 Bauwerksarbeiten Rückbau LSW',
        'street': 'Gemeindestraße',
        'direction': 'BOTH_DIRECTIONS',
        'starttime': '2025-04-15T07:00:00.000+02:00',
        'endtime': '2025-04-15T20:00:00.000+02:00',
        'id': '2454613-37594876-37594877-37594882.002',
        'type': 'CONSTRUCTION',
        'subtype': '',
    }

    t = DatexII2CifsTransformer('Test', current_time=datetime.strptime('2024-01-01', '%Y-%m-%d'))
    geojson = t.transform('./tests/transformer/situation_multi_valid_periods.xml', format='geojson')

    assert len(geojson.get('features')) == 2

    assert geojson['features'][1]['properties'] == expected_feature_properties
    # Assert IDs are unique
    ids = set([incident['properties']['id'] for incident in geojson['features']])
    assert len(geojson['features']) == len(ids)
    # Assert Geometries are unique
    assert geojson['features'][0]['geometry'] == geojson['features'][1]['geometry']


def test_situation_multi_valid_consecutive_periods():
    """
    This tests asserts that for a situationRecord with multiple separate but consecutive
    validPeriods, these are merged.
    """
    t = DatexII2CifsTransformer('Test', current_time=datetime.strptime('2024-01-01', '%Y-%m-%d'))
    cifs = t.transform('./tests/transformer/situation_multi_valid_consecutive_periods.xml')
    assert 'incidents' in cifs
    assert len(cifs['incidents']) == 2
    assert cifs['incidents'][0]['endtime'] == '2025-04-15T07:00:00.000+02:00'
    assert cifs['incidents'][1]['endtime'] == '2025-04-16T07:00:00.000+02:00'


@pytest.mark.parametrize(
    'test_laneStatusCoded,expected',
    [('x2x', True), ('u1x', False), ('sluu2xxro', False), ('uo2xx', True), ('uu2uoo', True)],
)
def test_eval(test_laneStatusCoded, expected):
    """
    Assert that DatexII2CifsTransformer deduces correctly from laneStatusCoded,
    if opposite direction is concerned.
    Opposite direction is concerned if all lanes on left carriageway (=left of lane separator code 1 or 2)
    are unrestricted (u) and no opposite lane is shifted to the right carriageway.
    """
    t = DatexII2CifsTransformer('Test')

    assert t._is_opposite_direction_concerned(test_laneStatusCoded) == expected
