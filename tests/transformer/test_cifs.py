#  pytest --disable-warnings -s tests/transformer/test_cifs.py
from datetime import datetime

import pytest

from pipeline.transformer.cifs import DatexII2CifsTransformer


def test_situation_1487640():
    """
    This tests asserts that for a complex situationRecord describing a
    roadwork with multiple indipendent situations, some of them in the past,
    only the currently active situtation is extracted with it's corresponding properties.
    """
    t = DatexII2CifsTransformer(
        'Test', current_time=datetime.strptime('2024-01-01', '%Y-%m-%d').astimezone('Europe/Berlin')
    )
    cifs = t.transform('./tests/transformer/situation_1487640.xml')
    assert 'incidents' in cifs
    incidents = cifs['incidents']
    assert len(incidents) == 1
    incident = cifs['incidents'][0]

    assert incident['type'] == 'ROAD_CLOSED'
    assert incident['subtype'] == 'ROAD_CLOSED_CONSTRUCTION'
    assert incident['location']['street'] == 'L154 Albbruck-St. Blasien'


def test_situation_2959413():
    """
    This tests asserts that for a complex situationRecord describing a
    roadwork with multiple indipendent situations a specific situtation is extracted
    with it's corresponding properties.
    """
    t = DatexII2CifsTransformer(
        'Test', current_time=datetime.strptime('2024-01-01', '%Y-%m-%d').astimezone('Europe/Berlin')
    )
    cifs = t.transform('./tests/transformer/situation_2959413-4272241-4272242-4272245.xml')
    assert 'incidents' in cifs
    incident = list(filter(lambda incident: incident['id'] == '2959413-4272241-4272242-4272245', cifs['incidents']))[0]

    assert incident['type'] == 'CONSTRUCTION'
    assert incident['location']['street'] == 'L409 B294/L409 Krähenhart-B462/L409 Klosterreichenbach'
    assert incident['description'] == 'L409 Lkw-Verbot'


@pytest.mark.parametrize(
    'test_laneStatusCoded,expected',
    [('x2x', True), ('u1x', False), ('sluu2xxro', False), ('uo2xx', True), ('uu2uoo', True)],
)
def test_eval(test_laneStatusCoded, expected):
    t = DatexII2CifsTransformer('Test')

    assert t._is_opposite_direction_concerned(test_laneStatusCoded) == expected
