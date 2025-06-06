# Copyright 2023 Holger Bruch (hb@mfdz.de)
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

import argparse
import json
import logging
import re
from datetime import datetime
from typing import Optional

import defusedxml.ElementTree as ET
import requests

logger = logging.getLogger(__name__)

INCIDENT_TYPE_MAPPPING = {
    'roadClosed': 'ROAD_CLOSED',
    'carriagewayClosures': 'ROAD_CLOSED',
    'newRoadworksLayout': 'CONSTRUCTION',
    'repairWork': 'CONSTRUCTION',
}

# Datex namespace
ns = {'d': 'http://datex2.eu/schema/2/2_0'}


class DatexII2CifsTransformer:
    # Pattern to validate lanestatus encoding. For expected values, see https://www.mdm-portal.de/wp-content/uploads/2019/03/mdm_datenmodell_baustellen_04-00-00.zip
    LANE_STATUS_PATTERN = re.compile('^([sxui]*l?)?[suioewx]*(1|2)[suioewx]*(r?[xsoe]*)?$')

    should_skip_roadworks_in_past = True

    def __init__(self, reference, should_skip_roadworks_in_past: bool = True, current_time: datetime = datetime.now()):
        self.reference = reference
        self.should_skip_roadworks_in_past = should_skip_roadworks_in_past
        self.current_time = current_time

    def _roadworks_name(self, situationRecord: ET) -> str | None:
        """
        Extracts roadworks name from generalPublicComment with commentType2 equal to roadworksName.

        <generalPublicComment>
            <comment>
                <values>
                    <value lang="DE">L154 Albtalsperrung</value>
                </values>
            </comment>
            <commentExtension>
                <commentExtended>
                    <commentType2>roadworksName</commentType2>
                </commentExtended>
            </commentExtension>
        </generalPublicComment>
        </groupOfLocations>
        """
        for generalPublicComment in situationRecord.findall('d:generalPublicComment', ns):
            if (
                'roadworksName'
                in generalPublicComment.find('d:commentExtension/d:commentExtended/d:commentType2', ns).text
            ):
                return generalPublicComment.find('d:comment/d:values/d:value', ns).text
        return None

    def _road_name(self, situationRecord: ET) -> str:
        """
        Extracts road name from linearElement within groupOfLocations:

        <groupOfLocations xsi:type="Linear">
            ...
            <linearWithinLinearElement>
                ...
                <linearElement>
                    <roadName>
                        <values>
                            <value lang="de">Albbruck-St. Blasien</value>
                        </values>
                    </roadName>
                    <roadNumber>L154</roadNumber>
                </linearElement>
        </groupOfLocations>
        """
        linearElement = situationRecord.find('d:groupOfLocations//d:linearElement', ns)
        if linearElement is None:
            return 'Gemeindestraße'  # TODO should be in DATEX

        roadnameElement = linearElement.find('d:roadName/d:values/d:value', ns)
        roadname = roadnameElement.text if roadnameElement is not None else ''
        roadNumberElement = linearElement.find('d:roadNumber', ns)
        roadnumber = roadNumberElement.text if roadNumberElement is not None else ''
        return f'{roadnumber} {roadname}'.strip()

    def _incident_type(self, situationRecord: ET) -> str:
        roadworkType = situationRecord.find('d:roadOrCarriagewayOrLaneManagementType', ns)
        if roadworkType is None:
            roadworkType = situationRecord.find('d:roadMaintenanceType', ns)

        type = 'CONSTRUCTION'
        if roadworkType is not None:
            type = INCIDENT_TYPE_MAPPPING.get(roadworkType.text, 'CONSTRUCTION')

        return type

    def _incident_subtype(self, situationRecord: ET) -> str:
        """
        Returns ROAD_CLOSED_CONSTRUCTION in case the road is incident_type is ROAD_CLOSED
        """
        return 'ROAD_CLOSED_CONSTRUCTION' if self._incident_type(situationRecord) == 'ROAD_CLOSED' else ''

    def _is_referenced_as_cause(self, situation, situationRecord):
        situationRecordId = situationRecord.get('id')
        managedCause = situation.find(
            "d:situationRecord/d:cause/d:managedCause/[@id='{}']".format(situationRecordId), ns
        )

        return managedCause is not None

    def _should_skip(self, situation: ET, situationRecord: ET) -> bool:
        """
        Skips a situationRecord if one of the following criteris is met:
        * suffix ends on '-gegen' (BEMaS/BIS specific encoding of opposite direction, which will be handled by setting direction as BOTH_DIRECTIONS)
        * self.should_skip_roadworks_in_past is True and endtime is in the past
        * the situationRecord is referenced as cause (in which case we assume the caused situationRecord has the relevant restriction details)
        """
        situationRecordId = situationRecord.get('id')
        if '-gegen' in situationRecordId:
            # roadworks in oposite direction are handled via directions attribute
            # Note: This is a BW specific encoding which will not work out for other datasets
            logger.debug('skip situationRecord %s as it is opposite direction', situationRecord.get('id'))

            return True

        if self.should_skip_roadworks_in_past:
            (starttime, endtime) = self._get_start_end_time(situationRecord)
            if self.current_time.astimezone() > datetime.fromisoformat(endtime):
                logger.debug('skip situationRecord %s as it is in the past', situationRecord.get('id'))
                return True

        if self._is_referenced_as_cause(situation, situationRecord):
            logger.debug('skip situationRecord %s as it is referenced as cause', situationRecord.get('id'))
            return True

        return False

    def _laneStatusCoded(self, situationRecord: ET) -> Optional[str]:
        """
        Extracts the laneStatusCoded value of the given situationRecord or None, if not available.

        Example: for the following examplary, partial situationRecord, `o2xx` would be returned:

        <situationRecord id="xxx">
            <impact>
                <impactExtension>
                    <impactExtended>
                        <laneStatusCoded>o2xx</laneStatusCoded>
                        <laneRestriction>
                            <lane>allLanesCompleteCarriageway</lane>
                        </laneRestriction>
                    </impactExtended>
                </impactExtension>
            </impact>
        </situationRecord>
        """
        lsElement = situationRecord.find('d:impact/d:impactExtension/d:impactExtended/d:laneStatusCoded', ns)

        if lsElement is None:
            return None
        if self.LANE_STATUS_PATTERN.match(lsElement.text):
            return lsElement.text

        logger.warning(
            'ignore laneStatus %s for situatinoRecord %s as it has unexpected encoding',
            lsElement.text,
            situationRecord.get('id'),
        )
        return None

    @staticmethod
    def _is_opposite_direction_concerned(lanestatus: str) -> bool:
        # lanes can be single carriageways (encoded by a centre line indicated by '1', or dual carriageways (encoded by separating centreline indicated '2'))
        # we split at both.
        lanesPerDirection = lanestatus.replace('2', '1').split('1')

        leftLanes = lanesPerDirection[0]
        rightLanes = lanesPerDirection[1]

        # if leftLanes include more than unnrestricted lane, should, shoulder separatore, or
        # some lanes of opposite directions are switched to the right lines, opposite direction is concerned
        hasAnyLaneLeftToCentreLine = 0 < len(leftLanes)
        hasLeftLanesOtherThanUnrestrictedAndSideLanes = len(re.sub('[usl]', '', leftLanes)) > 0
        hasOppositeLanesRightToCentreLine = len(re.sub('[^uiw]', '', rightLanes)) > 0
        return (
            hasAnyLaneLeftToCentreLine
            and hasLeftLanesOtherThanUnrestrictedAndSideLanes
            or hasOppositeLanesRightToCentreLine
        )

    def _detect_direction(self, situation: ET, situationRecord: ET) -> str:
        """
        For BIS/BEMaS generated DATEX, a road closure has also an opposite direction,
        if for a situationRecord with id suffix -sperrung a situation with
        id suffix '-gegen-sperrung' exists.
        For constructions, we rely on existance of laneStatusCoded to deduce if
        any lane left of the centre line is blocked or dedicated to traffic
        in this record's direction.
        """

        situationRecordId = situationRecord.get('id')
        if situationRecordId.endswith('-sperrung'):
            inverse_direction_id = situationRecordId.replace('-sperrung', '-gegen-sperrung')
            check: list | None = situation.find("d:situationRecord[@id='{}']".format(inverse_direction_id), ns)
            if check is None or len(check) == 0:
                return 'ONE_DIRECTION'
            return 'BOTH_DIRECTIONS'

        laneStatusCoded = self._laneStatusCoded(situationRecord)
        if laneStatusCoded is not None:
            return 'BOTH_DIRECTIONS' if self._is_opposite_direction_concerned(laneStatusCoded) else 'ONE_DIRECTION'

        # be defensive, if we don't know, be assume both are concerned
        return 'BOTH_DIRECTIONS'

    def _get_start_end_time(self, situationRecord: ET) -> tuple[str, str]:
        """
        Extracts daate/time intervaal from validityTimeSpecification.
        """
        validity = situationRecord.find('d:validity/d:validityTimeSpecification', ns)
        endtime = validity.find('d:overallEndTime', ns).text
        starttime = validity.find('d:overallStartTime', ns).text

        return (starttime, endtime)

    def _parse(self, datex2file: str) -> ET:
        if datex2file.startswith('http'):
            r = requests.get(datex2file, timeout=10)
            r.encoding = 'UTF-8'
            return ET.fromstring(r.text)

        return ET.parse(datex2file).getroot()

    def _pairwise(self, t: list) -> list[list]:
        it = iter(t)
        return [[t[1], t[0]] for t in zip(it, it)]

    def transform_datex2(self, datex2doc: ET, format: str = 'cifs') -> dict:
        """
        Transforms situation records into cifs-roadworks, like e.g.:
        [{
          "id": "101",
          "type": "ROAD_CLOSED",
          "subtype": "ROAD_CLOSED_CONSTRUCTION",
          "polyline": "51.510090 -0.006902 51.509142 -0.006564 51.506291 -0.003640 51.503796 0.001051 51.499218 0.001687 51.497365 0.002020",
          "street": "NW 12th St",
          "starttime": "2016-04-07T09:00:00+01:00",
          "endtime": "2016-04-07T23:00:00+01:00",
          "description": "Closure on I-95 NB due to construction",
          "direction": "BOTH_DIRECTIONS"
        },
        ...
        ]
        """

        closures = []
        features = []

        root = datex2doc
        payload = root.find('d:payloadPublication', ns)
        for situation in payload.findall('d:situation', ns):
            overallSituation = situation.find('d:situationExtension/d:situationExtended/d:overallSituation', ns)
            for situationRecord in situation.findall('d:situationRecord', ns):
                if self._should_skip(situation, situationRecord):
                    continue

                polyline = situationRecord.find(
                    'd:groupOfLocations/d:linearExtension/d:linearExtended/d:gmlLineString/d:posList', ns
                )
                if polyline is None:
                    # FIXME the order of lat/lon currently is wrong for the BW publication
                    longitude_element = situationRecord.find('d:groupOfLocations/d:locationForDisplay/d:longitude', ns)
                    latitude_element = situationRecord.find('d:groupOfLocations/d:locationForDisplay/d:latitude', ns)
                    if longitude_element is None or latitude_element is None:
                        # TODO log warning
                        continue
                    lat = float(longitude_element.text)
                    lon = float(latitude_element.text)
                    # FIXME the BW publication does not contain a LineString. As this is required by cifs, we add a minimal offset as workaround
                    geometry = '{} {} {} {}'.format(lat, lon, lat, lon + 0.00001)
                    geojsonGeometry = {'type': 'Point', 'coordinates': [lon, lat]}
                else:
                    geometry = polyline.text
                    geojsonGeometry = {
                        'type': 'LineString',
                        'coordinates': self._pairwise([float(i) for i in geometry.split()]),
                    }

                (starttime, endtime) = self._get_start_end_time(situationRecord)
                location = {
                    'polyline': geometry,
                    'street': self._road_name(situationRecord),
                    'direction': self._detect_direction(situation, situationRecord),
                }
                closure = {
                    'id': situationRecord.get('id'),
                    'type': self._incident_type(situationRecord),
                    'subtype': self._incident_subtype(situationRecord),
                    'starttime': starttime,
                    'endtime': endtime,
                    'description': self._roadworks_name(situationRecord) or self._roadworks_name(overallSituation),
                    'reference': self.reference,
                }

                if 'geojson' == format:
                    closure['street'] = location.get('street')
                    closure['direction'] = location.get('direction')
                    feature = {'type': 'Feature', 'geometry': geojsonGeometry, 'properties': closure}
                    features.append(feature)
                else:
                    closure['location'] = location
                    closures.append(closure)

        if 'geojson' == format:
            geojson = {'type': 'FeatureCollection', 'features': features}
            json_result = geojson
        else:
            incidents = {'incidents': closures, 'timestamp': self.current_time.isoformat()}
            json_result = incidents

        return json_result

    def transform(self, datex2file: str, format: str = 'cifs') -> dict:
        """
        Transforms situation records into cifs-roadworks, like e.g.:
        [{
          "id": "101",
          "type": "ROAD_CLOSED",
          "subtype": "ROAD_CLOSED_CONSTRUCTION",
          "polyline": "51.510090 -0.006902 51.509142 -0.006564 51.506291 -0.003640 51.503796 0.001051 51.499218 0.001687 51.497365 0.002020",
          "street": "NW 12th St",
          "starttime": "2016-04-07T09:00:00+01:00",
          "endtime": "2016-04-07T23:00:00+01:00",
          "description": "Closure on I-95 NB due to construction",
          "direction": "BOTH_DIRECTIONS"
        },
        ...
        ]
        """

        return self.transform_datex2(self._parse(datex2file), format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('datex2file', help='DATEX2 file or URL')
    parser.add_argument('-f', dest='format', required=False, default='cifs', choices=['cifs', 'geojson'])
    parser.add_argument('-o', dest='outfile', required=False, default='-', nargs='?', type=argparse.FileType('w'))
    parser.add_argument('-r', dest='reference', required=False, default='SVZ-BW')
    args = parser.parse_args()

    json_result = DatexII2CifsTransformer(args.reference).transform(args.datex2file, args.format)
    json.dump(json_result, args.outfile)
