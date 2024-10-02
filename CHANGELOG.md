# Changelog

The changelog lists most feature changes between each release. 

## 2024-10-01

- GTFS import: adapt to [`postgis-gtfs-importer:v4-2024-09-24T15.06.43-9a66d7d` image](https://github.com/mobidata-bw/postgis-gtfs-importer/pkgs/container/postgis-gtfs-importer/278891924?tag=v4-2024-09-24T15.06.43-9a66d7d) (21449e7)
	- This is the `ipl-dagster-pipeline` equivalent to [`ipl-orchestration#726e765`](https://github.com/mobidata-bw/ipl-orchestration/commit/726e7650820adf848cd79787946340ee7c4cf02f).
- GTFS import: don't remove redundant stops (6a46a63)

## 2024-09-06
- addition: `sharing_vehicle` table now also contains a `station_id`

## 2024-07-05
- change: reduce CPU shares of GTFS import to 512 (#140)
- change: bump dagster to v1.7.12 and dagster-docker to v0.23.12 (#160)

## 2024-06-26
- change: `sharing_station.capacity` is changed to an integer field, `vehicle.max_range_meters` and `vehicle.current_range_meters`, according to the [GBFS spec](https://github.com/MobilityData/gbfs/blob/cd75662c25180f68f76237f88a861d82e940cf3b/gbfs.md?plain=1#L1044), to float.
- change: `sharing_station_status` now reports vehicle availability for the feed's predominant `form_factor`, even for station, which don't have `vehicle_types_available` explicitly stated. Note: this requires, that all `vehicle_types` in `vehicle_types.json` declare the same `form_factor`. 
