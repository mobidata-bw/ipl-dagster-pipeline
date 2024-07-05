# Changelog

The changelog lists most feature changes between each release. 

## 2024-07-05
- change: reduce CPU shares of GTFS import to 512 (#140)
- change: bump dagster to v1.7.12 and dagster-docker to v0.23.12 (#160)

## 2024-06-26
- change: `sharing_station.capacity` is changed to an integer field, `vehicle.max_range_meters` and `vehicle.current_range_meters`, according to the [GBFS spec](https://github.com/MobilityData/gbfs/blob/cd75662c25180f68f76237f88a861d82e940cf3b/gbfs.md?plain=1#L1044), to float.
- change: `sharing_station_status` now reports vehicle availability for the feed's predominant `form_factor`, even for station, which don't have `vehicle_types_available` explicitly stated. Note: this requires, that all `vehicle_types` in `vehicle_types.json` declare the same `form_factor`. 
