# Changelog

The changelog lists most feature changes between each release. 


## 2024-05-17
- change: `sharing_station_status` now reports vehicle availability for the feed's predominant `form_factor`, even for station, which don't have `vehicle_types_available` explicitly stated. Note: this requires, that all `vehicle_types` in `vehicle_types.json` declare the same `form_factor`. 

