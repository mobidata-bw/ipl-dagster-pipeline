# Changelog

The changelog lists most feature changes between each release.

## [unreleased]

- add CSS styling to [webcam overview page](https://api.mobidata-bw.de/webcam/overview/)

## 2025-06-06

- ⚠️ Change to Python 3.12 as the base python version
- ⚠️ Webcams: extended and breaking approach with symlinking an index page. This comes with an extended sync system,
  requiring new env vars: `$IPL_WEBCAM_IMAGE_PATH` and `$IPL_WEBCAM_SYMLINK_PATH`. As there are relative symlinks
  between the two of them, please ensure that the host stucture is the same as the container structure when mounting
  these two paths.


## 2025-05-09

- GTFS import: Allow manually resolving a domain name using `$IPL_GTFS_IMPORTER_EXTRA_HOST_HOSTNAME` & `$IPL_GTFS_IMPORTER_EXTRA_HOST_IP`.
  - To make the GTFS import work with the *MobiData-BW IPL deployment*, where we must manually resolve the GTFS server's domain.
    - This is the `ipl-dagster-pipeline` equivalent to [`ipl-orchestration#62fb7e2`](https://github.com/mobidata-bw/ipl-orchestration/commit/62fb7e28aeaee1ad88914ce7004be1cda539abec) and  [`ipl-orchestration#07cc70f`](https://github.com/mobidata-bw/ipl-orchestration/commit/07cc70fca8af543315722d5cd93fad84e0073429).


## 2025-03-12
- Fix: on startup, (force) terminate runs still in started/starting state, as dagster doesn't terminate them cleanly on shutdown (https://github.com/mobidata-bw/ipl-dagster-pipeline/commit/81135abf8f80a4ba49f16fbcf24a6496a5bc48dc).
- Fix: enable run monitoring to terminate jobs hanging on startup/cancellation (after 180s) or running for more than 6h (https://github.com/mobidata-bw/ipl-dagster-pipeline/commit/7defa4d7ec22f69595e2de2ad0ee3c49bd22dc90)
    - ⚠️ NOTE: note this config needs to be replicated in case you use ipl-dagster-pipeline with an externally mounted dagster config.
- Chore: [update to Dagster 1.10.2.](https://github.com/mobidata-bw/ipl-dagster-pipeline/pull/188)
- Switch from deprecated `AutoMaterialization` and `FreshnessPolicy` to `AutomationCondition`. Assets will now be materialized on a cronlike schedule and eagerly on startup (in case they did not exist before).


## 2025-01-28
- Fix: [create primary key if missing](https://github.com/mobidata-bw/ipl-dagster-pipeline/pull/182)
- Fix: [only push roadworks with geom_type LineString to postgis](https://github.com/mobidata-bw/ipl-dagster-pipeline/pull/184)


## 2024-12-03

- Fix [webcam cleanup](https://github.com/mobidata-bw/ipl-dagster-pipeline/pull/180)


## 2024-11-04

- addition: Add the ability to [download webcam images](https://github.com/mobidata-bw/ipl-dagster-pipeline/pull/177)
  with [configurable worker count](https://github.com/mobidata-bw/ipl-dagster-pipeline/pull/179)

## 2024-10-29

- GTFS import: Mount local (modified) `download.sh` into `postgis-gtfs-importer` container.
    - ⚠️ `$IPL_GTFS_IMPORTER_HOST_CUSTOM_SCRIPTS_DIR` must contain a `download.sh`, so if it doesn't, this is a breaking change!
    - This is the `ipl-dagster-pipeline` equivalent to [`ipl-orchestration#fd42328`](https://github.com/mobidata-bw/ipl-orchestration/commit/fd423288ac8d1a1902ebfed60339f1fe120ce508).

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
