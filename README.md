# MobiData-BW DIP Pipeline

This repo represents the MobiData-BW DIP Pipeline, which retrieves and transforms 
different datasources and publishes them either as datasets or pushes them to 
databases form where they are served, e.g. as WMS/WFS or REST service.

To run this pipeline locally, copy `.env.EXAMPLE` to `.env` and adapt the variables, i.e. 
choose your proper password for the DAGSTER_POSTGRES_PASSWORD.

To start, use

```sh
$ docker-compose up
```
