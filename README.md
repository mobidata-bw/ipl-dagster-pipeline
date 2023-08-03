# MobiData-BW DIP Pipeline

This repo represents the MobiData-BW DIP Pipeline, which retrieves and transforms 
different datasources and publishes them either as datasets or pushes them to 
databases form where they are served, e.g. as WMS/WFS or REST service.

To run this pipeline, copy `.env.EXAMPLE` to `.env` and adapt the variables, i.e. 
choose your proper password for the DAGSTER_POSTGRES_PASSWORD.

This dagster project can be started either in develepment mode, using 

```sh
$ dagster dev
```

or via docker-compose, which is the way it is itended to be deployed with:

```sh
$ docker-compose up --build
```

Note that the config differs in that for docker-compose, `workspace.docker.yaml` and `dagster.docker.yaml` will be used, which configure a postgres db as dagster storage, whild `dagster dev` will use sqlite and temporary folders.
