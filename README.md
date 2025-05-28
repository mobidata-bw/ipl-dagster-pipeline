# IPL Data Pipeline

This repo represents the **data transformation pipeline of the MobiData-BW *Integrationsplatform* (IPL)**.

It uses [Dagster](https://dagster.io) to retrieve and transform several datasources. The results are either published as datasets, or written into databases and then being served (e.g. as WMS/WFS or REST service) by other IPL services.

## Usage

*Note:* This repo to is designed to be run as a part of the entire IPL platform, [as defined in the `ipl-orchestration` repo](https://github.com/mobidata-bw/ipl-orchestration). But you can also run it in a standalone fashion.

### Prerequisites

If you intend to run this project locally via `dagster dev`, you need to have python 3.10–3.12 for everything except
for the webcam handling, there it has to be 3.12 because of
[relative_to(walk_up=True)](https://docs.python.org/3/library/pathlib.html#pathlib.PurePath.relative_to)

To install all required libraries, use

`pip install -r requirements.txt`

Note: `requirements.txt` imports `requirements-pipeline.txt` and `requirements-dagster.txt`, which include the
dependencies for the different dagster services. `pipeline.Dockerfile` and `dagster.Dockerfile` just import these respective requirements.

In addition, you need a postgres database into which the datasets are loaded.
This database can be started via

`docker compose -f docker-compose.dev.yml up`

### Running

To start this dagster project in interactive develepment mode, you should use a DAGSTER_HOME other than
this project directory, as a) the dagster.yml defines a postgres storage for dagster run information
and is usually intended for prod use, and b) a number of files is generated in the temporary dagster
directories which would impact your IDE responsiveness if it's indexing new files continuously.


```sh
$  DAGSTER_HOME=/tmp/DAGSTER_HOME dagster dev
```

or via `docker compose`, which is the way it is itended to be deployed with:

```sh
$ docker compose up --build
```

Note that the config differs in that for docker-compose, `workspace.docker.yaml` and `dagster.docker.yaml` will be used, which configure a postgres db as dagster storage, whild `dagster dev` will use sqlite and temporary folders.
