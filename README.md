# IPL Data Pipeline

This repo represents the **data transformation pipeline of the MobiData-BW *Integrationsplatform* (IPL)**.

It uses [Dagster](https://dagster.io) to retrieve and transform several datasources. The results are either published as datasets, or written into databases and then being served (e.g. as WMS/WFS or REST service) by other IPL services.

## Usage

*Note:* This repo to is designed to be run as a part of the entire IPL platform, [as defined in the `ipl-orchestration` repo](https://github.com/mobidata-bw/ipl-orchestration). But you can also run it in a standalone fashion.

### Prerequisites

If you intend to run this project locally via `dagster dev`, you need to have python 3.12.

To install all required libraries, use

`pip install -r requirements.txt`

Note: `requirements.txt` imports `requirements-pipeline.txt` and `requirements-dagster.txt`, which include the
dependencies for the different dagster services. `pipeline.Dockerfile` and `dagster.Dockerfile` just import these respective requirements.


### Development docker compose

Instead of running it manually, you can use the `docker-compose.dev.yml` for running all required dependencies. To do
so, you need an `.env` file with the uid / gid of your local user like this:

```
DOCKER_LOCAL_USER=1000:1000
```

Afterwards, you can start the dev container with it's dependencies (a PostGIS and an FTP server) using

```bash
docker compose -f docker-compose.dev.yml run --rm python bash
```

With this approach, you can run tests, too:

```bash
docker compose -f docker-compose.dev.yml run --rm python python -m pytest tests
```

There is a makefile to make it a bit simpler for you:

```bash
make  # gives the interactive shell
make test  # runs tests
make docker-build  # builds everything
make docker-purge  # purges everything, including, good for a fresh start
make lint-check  # runs the ruff linter and formatter in check mode
make lint-fix  # runs the ruff linter and formatter in fix mode
```

For committing, there is also a pre-commit file. You can activate it with

```bash
pre-commit install
```


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
