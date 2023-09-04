# Dagster libraries to run both dagit and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.11 AS base

RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker

ARG DAGSTER_HOME=/opt/dagster/dagster_home/
ENV DAGSTER_HOME=$DAGSTER_HOME
WORKDIR $DAGSTER_HOME

COPY dagster.docker.yaml ./dagster.yaml
COPY workspace.docker.yaml ./workspace.yaml

ENTRYPOINT []

FROM base AS dagit

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]

CMD ["dagster-daemon", "run"]
