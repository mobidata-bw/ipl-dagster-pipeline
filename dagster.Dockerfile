# Dagster libraries to run both dagit and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.11 AS base

COPY requirements-dagster.txt ./requirements-dagster.txt
RUN pip install -r requirements-dagster.txt

ARG DAGSTER_HOME=/opt/dagster/dagster_home/
ENV DAGSTER_HOME=$DAGSTER_HOME
WORKDIR $DAGSTER_HOME

COPY dagster.docker.yaml ./dagster.yaml
COPY workspace.docker.yaml ./workspace.yaml

ENTRYPOINT []

FROM base AS dagit

LABEL org.opencontainers.image.title="data pipeline webserver"
LABEL org.opencontainers.image.authors="Holger Bruch <hb@mfdz.de>, MobiData-BW IPL contributors <mobidata-bw@nvbw.de>"
LABEL org.opencontainers.image.documentation="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.source="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.licenses="(EUPL-1.2)"

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]

FROM base AS daemon

LABEL org.opencontainers.image.title="data pipeline daemon"
LABEL org.opencontainers.image.authors="Holger Bruch <hb@mfdz.de>, MobiData-BW IPL contributors <mobidata-bw@nvbw.de>"
LABEL org.opencontainers.image.documentation="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.source="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.licenses="(EUPL-1.2)"

CMD ["dagster-daemon", "run"]
