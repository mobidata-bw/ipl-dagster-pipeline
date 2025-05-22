# syntax = docker/dockerfile:1
FROM python:3.11-slim

LABEL org.opencontainers.image.title="MobiData-BW Data Pipeline API"
LABEL org.opencontainers.image.authors="Holger Bruch <hb@mfdz.de>, MobiData-BW IPL contributors <mobidata-bw@nvbw.de>"
LABEL org.opencontainers.image.documentation="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.source="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.licenses="(EUPL-1.2)"

WORKDIR /opt/dagster/app

RUN apt update && apt install -y \
        build-essential \
        libgdal-dev \
        lftp \
        git \
    && rm -rf /var/lib/apt/lists/*


# Copy and install requirements
COPY requirements.txt requirements-pipeline.txt requirements-dev.txt requirements-dagster.txt /opt/dagster/app/
RUN pip install -r requirements.txt
