# syntax = docker/dockerfile:1
FROM python:3.12-slim

LABEL org.opencontainers.image.title="MobiData-BW Data Pipeline API"
LABEL org.opencontainers.image.authors="Holger Bruch <hb@mfdz.de>, MobiData-BW IPL contributors <mobidata-bw@nvbw.de>"
LABEL org.opencontainers.image.documentation="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.source="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.licenses="(EUPL-1.2)"

WORKDIR /opt/dagster/app

RUN apt update && apt install -y \
	build-essential \
	libgdal-dev \
	lftp expect \
	&& rm -rf /var/lib/apt/lists/*

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load the DagsterInstance
COPY requirements-pipeline.txt /opt/dagster/app

# Install requirements
RUN pip install -r requirements-pipeline.txt

# Add repository code
COPY pipeline/ /opt/dagster/app/pipeline/
COPY scripts/ /opt/dagster/app/scripts/

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipeline"]
