# syntax = docker/dockerfile:1
# Note: Image version should match GDAL version in requirements-pipeline.txt
# for current versions, see https://github.com/OSGeo/gdal/pkgs/container/gdal,
# sha256:c7d5058385 corresponds to 3.8.1
FROM ghcr.io/osgeo/gdal:ubuntu-small-latest@sha256:c7d5058385b0726379f241ebd0fa4754bba789dd6c9d0ae34b1e53bb373a1647

LABEL org.opencontainers.image.title="MobiData-BW Data Pipeline API"
LABEL org.opencontainers.image.authors="Holger Bruch <hb@mfdz.de>, MobiData-BW IPL contributors <mobidata-bw@nvbw.de>"
LABEL org.opencontainers.image.documentation="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.source="https://github.com/mobidata-bw/ipl-dagster-pipeline"
LABEL org.opencontainers.image.licenses="(EUPL-1.2)"

RUN apt-get update -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y python3-pip \
    && rm -rf /var/lib/apt/lists/*;

WORKDIR /opt/dagster/app


# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load the DagsterInstance
COPY requirements-pipeline.txt /opt/dagster/app

# Install requirements
# pip complains about Fionas version, use legacy-resolver to work around (see https://stackoverflow.com/questions/67074684/pip-has-problems-with-metadata)
RUN --mount=type=cache,target=/root/.cache/pip \
	pip install --use-deprecated=legacy-resolver -r requirements-pipeline.txt

# Add repository code
COPY pipeline/ /opt/dagster/app/pipeline/

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipeline"]
