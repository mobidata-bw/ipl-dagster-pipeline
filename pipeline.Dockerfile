FROM python:3.11

WORKDIR /opt/dagster/app

# Checkout and install dagster libraries needed to run the gRPC server
# exposing your repository to dagit and dagster-daemon, and to load the DagsterInstance
COPY requirements.txt /opt/dagster/app

# Install requirements
RUN pip install -r requirements.txt

# Add repository code
COPY pipeline/ /opt/dagster/app/pipeline/

# Run dagster gRPC server on port 4000

EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors that want
# to run other commands against your repository
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "pipeline"]
