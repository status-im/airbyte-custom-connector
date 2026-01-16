#FROM airbyte/python-connector-base:1.1.0@sha256:dd17e347fbda94f7c3abff539be298a65af2d7fc27a307d89297df1081a45c27
FROM --platform=linux/amd64 airbyte/python-connector-base:1.1.0
COPY setup.py requirements.txt ./airbyte/integration_code/

# Force reinstall the correct airbyte-cdk version to avoid conflicts
RUN pip uninstall -y airbyte-cdk
RUN pip install ./airbyte/integration_code

COPY . ./airbyte/integration_code

# The entrypoint and default env vars are already set in the base image
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

