FROM airbyte/python-connector-base:1.1.0@sha256:dd17e347fbda94f7c3abff539be298a65af2d7fc27a307d89297df1081a45c27

COPY . ./airbyte/integration_code
RUN pip install ./airbyte/integration_code

# The entrypoint and default env vars are already set in the base image
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
