FROM --platform=linux/amd64 airbyte/python-connector-base:1.1.0

# Copy and install Dep
COPY ./setup.py ./airbyte/integration_code/
RUN pip install ./airbyte/integration_code

COPY . ./airbyte/integration_code

# The entrypoint and default env vars are already set in the base image
ENV AIRBYTE_ENTRYPOINT "python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
